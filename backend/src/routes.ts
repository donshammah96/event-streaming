import { Router, Request, Response, NextFunction } from "express";
import Ajv from "ajv";
import { headers as natsHeaders } from "nats";
import { prisma } from "./database";
import {
  listStreams,
  getStreamInfo,
  createStream,
  deleteStream,
  listConsumers,
  getConsumerInfo,
  createConsumer,
  deleteConsumer,
  getStreamMessage,
  getJetStream,
  codec,
} from "./nats";
import { validateMessage, reloadSchemaCache } from "./schema";
import { startSimulator, stopSimulator } from "./services/simulator";

export const router = Router();

// Ajv instance — module-level singleton (was dynamic require() per-request)
const ajv = new Ajv();

// ==========================================
// RATE LIMITING MIDDLEWARE
// ==========================================

/**
 * Simple in-process rate limiter.
 * Maps `clientKey` → sliding-window request timestamps.
 * For multi-instance deployments, replace with Redis-backed upstash/ratelimit.
 */
interface RateLimitConfig {
  windowMs: number;
  max: number;
}

function rateLimit(config: RateLimitConfig) {
  const { windowMs, max } = config;
  const windows = new Map<string, number[]>();

  // Garbage-collect expired entries every minute to prevent memory growth
  setInterval(() => {
    const cutoff = Date.now() - windowMs;
    for (const [key, timestamps] of windows) {
      const pruned = timestamps.filter((t) => t > cutoff);
      if (pruned.length === 0) {
        windows.delete(key);
      } else {
        windows.set(key, pruned);
      }
    }
  }, 60_000);

  return (req: Request, res: Response, next: NextFunction): void => {
    // Use IP as the client key; prefer X-Forwarded-For for proxied environments
    const clientKey =
      (req.headers["x-forwarded-for"] as string | undefined)?.split(",")[0]?.trim() ??
      req.socket.remoteAddress ??
      "unknown";

    const now = Date.now();
    const cutoff = now - windowMs;
    const timestamps = (windows.get(clientKey) ?? []).filter((t) => t > cutoff);

    if (timestamps.length >= max) {
      const retryAfter = Math.ceil(
        (timestamps[0]! + windowMs - now) / 1000,
      );
      res.setHeader("Retry-After", String(retryAfter));
      res.status(429).json({
        error: `Rate limit exceeded. Retry after ${retryAfter}s.`,
      });
      return;
    }

    timestamps.push(now);
    windows.set(clientKey, timestamps);
    next();
  };
}

// Publish limiter: 60 requests per minute per IP
const publishLimiter = rateLimit({ windowMs: 60_000, max: 60 });
// Simulator start/stop limiter: 20 requests per minute per IP
const simulatorLimiter = rateLimit({ windowMs: 60_000, max: 20 });

// ==========================================
// 1. STREAM ROUTES
// ==========================================

// List all streams
router.get("/streams", async (req, res) => {
  try {
    const list = await listStreams();
    res.json(list);
  } catch (err: unknown) {
    res.status(500).json({ error: (err as Error).message });
  }
});

// Get stream info
router.get("/streams/:name", async (req, res) => {
  try {
    const info = await getStreamInfo(req.params.name);
    if (!info) return res.status(404).json({ error: "Stream not found" });
    res.json(info);
  } catch (err: unknown) {
    res.status(500).json({ error: (err as Error).message });
  }
});

// Create a stream
router.post("/streams", async (req, res) => {
  const { name, subjects } = req.body as {
    name?: unknown;
    subjects?: unknown;
  };
  if (!name || !subjects || !Array.isArray(subjects)) {
    return res.status(400).json({
      error:
        "Missing required fields: name (string), subjects (array of strings)",
    });
  }
  try {
    const info = await createStream(String(name), subjects as string[]);
    res.status(201).json(info);
  } catch (err: unknown) {
    res.status(500).json({ error: (err as Error).message });
  }
});

// Delete a stream
router.delete("/streams/:name", async (req, res) => {
  try {
    await deleteStream(req.params.name);
    res.json({ success: true });
  } catch (err: unknown) {
    res.status(500).json({ error: (err as Error).message });
  }
});

// Get messages in a stream (paginated backwards from last sequence)
router.get("/streams/:name/messages", async (req, res) => {
  const streamName = req.params.name;
  const limit = Math.min(parseInt(String(req.query.limit ?? "50"), 10), 200);
  const offsetSeqStr = req.query.offsetSeq as string | undefined;

  try {
    const info = await getStreamInfo(streamName);
    if (!info) return res.status(404).json({ error: "Stream not found" });

    const { first_seq, last_seq } = info.state;
    if (last_seq === 0) {
      return res.json({ messages: [], nextOffsetSeq: null });
    }

    let currentSeq = offsetSeqStr ? parseInt(offsetSeqStr, 10) : last_seq;
    const messages: unknown[] = [];
    let count = 0;

    // Fetch messages sequentially backwards
    while (currentSeq >= first_seq && count < limit) {
      const msg = await getStreamMessage(streamName, currentSeq);
      if (msg) {
        messages.push(msg);
        count++;
      }
      currentSeq--;
    }

    res.json({
      messages,
      nextOffsetSeq: currentSeq >= first_seq ? currentSeq : null,
    });
  } catch (err: unknown) {
    res.status(500).json({ error: (err as Error).message });
  }
});

// Publish a message with schema validation
router.post("/publish", publishLimiter, async (req, res) => {
  const { subject, payload, headers } = req.body as {
    subject?: unknown;
    payload?: unknown;
    headers?: Record<string, unknown>;
  };
  if (!subject || payload === undefined) {
    return res.status(400).json({
      error: "Missing required fields: subject (string), payload (any)",
    });
  }

  // Schema Validation
  const validation = validateMessage(String(subject), payload);
  if (!validation.valid) {
    return res.status(400).json({
      error: "Schema validation failed",
      errors: validation.errors,
      matchedPattern: validation.matchedPattern,
    });
  }

  try {
    const js = getJetStream();

    // Construct NATS headers
    const opt: { headers?: ReturnType<typeof natsHeaders> } = {};
    if (headers && typeof headers === "object") {
      const nHeaders = natsHeaders();
      for (const [key, value] of Object.entries(headers)) {
        nHeaders.append(key, String(value));
      }
      opt.headers = nHeaders;
    }

    const pa = await js.publish(String(subject), codec.encode(payload), opt);
    res.json({
      success: true,
      seq: pa.seq,
      stream: pa.stream,
      duplicate: pa.duplicate,
      matchedPattern: validation.matchedPattern || null,
    });
  } catch (err: unknown) {
    res.status(500).json({ error: (err as Error).message });
  }
});

// ==========================================
// 2. CONSUMER ROUTES
// ==========================================

// List consumers of a stream
router.get("/streams/:streamName/consumers", async (req, res) => {
  try {
    const list = await listConsumers(req.params.streamName);
    res.json(list);
  } catch (err: unknown) {
    res.status(500).json({ error: (err as Error).message });
  }
});

// Create consumer on stream
router.post("/streams/:streamName/consumers", async (req, res) => {
  const { durableName, filterSubject, maxDeliver } = req.body as {
    durableName?: unknown;
    filterSubject?: unknown;
    maxDeliver?: unknown;
  };
  if (!durableName) {
    return res
      .status(400)
      .json({ error: "Missing required field: durableName (string)" });
  }
  try {
    const info = await createConsumer(req.params.streamName, {
      durableName: String(durableName),
      filterSubject: filterSubject ? String(filterSubject) : undefined,
      maxDeliver: maxDeliver ? parseInt(String(maxDeliver), 10) : undefined,
    });
    res.status(201).json(info);
  } catch (err: unknown) {
    res.status(500).json({ error: (err as Error).message });
  }
});

// Delete consumer from stream
router.delete(
  "/streams/:streamName/consumers/:consumerName",
  async (req, res) => {
    try {
      await deleteConsumer(req.params.streamName, req.params.consumerName);
      res.json({ success: true });
    } catch (err: unknown) {
      res.status(500).json({ error: (err as Error).message });
    }
  },
);

// ==========================================
// 3. SCHEMA REGISTRY ROUTES
// ==========================================

// Get all schemas
router.get("/schemas", async (req, res) => {
  try {
    const schemas = await prisma.schema.findMany({
      orderBy: { createdAt: "desc" },
    });
    res.json(schemas);
  } catch (err: unknown) {
    res.status(500).json({ error: (err as Error).message });
  }
});

// Register new schema
router.post("/schemas", async (req, res) => {
  const { subjectPattern, schema, description } = req.body as {
    subjectPattern?: unknown;
    schema?: unknown;
    description?: unknown;
  };
  if (!subjectPattern || !schema) {
    return res.status(400).json({
      error:
        "Missing required fields: subjectPattern (string), schema (object)",
    });
  }

  try {
    // Validate that the schema definition is valid JSON Schema using module-level Ajv
    ajv.compile(schema);
  } catch (compileErr: unknown) {
    return res.status(400).json({
      error: `Invalid JSON Schema definition: ${(compileErr as Error).message}`,
    });
  }

  try {
    const newSchema = await prisma.schema.create({
      data: {
        subjectPattern: String(subjectPattern),
        schema,
        description: description ? String(description) : undefined,
        version: 1,
      },
    });

    // Reload Ajv compiler cache
    await reloadSchemaCache();

    res.status(201).json(newSchema);
  } catch (err: unknown) {
    const prismaErr = err as { code?: string; message?: string };
    if (prismaErr.code === "P2002") {
      return res.status(400).json({
        error: `A schema for subject pattern '${String(subjectPattern)}' already exists`,
      });
    }
    res.status(500).json({ error: prismaErr.message ?? "Internal error" });
  }
});

// Update schema
router.put("/schemas/:id", async (req, res) => {
  const { schema, description } = req.body as {
    schema?: unknown;
    description?: unknown;
  };
  if (!schema) {
    return res
      .status(400)
      .json({ error: "Missing required field: schema (object)" });
  }

  try {
    ajv.compile(schema);
  } catch (compileErr: unknown) {
    return res.status(400).json({
      error: `Invalid JSON Schema definition: ${(compileErr as Error).message}`,
    });
  }

  try {
    const current = await prisma.schema.findUnique({
      where: { id: req.params.id },
    });
    if (!current) return res.status(404).json({ error: "Schema not found" });

    const updated = await prisma.schema.update({
      where: { id: req.params.id },
      data: {
        schema,
        description: description ? String(description) : undefined,
        version: current.version + 1,
      },
    });

    await reloadSchemaCache();
    res.json(updated);
  } catch (err: unknown) {
    res.status(500).json({ error: (err as Error).message });
  }
});

// Delete schema
router.delete("/schemas/:id", async (req, res) => {
  try {
    await prisma.schema.delete({ where: { id: req.params.id } });
    await reloadSchemaCache();
    res.json({ success: true });
  } catch (err: unknown) {
    res.status(500).json({ error: (err as Error).message });
  }
});

// ==========================================
// 4. DLQ ROUTES
// ==========================================

// List DLQ events
router.get("/dlq", async (req, res) => {
  const { status } = req.query;
  const where: { status?: string } = {};
  if (status) {
    where.status = String(status);
  }

  try {
    const dlq = await prisma.dlqEvent.findMany({
      where,
      orderBy: { createdAt: "desc" },
    });
    res.json(dlq);
  } catch (err: unknown) {
    res.status(500).json({ error: (err as Error).message });
  }
});

// Replay DLQ message
router.post("/dlq/:id/replay", async (req, res) => {
  const { targetSubject } = req.body as { targetSubject?: unknown };

  try {
    const event = await prisma.dlqEvent.findUnique({
      where: { id: req.params.id },
    });

    if (!event) return res.status(404).json({ error: "DLQ event not found" });

    const js = getJetStream();
    const subject = targetSubject ? String(targetSubject) : event.subject;

    // Build headers from original headers
    const nHeaders = natsHeaders();
    if (event.headers && typeof event.headers === "object") {
      for (const [key, value] of Object.entries(
        event.headers as Record<string, unknown>,
      )) {
        nHeaders.append(key, String(value));
      }
    }
    // Add a header to indicate this is a replayed event
    nHeaders.set("X-Event-Replayed-From-DLQ", event.id);

    // Publish event back to NATS
    const pa = await js.publish(subject, codec.encode(event.payload), {
      headers: nHeaders,
    });

    // Update status in PostgreSQL
    const updated = await prisma.dlqEvent.update({
      where: { id: req.params.id },
      data: { status: "REPLAYED" },
    });

    res.json({
      success: true,
      seq: pa.seq,
      stream: pa.stream,
      event: updated,
    });
  } catch (err: unknown) {
    res.status(500).json({ error: (err as Error).message });
  }
});

// Dismiss DLQ message
router.post("/dlq/:id/dismiss", async (req, res) => {
  try {
    const updated = await prisma.dlqEvent.update({
      where: { id: req.params.id },
      data: { status: "DISMISSED" },
    });
    res.json(updated);
  } catch (err: unknown) {
    res.status(500).json({ error: (err as Error).message });
  }
});

// Purge or clear DLQ database
router.post("/dlq/purge", async (req, res) => {
  try {
    await prisma.dlqEvent.deleteMany({});
    res.json({ success: true });
  } catch (err: unknown) {
    res.status(500).json({ error: (err as Error).message });
  }
});

// ==========================================
// 5. SIMULATOR CONFIG ROUTES
// ==========================================

// Get all simulators configs
router.get("/simulator", async (req, res) => {
  try {
    const configs = await prisma.consumerSimulatorConfig.findMany({
      orderBy: { createdAt: "desc" },
    });
    res.json(configs);
  } catch (err: unknown) {
    res.status(500).json({ error: (err as Error).message });
  }
});

// Create/Register simulator config
router.post("/simulator", async (req, res) => {
  const {
    stream,
    subject,
    durableName,
    successRate,
    processingDelay,
    maxDeliver,
  } = req.body as {
    stream?: unknown;
    subject?: unknown;
    durableName?: unknown;
    successRate?: unknown;
    processingDelay?: unknown;
    maxDeliver?: unknown;
  };
  if (
    !stream ||
    !subject ||
    !durableName ||
    successRate === undefined ||
    processingDelay === undefined
  ) {
    return res.status(400).json({
      error:
        "Missing required fields: stream, subject, durableName, successRate (float), processingDelay (int)",
    });
  }

  try {
    const config = await prisma.consumerSimulatorConfig.create({
      data: {
        stream: String(stream),
        subject: String(subject),
        durableName: String(durableName),
        successRate: parseFloat(String(successRate)),
        processingDelay: parseInt(String(processingDelay), 10),
        maxDeliver: maxDeliver ? parseInt(String(maxDeliver), 10) : 3,
        active: false,
      },
    });
    res.status(201).json(config);
  } catch (err: unknown) {
    const prismaErr = err as { code?: string; message?: string };
    if (prismaErr.code === "P2002") {
      return res.status(400).json({
        error: `A consumer group with name '${String(durableName)}' is already registered`,
      });
    }
    res.status(500).json({ error: prismaErr.message ?? "Internal error" });
  }
});

// Update simulator configuration
router.put("/simulator/:durableName", async (req, res) => {
  const {
    successRate,
    processingDelay,
    maxDeliver,
    subject,
  } = req.body as {
    successRate?: unknown;
    processingDelay?: unknown;
    maxDeliver?: unknown;
    subject?: unknown;
  };
  const { durableName } = req.params;

  try {
    const data: {
      successRate?: number;
      processingDelay?: number;
      maxDeliver?: number;
      subject?: string;
    } = {};
    if (successRate !== undefined) data.successRate = parseFloat(String(successRate));
    if (processingDelay !== undefined)
      data.processingDelay = parseInt(String(processingDelay), 10);
    if (maxDeliver !== undefined) data.maxDeliver = parseInt(String(maxDeliver), 10);
    if (subject !== undefined) data.subject = String(subject);

    const updated = await prisma.consumerSimulatorConfig.update({
      where: { durableName },
      data,
    });

    // If active, restart it so it picks up changes
    const config = await prisma.consumerSimulatorConfig.findUnique({
      where: { durableName },
    });
    if (config?.active) {
      await stopSimulator(durableName);
      await startSimulator(durableName);
    }

    res.json(updated);
  } catch (err: unknown) {
    res.status(500).json({ error: (err as Error).message });
  }
});

// Start simulator worker
router.post("/simulator/:durableName/start", simulatorLimiter, async (req, res) => {
  try {
    await startSimulator(req.params.durableName);
    res.json({ success: true });
  } catch (err: unknown) {
    res.status(500).json({ error: (err as Error).message });
  }
});

// Stop simulator worker
router.post("/simulator/:durableName/stop", simulatorLimiter, async (req, res) => {
  try {
    await stopSimulator(req.params.durableName);
    res.json({ success: true });
  } catch (err: unknown) {
    res.status(500).json({ error: (err as Error).message });
  }
});

// Delete simulator config
router.delete("/simulator/:durableName", async (req, res) => {
  const { durableName } = req.params;
  try {
    await stopSimulator(durableName);
    await prisma.consumerSimulatorConfig.delete({ where: { durableName } });
    res.json({ success: true });
  } catch (err: unknown) {
    res.status(500).json({ error: (err as Error).message });
  }
});

// Suppress unused import warning — getConsumerInfo is part of the public API
export { getConsumerInfo };
