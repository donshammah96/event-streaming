import { Router } from "express";
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
  codec
} from "./nats";
import { validateMessage, reloadSchemaCache } from "./schema";
import { startSimulator, stopSimulator } from "./services/simulator";

export const router = Router();

// ==========================================
// 1. STREAM ROUTES
// ==========================================

// List all streams
router.get("/streams", async (req, res) => {
  try {
    const list = await listStreams();
    res.json(list);
  } catch (err: any) {
    res.status(500).json({ error: err.message });
  }
});

// Get stream info
router.get("/streams/:name", async (req, res) => {
  try {
    const info = await getStreamInfo(req.params.name);
    if (!info) return res.status(404).json({ error: "Stream not found" });
    res.json(info);
  } catch (err: any) {
    res.status(500).json({ error: err.message });
  }
});

// Create a stream
router.post("/streams", async (req, res) => {
  const { name, subjects } = req.body;
  if (!name || !subjects || !Array.isArray(subjects)) {
    return res.status(400).json({ error: "Missing required fields: name (string), subjects (array of strings)" });
  }
  try {
    const info = await createStream(name, subjects);
    res.status(201).json(info);
  } catch (err: any) {
    res.status(500).json({ error: err.message });
  }
});

// Delete a stream
router.delete("/streams/:name", async (req, res) => {
  try {
    await deleteStream(req.params.name);
    res.json({ success: true });
  } catch (err: any) {
    res.status(500).json({ error: err.message });
  }
});

// Get messages in a stream (paginated backwards from last sequence)
router.get("/streams/:name/messages", async (req, res) => {
  const streamName = req.params.name;
  const limit = parseInt(req.query.limit as string) || 50;
  const offsetSeqStr = req.query.offsetSeq as string;
  
  try {
    const info = await getStreamInfo(streamName);
    if (!info) return res.status(404).json({ error: "Stream not found" });
    
    const { first_seq, last_seq } = info.state;
    if (last_seq === 0) {
      return res.json({ messages: [], nextOffsetSeq: null });
    }

    let currentSeq = offsetSeqStr ? parseInt(offsetSeqStr) : last_seq;
    const messages: any[] = [];
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
      nextOffsetSeq: currentSeq >= first_seq ? currentSeq : null
    });
  } catch (err: any) {
    res.status(500).json({ error: err.message });
  }
});

// Publish a message with schema validation
router.post("/publish", async (req, res) => {
  const { subject, payload, headers } = req.body;
  if (!subject || payload === undefined) {
    return res.status(400).json({ error: "Missing required fields: subject (string), payload (any)" });
  }

  // Schema Validation
  const validation = validateMessage(subject, payload);
  if (!validation.valid) {
    return res.status(400).json({
      error: "Schema validation failed",
      errors: validation.errors,
      matchedPattern: validation.matchedPattern
    });
  }

  try {
    const js = getJetStream();
    
    // Construct NATS headers
    const opt: any = {};
    if (headers && typeof headers === "object") {
      const nHeaders = natsHeaders();
      for (const [key, value] of Object.entries(headers)) {
        nHeaders.append(key, String(value));
      }
      opt.headers = nHeaders;
    }

    const pa = await js.publish(subject, codec.encode(payload), opt);
    res.json({
      success: true,
      seq: pa.seq,
      stream: pa.stream,
      duplicate: pa.duplicate,
      matchedPattern: validation.matchedPattern || null
    });
  } catch (err: any) {
    res.status(500).json({ error: err.message });
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
  } catch (err: any) {
    res.status(500).json({ error: err.message });
  }
});

// Create consumer on stream
router.post("/streams/:streamName/consumers", async (req, res) => {
  const { durableName, filterSubject, maxDeliver } = req.body;
  if (!durableName) {
    return res.status(400).json({ error: "Missing required field: durableName (string)" });
  }
  try {
    const info = await createConsumer(req.params.streamName, {
      durableName,
      filterSubject,
      maxDeliver: maxDeliver ? parseInt(maxDeliver) : undefined
    });
    res.status(201).json(info);
  } catch (err: any) {
    res.status(500).json({ error: err.message });
  }
});

// Delete consumer from stream
router.delete("/streams/:streamName/consumers/:consumerName", async (req, res) => {
  try {
    await deleteConsumer(req.params.streamName, req.params.consumerName);
    res.json({ success: true });
  } catch (err: any) {
    res.status(500).json({ error: err.message });
  }
});

// ==========================================
// 3. SCHEMA REGISTRY ROUTES
// ==========================================

// Get all schemas
router.get("/schemas", async (req, res) => {
  try {
    const schemas = await prisma.schema.findMany({
      orderBy: { createdAt: "desc" }
    });
    res.json(schemas);
  } catch (err: any) {
    res.status(500).json({ error: err.message });
  }
});

// Register new schema
router.post("/schemas", async (req, res) => {
  const { subjectPattern, schema, description } = req.body;
  if (!subjectPattern || !schema) {
    return res.status(400).json({ error: "Missing required fields: subjectPattern (string), schema (object)" });
  }
  
  try {
    // Validate that the schema definition is valid JSON Schema
    const Ajv = require("ajv");
    const ajvInstance = new Ajv();
    ajvInstance.compile(schema);
  } catch (compileErr: any) {
    return res.status(400).json({ error: `Invalid JSON Schema definition: ${compileErr.message}` });
  }

  try {
    const newSchema = await prisma.schema.create({
      data: {
        subjectPattern,
        schema,
        description,
        version: 1
      }
    });
    
    // Reload Ajv compiler cache
    await reloadSchemaCache();
    
    res.status(201).json(newSchema);
  } catch (err: any) {
    if (err.code === "P2002") {
      return res.status(400).json({ error: `A schema for subject pattern '${subjectPattern}' already exists` });
    }
    res.status(500).json({ error: err.message });
  }
});

// Update schema
router.put("/schemas/:id", async (req, res) => {
  const { schema, description } = req.body;
  if (!schema) {
    return res.status(400).json({ error: "Missing required field: schema (object)" });
  }

  try {
    const Ajv = require("ajv");
    const ajvInstance = new Ajv();
    ajvInstance.compile(schema);
  } catch (compileErr: any) {
    return res.status(400).json({ error: `Invalid JSON Schema definition: ${compileErr.message}` });
  }

  try {
    const current = await prisma.schema.findUnique({ where: { id: req.params.id } });
    if (!current) return res.status(404).json({ error: "Schema not found" });

    const updated = await prisma.schema.update({
      where: { id: req.params.id },
      data: {
        schema,
        description,
        version: current.version + 1
      }
    });

    await reloadSchemaCache();
    res.json(updated);
  } catch (err: any) {
    res.status(500).json({ error: err.message });
  }
});

// Delete schema
router.delete("/schemas/:id", async (req, res) => {
  try {
    await prisma.schema.delete({ where: { id: req.params.id } });
    await reloadSchemaCache();
    res.json({ success: true });
  } catch (err: any) {
    res.status(500).json({ error: err.message });
  }
});

// ==========================================
// 4. DLQ ROUTES
// ==========================================

// List DLQ events
router.get("/dlq", async (req, res) => {
  const { status } = req.query;
  const where: any = {};
  if (status) {
    where.status = String(status);
  }
  
  try {
    const dlq = await prisma.dlqEvent.findMany({
      where,
      orderBy: { createdAt: "desc" }
    });
    res.json(dlq);
  } catch (err: any) {
    res.status(500).json({ error: err.message });
  }
});

// Replay DLQ message
router.post("/dlq/:id/replay", async (req, res) => {
  const { targetSubject } = req.body; // Allow replaying to a custom subject
  
  try {
    const event = await prisma.dlqEvent.findUnique({
      where: { id: req.params.id }
    });

    if (!event) return res.status(404).json({ error: "DLQ event not found" });

    const js = getJetStream();
    const subject = targetSubject || event.subject;

    // Build headers from original headers
    const opt: any = {};
    if (event.headers && typeof event.headers === "object") {
      const nHeaders = natsHeaders();
      for (const [key, value] of Object.entries(event.headers)) {
        nHeaders.append(key, String(value));
      }
      // Add a header to indicate this is a replayed event
      nHeaders.set("X-Event-Replayed-From-DLQ", event.id);
      opt.headers = nHeaders;
    } else {
      const nHeaders = natsHeaders();
      nHeaders.set("X-Event-Replayed-From-DLQ", event.id);
      opt.headers = nHeaders;
    }

    // Publish event back to NATS
    const pa = await js.publish(subject, codec.encode(event.payload), opt);

    // Update status in PostgreSQL
    const updated = await prisma.dlqEvent.update({
      where: { id: req.params.id },
      data: { status: "REPLAYED" }
    });

    res.json({
      success: true,
      seq: pa.seq,
      stream: pa.stream,
      event: updated
    });
  } catch (err: any) {
    res.status(500).json({ error: err.message });
  }
});

// Dismiss DLQ message
router.post("/dlq/:id/dismiss", async (req, res) => {
  try {
    const updated = await prisma.dlqEvent.update({
      where: { id: req.params.id },
      data: { status: "DISMISSED" }
    });
    res.json(updated);
  } catch (err: any) {
    res.status(500).json({ error: err.message });
  }
});

// Purge or clear DLQ database
router.post("/dlq/purge", async (req, res) => {
  try {
    await prisma.dlqEvent.deleteMany({});
    res.json({ success: true });
  } catch (err: any) {
    res.status(500).json({ error: err.message });
  }
});

// ==========================================
// 5. SIMULATOR CONFIG ROUTES
// ==========================================

// Get all simulators configs
router.get("/simulator", async (req, res) => {
  try {
    const configs = await prisma.consumerSimulatorConfig.findMany({
      orderBy: { createdAt: "desc" }
    });
    res.json(configs);
  } catch (err: any) {
    res.status(500).json({ error: err.message });
  }
});

// Create/Register simulator config
router.post("/simulator", async (req, res) => {
  const { stream, subject, durableName, successRate, processingDelay, maxDeliver } = req.body;
  if (!stream || !subject || !durableName || successRate === undefined || processingDelay === undefined) {
    return res.status(400).json({
      error: "Missing required fields: stream, subject, durableName, successRate (float), processingDelay (int)"
    });
  }

  try {
    const config = await prisma.consumerSimulatorConfig.create({
      data: {
        stream,
        subject,
        durableName,
        successRate: parseFloat(successRate),
        processingDelay: parseInt(processingDelay),
        maxDeliver: maxDeliver ? parseInt(maxDeliver) : 3,
        active: false
      }
    });
    res.status(201).json(config);
  } catch (err: any) {
    if (err.code === "P2002") {
      return res.status(400).json({ error: `A consumer group with name '${durableName}' is already registered` });
    }
    res.status(500).json({ error: err.message });
  }
});

// Update simulator configuration
router.put("/simulator/:durableName", async (req, res) => {
  const { successRate, processingDelay, maxDeliver, subject } = req.body;
  const durableName = req.params.durableName;
  
  try {
    const data: any = {};
    if (successRate !== undefined) data.successRate = parseFloat(successRate);
    if (processingDelay !== undefined) data.processingDelay = parseInt(processingDelay);
    if (maxDeliver !== undefined) data.maxDeliver = parseInt(maxDeliver);
    if (subject !== undefined) data.subject = subject;

    const updated = await prisma.consumerSimulatorConfig.update({
      where: { durableName },
      data
    });

    // If active, we restart it so it picks up changes
    const config = await prisma.consumerSimulatorConfig.findUnique({ where: { durableName } });
    if (config?.active) {
      await stopSimulator(durableName);
      await startSimulator(durableName);
    }

    res.json(updated);
  } catch (err: any) {
    res.status(500).json({ error: err.message });
  }
});

// Start simulator worker
router.post("/simulator/:durableName/start", async (req, res) => {
  try {
    await startSimulator(req.params.durableName);
    res.json({ success: true });
  } catch (err: any) {
    res.status(500).json({ error: err.message });
  }
});

// Stop simulator worker
router.post("/simulator/:durableName/stop", async (req, res) => {
  try {
    await stopSimulator(req.params.durableName);
    res.json({ success: true });
  } catch (err: any) {
    res.status(500).json({ error: err.message });
  }
});

// Delete simulator config
router.delete("/simulator/:durableName", async (req, res) => {
  const durableName = req.params.durableName;
  try {
    await stopSimulator(durableName);
    await prisma.consumerSimulatorConfig.delete({ where: { durableName } });
    res.json({ success: true });
  } catch (err: any) {
    res.status(500).json({ error: err.message });
  }
});
