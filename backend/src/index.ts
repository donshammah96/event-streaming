import http from "http";
import express from "express";
import cors from "cors";
import { WebSocketServer, WebSocket } from "ws";
import dotenv from "dotenv";
import Ajv from "ajv"; // top-level static import (was dynamic require in routes.ts)
import { router } from "./routes";
import { initNats, getNatsConnection, listStreams } from "./nats";
import { reloadSchemaCache } from "./schema";
import { initializeSimulators, simulatorEvents } from "./services/simulator";
import { prisma } from "./database";

// Load environmental variables
dotenv.config();

const app = express();
const port = process.env.PORT || 3001;

// CORS — restrict to configured origin in production
const corsOrigin = process.env.CORS_ORIGIN ?? "*";
app.use(cors({ origin: corsOrigin }));
app.use(express.json());

// API Routes
app.use("/api", router);

// Create HTTP Server
const server = http.createServer(app);

// Create WebSocket Server
const wss = new WebSocketServer({ noServer: true });

// Handle WebSocket upgrade
server.on("upgrade", (request, socket, head) => {
  wss.handleUpgrade(request, socket, head, (ws) => {
    wss.emit("connection", ws, request);
  });
});

/** Typed broadcast payload discriminated union */
type BroadcastPayload =
  | { type: "SYSTEM"; message: string }
  | { type: "LOG"; data: Record<string, unknown> }
  | { type: "METRICS"; data: Record<string, unknown> }
  | {
      type: "STATS";
      data: {
        streams: { name: string; subjects: string[] }[];
        activeSimulators: number;
        pendingDlq: number;
        registeredSchemas: number;
        timestamp: string;
      };
    };

const clients = new Set<WebSocket>();

wss.on("connection", (ws) => {
  clients.add(ws);
  console.log(`WebSocket client connected. Total clients: ${clients.size}`);

  // Send an initial greeting
  ws.send(
    JSON.stringify({
      type: "SYSTEM",
      message: "Connected to Event Streaming Platform WebSocket Gateway",
    } satisfies BroadcastPayload),
  );

  ws.on("close", () => {
    clients.delete(ws);
    console.log(`WebSocket client disconnected. Total clients: ${clients.size}`);
  });

  ws.on("error", (err) => {
    console.error("WebSocket client error:", err);
  });
});

// Broadcast helper — strongly typed
function broadcast(payload: BroadcastPayload): void {
  const message = JSON.stringify(payload);
  for (const client of clients) {
    if (client.readyState === WebSocket.OPEN) {
      client.send(message);
    }
  }
}

// Forward events from Simulator emitter to WebSockets
simulatorEvents.on("log", (logData: Record<string, unknown>) => {
  broadcast({ type: "LOG", data: logData });
});

simulatorEvents.on("metrics", (metricsData: Record<string, unknown>) => {
  broadcast({ type: "METRICS", data: metricsData });
});

// Setup interval to poll and broadcast NATS/DB statistics in real time
let statsInterval: NodeJS.Timeout;

async function startStatsPolling(): Promise<void> {
  statsInterval = setInterval(async () => {
    if (clients.size === 0) return; // Skip polling if no active UI listeners

    try {
      // 1. Get streams statistics from NATS
      let streamsData: { name: string; subjects: string[] }[] = [];
      try {
        const streams = await listStreams();
        streamsData = streams.map((s) => ({
          name: s.name,
          subjects: s.subjects,
        }));
      } catch {
        // Suppress logs to avoid noise
      }

      // 2. Count active simulator consumers
      const activeSimulatorsCount =
        await prisma.consumerSimulatorConfig.count({
          where: { active: true },
        });

      // 3. Count pending DLQ events
      const pendingDlqCount = await prisma.dlqEvent.count({
        where: { status: "PENDING" },
      });

      // 4. Count total schemas registered
      const schemaCount = await prisma.schema.count();

      broadcast({
        type: "STATS",
        data: {
          streams: streamsData,
          activeSimulators: activeSimulatorsCount,
          pendingDlq: pendingDlqCount,
          registeredSchemas: schemaCount,
          timestamp: new Date().toISOString(),
        },
      });
    } catch (err) {
      console.warn("Stats polling error:", err);
    }
  }, 2000); // Poll every 2 seconds
}

// Bootstrap execution
async function bootstrap(): Promise<void> {
  try {
    // 1. Connect to NATS JetStream
    await initNats();

    // 2. Compile and cache Schemas from PostgreSQL
    await reloadSchemaCache();

    // 3. Bootstrap active simulators from last state
    await initializeSimulators();

    // 4. Start stats polling
    await startStatsPolling();

    // 5. Listen
    server.listen(port, () => {
      console.log(
        `Express Backend Server is running at http://localhost:${port}`,
      );
      console.log(`WebSocket Server is listening on ws://localhost:${port}`);
    });
  } catch (err) {
    console.error("Bootstrap initialization error. Shutting down:", err);
    process.exit(1);
  }
}

// ── Graceful shutdown ─────────────────────────────────────────────────────────
async function shutdown(signal: string): Promise<void> {
  console.log(`\n[${signal}] Shutting down gracefully...`);

  // 1. Stop stat polling to prevent DB calls after disconnect
  clearInterval(statsInterval);

  // 2. Close all WebSocket connections cleanly
  for (const client of clients) {
    client.terminate();
  }
  wss.close((err) => {
    if (err) console.error("WSS close error:", err);
  });

  // 3. Close the HTTP server
  server.close();

  // 4. Disconnect from NATS
  try {
    const nc = getNatsConnection();
    await nc.drain();
  } catch {
    // NATS may not have been initialized — skip drain gracefully
  }

  // 5. Disconnect Prisma
  await prisma.$disconnect();

  process.exit(0);
}

process.on("SIGTERM", () => void shutdown("SIGTERM"));
process.on("SIGINT", () => void shutdown("SIGINT")); // handle Ctrl+C in local dev

// Export Ajv at module level to confirm static import works (used in routes.ts)
export { Ajv };

bootstrap();
