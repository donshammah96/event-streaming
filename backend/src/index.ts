import http from "http";
import express from "express";
import cors from "cors";
import { WebSocketServer, WebSocket } from "ws";
import dotenv from "dotenv";
import { router } from "./routes";
import { initNats, listStreams } from "./nats";
import { reloadSchemaCache } from "./schema";
import { initializeSimulators, simulatorEvents } from "./services/simulator";
import { prisma } from "./database";

// Load environmental variables
dotenv.config();

const app = express();
const port = process.env.PORT || 3001;

// Middlewares
app.use(cors());
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

const clients = new Set<WebSocket>();

wss.on("connection", (ws) => {
  clients.add(ws);
  console.log(`WebSocket client connected. Total clients: ${clients.size}`);

  // Send an initial greeting
  ws.send(JSON.stringify({
    type: "SYSTEM",
    message: "Connected to Event Streaming Platform WebSocket Gateway"
  }));

  ws.on("close", () => {
    clients.delete(ws);
    console.log(`WebSocket client disconnected. Total clients: ${clients.size}`);
  });
  
  ws.on("error", (err) => {
    console.error("WebSocket client error:", err);
  });
});

// Broadcast helper
function broadcast(payload: any) {
  const message = JSON.stringify(payload);
  clients.forEach((client) => {
    if (client.readyState === WebSocket.OPEN) {
      client.send(message);
    }
  });
}

// Forward events from Simulator emitter to WebSockets
simulatorEvents.on("log", (logData) => {
  broadcast({
    type: "LOG",
    data: logData
  });
});

simulatorEvents.on("metrics", (metricsData) => {
  broadcast({
    type: "METRICS",
    data: metricsData
  });
});

// Setup interval to poll and broadcast NATS/DB statistics in real time
let statsInterval: NodeJS.Timeout;

async function startStatsPolling() {
  statsInterval = setInterval(async () => {
    if (clients.size === 0) return; // Skip polling if no active UI listeners
    
    try {
      // 1. Get streams statistics from NATS
      let streamsData: any[] = [];
      try {
        const streams = await listStreams();
        streamsData = streams.map(s => ({
          name: s.name,
          subjects: s.subjects,
        }));
      } catch (natsErr) {
        // Suppress logs to avoid noise
      }

      // 2. Count active simulator consumers
      const activeSimulatorsCount = await prisma.consumerSimulatorConfig.count({
        where: { active: true }
      });

      // 3. Count pending DLQ events
      const pendingDlqCount = await prisma.dlqEvent.count({
        where: { status: "PENDING" }
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
          timestamp: new Date().toISOString()
        }
      });
    } catch (err) {
      console.warn("Stats polling error:", err);
    }
  }, 2000); // Poll every 2 seconds
}

// Bootstrap execution
async function bootstrap() {
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
      console.log(`Express Backend Server is running at http://localhost:${port}`);
      console.log(`WebSocket Server is listening on ws://localhost:${port}`);
    });
  } catch (err) {
    console.error("Bootstrap initialization error. Shutting down:", err);
    process.exit(1);
  }
}

// Handle graceful shutdown
process.on("SIGTERM", async () => {
  console.log("Shutting down gracefully...");
  clearInterval(statsInterval);
  await prisma.$disconnect();
  process.exit(0);
});

bootstrap();
