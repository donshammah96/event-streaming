import type { NextApiRequest, NextApiResponse } from "next";
import { WebSocketServer, WebSocket } from "ws";
import { IncomingMessage } from "http";
import { initNats, listStreams } from "@/lib/nats";
import { ensureSchemaCache } from "@/lib/schema";
import { initializeSimulators, simulatorEvents } from "@/lib/simulator";
import { supabase } from "@/lib/supabaseClient";

export const config = {
  api: {
    bodyParser: false,
  },
};

let wss: WebSocketServer | null = null;
let clients: Set<WebSocket> = new Set();
let statsInterval: NodeJS.Timeout | null = null;
let logListener:
  | ((data: {
      durableName: string;
      timestamp: string;
      text: string;
      type: string;
    }) => void)
  | null = null;
let metricsListener:
  | ((data: { durableName: string; event: string; stream: string }) => void)
  | null = null;

// dev environment singleton references caching
if (process.env.NODE_ENV === "development") {
  const g = global as unknown as {
    _wss: WebSocketServer | null;
    _clients: Set<WebSocket>;
    _statsInterval: NodeJS.Timeout | null;
  };
  if (!g._wss) {
    g._wss = null;
    g._clients = new Set();
    g._statsInterval = null;
  }
  wss = g._wss;
  clients = g._clients;
  statsInterval = g._statsInterval;
}

function broadcast(payload: unknown) {
  const message = JSON.stringify(payload);
  clients.forEach((client) => {
    if (client.readyState === WebSocket.OPEN) {
      client.send(message);
    }
  });
}

// Stats Polling Loop
async function startStatsPolling() {
  const currentInterval =
    statsInterval ||
    (process.env.NODE_ENV === "development"
      ? (global as unknown as { _statsInterval: NodeJS.Timeout | null })
          ._statsInterval
      : null);
  if (currentInterval) return;

  statsInterval = setInterval(async () => {
    const activeClients =
      process.env.NODE_ENV === "development"
        ? (global as unknown as { _clients: Set<WebSocket> })._clients
        : clients;
    if (!activeClients || activeClients.size === 0) return;

    try {
      let streamsData: unknown[] = [];
      try {
        const streams = await listStreams();
        streamsData = streams.map((s) => ({
          name: s.name,
          subjects: s.subjects,
        }));
      } catch (natsErr) {
        // Suppress logs to avoid dev noise
      }

      // Count active simulator consumers in Supabase
      const { count: activeSimulatorsCount } = await supabase
        .from("ConsumerSimulatorConfig")
        .select("*", { count: "exact", head: true })
        .eq("active", true);

      // Count pending DLQ events in Supabase
      const { count: pendingDlqCount } = await supabase
        .from("DlqEvent")
        .select("*", { count: "exact", head: true })
        .eq("status", "PENDING");

      // Count total registered validation schemas in Supabase
      const { count: schemaCount } = await supabase
        .from("Schema")
        .select("*", { count: "exact", head: true });

      broadcast({
        type: "STATS",
        data: {
          streams: streamsData,
          activeSimulators: activeSimulatorsCount || 0,
          pendingDlq: pendingDlqCount || 0,
          registeredSchemas: schemaCount || 0,
          timestamp: new Date().toISOString(),
        },
      });
    } catch (err) {
      console.warn("Stats polling error:", err);
    }
  }, 2000);

  if (process.env.NODE_ENV === "development") {
    (
      global as unknown as { _statsInterval: NodeJS.Timeout | null }
    )._statsInterval = statsInterval;
  }
}

// Bind Emitter Listeners safely
if (process.env.NODE_ENV === "development") {
  const g = global as any;
  if (g._logListener) {
    simulatorEvents.off("log", g._logListener);
  }
  if (g._metricsListener) {
    simulatorEvents.off("metrics", g._metricsListener);
  }

  g._logListener = (logData: unknown) => {
    broadcast({ type: "LOG", data: logData });
  };
  g._metricsListener = (metricsData: unknown) => {
    broadcast({ type: "METRICS", data: metricsData });
  };

  simulatorEvents.on("log", g._logListener);
  simulatorEvents.on("metrics", g._metricsListener);
} else {
  if (!logListener) {
    logListener = (logData: unknown) => {
      broadcast({ type: "LOG", data: logData });
    };
    simulatorEvents.on("log", logListener);
  }

  if (!metricsListener) {
    metricsListener = (metricsData: unknown) => {
      broadcast({ type: "METRICS", data: metricsData });
    };
    simulatorEvents.on("metrics", metricsListener);
  }
}

export default async function handler(req: NextApiRequest, res: any) {
  // 1. Bootstrap NATS, Schemas and Active Simulators once on connection setup
  try {
    await initNats();
    await ensureSchemaCache();
    await initializeSimulators();
    await startStatsPolling();
  } catch (err) {
    console.error("Bootstrap initialization error in WebSocket handler:", err);
  }

  // 2. Initialize the WebSocket Server on top of Node server
  const server = res.socket?.server;
  if (server && !server.wss) {
    console.log("Initializing WebSocket server...");
    wss = new WebSocketServer({ noServer: true });
    server.wss = wss;

    if (process.env.NODE_ENV === "development") {
      (global as any)._wss = wss;
    }

    // Single subscription registration for upgrade event
    if (!server.wssUpgradeRegistered) {
      server.wssUpgradeRegistered = true;
      server.on(
        "upgrade",
        (request: IncomingMessage, socket: any, head: any) => {
          const url = new URL(
            request.url || "",
            `http://${request.headers.host}`,
          );
          if (url.pathname === "/api/ws") {
            const serverWss = server.wss || wss;
            serverWss?.handleUpgrade(request, socket, head, (ws: any) => {
              serverWss?.emit("connection", ws, request);
            });
          }
        },
      );
    }

    wss.on("connection", (ws) => {
      const activeClients =
        process.env.NODE_ENV === "development"
          ? (global as any)._clients
          : clients;
      activeClients.add(ws);
      console.log(
        `WebSocket client connected. Total clients: ${activeClients.size}`,
      );

      ws.send(
        JSON.stringify({
          type: "SYSTEM",
          message: "Connected to Event Streaming Platform WebSocket Gateway",
        }),
      );

      ws.on("close", () => {
        activeClients.delete(ws);
        console.log(
          `WebSocket client disconnected. Total clients: ${activeClients.size}`,
        );
      });

      ws.on("error", (err) => {
        console.error("WebSocket client error:", err);
      });
    });
  }

  res.end();
}
