import type { NextApiRequest, NextApiResponse } from "next";

import { initNats } from "@/lib/nats";
import { ensureSchemaCache } from "@/lib/schema";
import { initializeSimulators } from "@/lib/simulator";

export const config = {
  api: {
    bodyParser: false,
  },
};

/**
 * GET /api/ws — Bootstrap initializer for NATS, schema cache, and simulator workers.
 *
 * NOTE: The raw WebSocket server (ws package) has been removed because it relies
 * on attaching to the underlying Node HTTP socket, which is incompatible with
 * Vercel's serverless runtime. Real-time log streaming is now handled via
 * Supabase Realtime (SimulatorLog table). Stats are polled via /api/stats.
 *
 * This endpoint is called once on page load to ensure server-side singletons
 * (NATS connection, Ajv schema cache, active simulator workers) are initialized
 * before any other API route is called.
 *
 * In local development, Next.js hot-reload protection (global singletons in
 * lib/nats.ts and lib/simulator.ts) prevents duplicate connections.
 */
export default async function handler(
  req: NextApiRequest,
  res: NextApiResponse<{ ok: boolean; message?: string }>,
) {
  if (req.method !== "GET") {
    res.setHeader("Allow", ["GET"]);
    return res
      .status(405)
      .json({ ok: false, message: `Method ${req.method} Not Allowed` });
  }

  try {
    await initNats();
    await ensureSchemaCache();
    await initializeSimulators();

    return res.status(200).json({ ok: true });
  } catch (err) {
    console.error("[/api/ws] Bootstrap initialization error:", err);
    // Return 200 so the frontend doesn't hard-fail on startup;
    // individual API calls will surface NATS errors if needed.
    return res.status(200).json({
      ok: false,
      message: err instanceof Error ? err.message : "Bootstrap warning",
    });
  }
}
