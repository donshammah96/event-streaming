import type { NextApiRequest, NextApiResponse } from "next";

import { initNats, listStreams } from "@/lib/nats";
import { supabaseServer } from "@/lib/supabaseClient";

export interface StatsPayload {
  streams: { name: string; subjects: string[] }[];
  activeSimulators: number;
  pendingDlq: number;
  registeredSchemas: number;
  timestamp: string;
}

/**
 * GET /api/stats — Returns a lightweight system-state snapshot.
 *
 * This endpoint replaces the WebSocket stats polling loop for Vercel deployments.
 * The frontend polls this route every 3 seconds. On Vercel, each invocation is
 * a fresh serverless function call — no persistent state is assumed.
 */
export default async function handler(
  req: NextApiRequest,
  res: NextApiResponse<StatsPayload | { error: string }>,
) {
  if (req.method !== "GET") {
    res.setHeader("Allow", ["GET"]);
    return res.status(405).json({ error: `Method ${req.method} Not Allowed` });
  }

  try {
    // 1. NATS stream list — tolerate failure gracefully
    let streams: { name: string; subjects: string[] }[] = [];
    try {
      await initNats();
      const list = await listStreams();
      streams = list.map((s) => ({
        name: s.name ?? "",
        subjects: s.subjects ?? [],
      }));
    } catch {
      // NATS may not be available in all environments — degrade gracefully
    }

    // 2. Aggregate counts from Supabase in parallel
    const [simResult, dlqResult, schemaResult] = await Promise.all([
      supabaseServer
        .from("ConsumerSimulatorConfig")
        .select("*", { count: "exact", head: true })
        .eq("active", true),
      supabaseServer
        .from("DlqEvent")
        .select("*", { count: "exact", head: true })
        .eq("status", "PENDING"),
      supabaseServer.from("Schema").select("*", { count: "exact", head: true }),
    ]);

    const payload: StatsPayload = {
      streams,
      activeSimulators: simResult.count ?? 0,
      pendingDlq: dlqResult.count ?? 0,
      registeredSchemas: schemaResult.count ?? 0,
      timestamp: new Date().toISOString(),
    };

    // No-cache — always return fresh data
    res.setHeader("Cache-Control", "no-store, max-age=0");
    return res.status(200).json(payload);
  } catch (err) {
    console.error("[/api/stats] Error fetching system stats:", err);
    return res.status(500).json({
      error: err instanceof Error ? err.message : "Internal server error",
    });
  }
}
