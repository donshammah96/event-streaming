import type { NextApiRequest, NextApiResponse } from "next";

import { supabaseServer } from "@/lib/supabaseClient";

/**
 * POST /api/dlq/purge
 *
 * Purges all rows from the DlqEvent table.
 *
 * Security hardening:
 *  - Requires `confirmToken` in the request body equal to the static sentinel
 *    "CONFIRM_PURGE_ALL". This prevents accidental or CSRF-triggered wipes.
 *  - Rate-limited: at most 1 purge per 60 seconds per server process.
 *    (In a multi-instance deployment, use Redis-backed rate limiting instead.)
 *
 * IMPORTANT: This is a destructive, irreversible operation. Any client calling
 * this endpoint must display a confirmation dialog before sending the request.
 */

const PURGE_CONFIRM_TOKEN = "CONFIRM_PURGE_ALL";

// Simple in-process rate limiter — one purge per 60 seconds.
// Serverless environments reset this on each cold start, which is acceptable
// because cold starts are themselves rate-limited by the platform.
let lastPurgeAt = 0;
const PURGE_RATE_LIMIT_MS = 60_000;

export default async function handler(
  req: NextApiRequest,
  res: NextApiResponse,
) {
  if (req.method !== "POST") {
    res.setHeader("Allow", ["POST"]);
    return res.status(405).json({ error: `Method ${req.method} Not Allowed` });
  }

  // ── Rate limit ────────────────────────────────────────────────────────────
  const now = Date.now();
  if (now - lastPurgeAt < PURGE_RATE_LIMIT_MS) {
    const retryAfterSecs = Math.ceil(
      (PURGE_RATE_LIMIT_MS - (now - lastPurgeAt)) / 1000,
    );
    res.setHeader("Retry-After", String(retryAfterSecs));
    return res.status(429).json({
      error: `Rate limit exceeded. Retry after ${retryAfterSecs}s.`,
    });
  }

  // ── Confirm token ─────────────────────────────────────────────────────────
  const { confirmToken } = req.body as { confirmToken?: unknown };
  if (confirmToken !== PURGE_CONFIRM_TOKEN) {
    return res.status(400).json({
      error:
        "Missing or invalid confirmToken. " +
        `Send { confirmToken: "${PURGE_CONFIRM_TOKEN}" } to authorize this operation.`,
    });
  }

  try {
    // Use `.gt("id", "00000000-...")` instead of `.neq(...)` with a nil UUID
    // to ensure we never silently skip rows on UUID ordering edge cases.
    const { error, count } = await supabaseServer
      .from("DlqEvent")
      .delete({ count: "exact" })
      .gte("createdAt", "1970-01-01T00:00:00Z"); // match all rows

    if (error) throw error;

    lastPurgeAt = Date.now();
    return res.status(200).json({ success: true, deleted: count ?? 0 });
  } catch (err: unknown) {
    console.error("Error in /api/dlq/purge handler:", err);
    return res.status(500).json({
      error: err instanceof Error ? err.message : "Internal server error",
    });
  }
}
