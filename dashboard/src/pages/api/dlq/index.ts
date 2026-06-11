import type { NextApiRequest, NextApiResponse } from "next";

import { supabaseServer } from "@/lib/supabaseClient";

export default async function handler(
  req: NextApiRequest,
  res: NextApiResponse,
) {
  if (req.method !== "GET") {
    res.setHeader("Allow", ["GET"]);
    return res.status(405).json({ error: `Method ${req.method} Not Allowed` });
  }

  const { status } = req.query;

  try {
    let query = supabaseServer
      .from("DlqEvent")
      .select("*")
      .order("createdAt", { ascending: false });

    if (status) {
      query = query.eq("status", String(status));
    }

    const { data: dlq, error } = await query;
    if (error) throw error;

    return res.status(200).json(dlq || []);
  } catch (err: unknown) {
    console.error("Error in /api/dlq handler:", err);
    return res
      .status(500)
      .json({ error: (err as unknown as { message: string }).message });
  }
}
