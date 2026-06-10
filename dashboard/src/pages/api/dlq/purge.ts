import type { NextApiRequest, NextApiResponse } from "next";
import { supabase } from "@/lib/supabaseClient";

export default async function handler(
  req: NextApiRequest,
  res: NextApiResponse,
) {
  if (req.method !== "POST") {
    res.setHeader("Allow", ["POST"]);
    return res.status(405).json({ error: `Method ${req.method} Not Allowed` });
  }

  try {
    // Delete all records (all UUIDs that are not equal to nil-uuid)
    const { error } = await supabase
      .from("DlqEvent")
      .delete()
      .neq("id", "00000000-0000-0000-0000-000000000000");

    if (error) throw error;
    return res.status(200).json({ success: true });
  } catch (err: unknown) {
    console.error("Error in /api/dlq/purge handler:", err);
    return res
      .status(500)
      .json({ error: (err as unknown as { message: string }).message });
  }
}
