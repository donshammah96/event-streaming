import type { NextApiRequest, NextApiResponse } from "next";
import { supabase } from "@/lib/supabaseClient";

export default async function handler(
  req: NextApiRequest,
  res: NextApiResponse,
) {
  const { id: queryId } = req.query;
  const id = String(queryId);

  if (req.method !== "POST") {
    res.setHeader("Allow", ["POST"]);
    return res.status(405).json({ error: `Method ${req.method} Not Allowed` });
  }

  try {
    const { data: updated, error } = await supabase
      .from("DlqEvent")
      .update({
        status: "DISMISSED",
        updatedAt: new Date().toISOString(),
      })
      .eq("id", id)
      .select()
      .single();

    if (error) throw error;
    return res.status(200).json(updated);
  } catch (err: unknown) {
    console.error(`Error in DLQ dismiss handler for event ${id}:`, err);
    return res
      .status(500)
      .json({ error: (err as unknown as { message: string }).message });
  }
}
