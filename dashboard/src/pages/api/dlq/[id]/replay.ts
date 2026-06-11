import { JetStreamPublishOptions, headers as natsHeaders } from "nats";
import type { NextApiRequest, NextApiResponse } from "next";

import { initNats, getJetStream, codec } from "@/lib/nats";
import { supabaseServer } from "@/lib/supabaseClient";

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

  const { targetSubject } = req.body;

  try {
    await initNats();

    // Fetch original event from Supabase
    const { data: event, error: fetchErr } = await supabaseServer
      .from("DlqEvent")
      .select("*")
      .eq("id", id)
      .single();

    if (fetchErr || !event) {
      return res.status(404).json({ error: "DLQ event not found" });
    }

    const js = getJetStream();
    const subject = targetSubject || event.subject;

    // Build headers from original event headers
    const opt: Partial<JetStreamPublishOptions> = {};
    const nHeaders = natsHeaders();

    if (event.headers && typeof event.headers === "object") {
      for (const [key, value] of Object.entries(event.headers)) {
        nHeaders.append(key, String(value));
      }
    }

    // Set tracking header
    nHeaders.set("X-Event-Replayed-From-DLQ", event.id);
    opt.headers = nHeaders;

    // Publish to NATS
    const pa = await js.publish(subject, codec.encode(event.payload), opt);

    // Update status in Supabase to REPLAYED
    const { data: updated, error: updateErr } = await supabaseServer
      .from("DlqEvent")
      .update({
        status: "REPLAYED",
        updatedAt: new Date().toISOString(),
      })
      .eq("id", id)
      .select()
      .single();

    if (updateErr) throw updateErr;

    return res.status(200).json({
      success: true,
      seq: pa.seq,
      stream: pa.stream,
      event: updated,
    });
  } catch (err: unknown) {
    console.error(`Error in DLQ replay handler for event ${id}:`, err);
    return res
      .status(500)
      .json({ error: (err as unknown as { message: string }).message });
  }
}
