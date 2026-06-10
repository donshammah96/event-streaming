import type { NextApiRequest, NextApiResponse } from "next";
import { initNats, getStreamInfo, getStreamMessage } from "@/lib/nats";

export default async function handler(
  req: NextApiRequest,
  res: NextApiResponse,
) {
  const { name, limit: queryLimit, offsetSeq: queryOffsetSeq } = req.query;
  const streamName = String(name);
  const limit = parseInt(String(queryLimit || "20")) || 20;

  try {
    await initNats();

    const info = await getStreamInfo(streamName);
    if (!info) {
      return res.status(404).json({ error: "Stream not found" });
    }

    const { first_seq, last_seq } = info.state;
    if (last_seq === 0) {
      return res.status(200).json({ messages: [], nextOffsetSeq: null });
    }

    let currentSeq = queryOffsetSeq
      ? parseInt(String(queryOffsetSeq))
      : last_seq;
    const messages: unknown[] = [];
    let count = 0;

    // Fetch messages sequentially backwards
    while (currentSeq >= first_seq && count < limit) {
      const msg = await getStreamMessage(streamName, currentSeq);
      if (msg) {
        messages.push(msg);
        count++;
      }
      currentSeq--;
    }

    return res.status(200).json({
      messages,
      nextOffsetSeq: currentSeq >= first_seq ? currentSeq : null,
    });
  } catch (err: unknown) {
    console.error(
      `Error in /api/streams/[name]/messages handler for ${streamName}:`,
      err,
    );
    return res
      .status(500)
      .json({ error: (err as unknown as { message: string }).message });
  }
}
