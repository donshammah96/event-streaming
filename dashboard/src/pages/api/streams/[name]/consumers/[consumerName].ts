import type { NextApiRequest, NextApiResponse } from "next";
import { initNats, deleteConsumer } from "@/lib/nats";

export default async function handler(
  req: NextApiRequest,
  res: NextApiResponse
) {
  const { name, consumerName } = req.query;
  const streamName = String(name);
  const consumer = String(consumerName);

  if (req.method !== "DELETE") {
    res.setHeader("Allow", ["DELETE"]);
    return res.status(405).json({ error: `Method ${req.method} Not Allowed` });
  }

  try {
    await initNats();
    await deleteConsumer(streamName, consumer);
    return res.status(200).json({ success: true });
  } catch (err: any) {
    console.error(`Error in DELETE /api/streams/[name]/consumers/[consumerName] for ${streamName}/${consumer}:`, err);
    return res.status(500).json({ error: err.message });
  }
}
