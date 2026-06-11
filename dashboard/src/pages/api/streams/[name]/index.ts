import type { NextApiRequest, NextApiResponse } from "next";

import { initNats, getStreamInfo, deleteStream } from "@/lib/nats";

export default async function handler(
  req: NextApiRequest,
  res: NextApiResponse,
) {
  const { name } = req.query;
  const streamName = String(name);

  try {
    await initNats();

    if (req.method === "GET") {
      const info = await getStreamInfo(streamName);
      if (!info) {
        return res.status(404).json({ error: "Stream not found" });
      }
      return res.status(200).json(info);
    } else if (req.method === "DELETE") {
      await deleteStream(streamName);
      return res.status(200).json({ success: true });
    } else {
      res.setHeader("Allow", ["GET", "DELETE"]);
      return res
        .status(405)
        .json({ error: `Method ${req.method} Not Allowed` });
    }
  } catch (err: unknown) {
    console.error(
      `Error in /api/streams/[name] handler for ${streamName}:`,
      err,
    );
    return res.status(500).json({ error: (err as Error).message });
  }
}
