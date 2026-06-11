import type { NextApiRequest, NextApiResponse } from "next";

import { initNats, listStreams, createStream } from "@/lib/nats";

export default async function handler(
  req: NextApiRequest,
  res: NextApiResponse,
) {
  try {
    // Bootstrap NATS
    await initNats();

    if (req.method === "GET") {
      const list = await listStreams();
      return res.status(200).json(list);
    } else if (req.method === "POST") {
      const { name, subjects } = req.body;
      if (!name || !subjects || !Array.isArray(subjects)) {
        return res.status(400).json({
          error:
            "Missing required fields: name (string), subjects (array of strings)",
        });
      }
      const info = await createStream(name, subjects);
      return res.status(201).json(info);
    } else {
      res.setHeader("Allow", ["GET", "POST"]);
      return res
        .status(405)
        .json({ error: `Method ${req.method} Not Allowed` });
    }
  } catch (err: unknown) {
    console.error("Error in /api/streams handler:", err);
    return res
      .status(500)
      .json({ error: (err as unknown as { message: string }).message });
  }
}
