import type { NextApiRequest, NextApiResponse } from "next";
import { initNats, listConsumers, createConsumer } from "@/lib/nats";

export default async function handler(
  req: NextApiRequest,
  res: NextApiResponse,
) {
  const { name } = req.query;
  const streamName = String(name);

  try {
    await initNats();

    if (req.method === "GET") {
      const list = await listConsumers(streamName);
      return res.status(200).json(list);
    } else if (req.method === "POST") {
      const { durableName, filterSubject, maxDeliver } = req.body;
      if (!durableName) {
        return res
          .status(400)
          .json({ error: "Missing required field: durableName (string)" });
      }

      const info = await createConsumer(streamName, {
        durableName,
        filterSubject,
        maxDeliver: maxDeliver ? parseInt(String(maxDeliver)) : undefined,
      });
      return res.status(201).json(info);
    } else {
      res.setHeader("Allow", ["GET", "POST"]);
      return res
        .status(405)
        .json({ error: `Method ${req.method} Not Allowed` });
    }
  } catch (err: unknown) {
    console.error(
      `Error in /api/streams/[name]/consumers handler for ${streamName}:`,
      err,
    );
    return res
      .status(500)
      .json({ error: (err as unknown as { message: string }).message });
  }
}
