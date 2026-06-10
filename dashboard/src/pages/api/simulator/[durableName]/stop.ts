import type { NextApiRequest, NextApiResponse } from "next";
import { stopSimulator } from "@/lib/simulator";
import { initNats } from "@/lib/nats";

export default async function handler(
  req: NextApiRequest,
  res: NextApiResponse
) {
  const { durableName } = req.query;
  const name = String(durableName);

  if (req.method !== "POST") {
    res.setHeader("Allow", ["POST"]);
    return res.status(405).json({ error: `Method ${req.method} Not Allowed` });
  }

  try {
    await initNats();
    await stopSimulator(name);
    return res.status(200).json({ success: true });
  } catch (err: any) {
    console.error(`Error in /api/simulator/[durableName]/stop for ${name}:`, err);
    return res.status(500).json({ error: err.message });
  }
}
