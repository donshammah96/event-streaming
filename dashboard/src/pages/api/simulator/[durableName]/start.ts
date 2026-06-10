import type { NextApiRequest, NextApiResponse } from "next";
import { startSimulator } from "@/lib/simulator";
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
    // Ensure NATS is connected
    await initNats();
    await startSimulator(name);
    return res.status(200).json({ success: true });
  } catch (err: any) {
    console.error(`Error in /api/simulator/[durableName]/start for ${name}:`, err);
    return res.status(500).json({ error: err.message });
  }
}
