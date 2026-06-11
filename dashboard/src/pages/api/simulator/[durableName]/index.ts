import type { NextApiRequest, NextApiResponse } from "next";

import { startSimulator, stopSimulator } from "@/lib/simulator";
import { supabaseServer } from "@/lib/supabaseClient";

export default async function handler(
  req: NextApiRequest,
  res: NextApiResponse,
) {
  const { durableName: queryDurableName } = req.query;
  const durableName = String(queryDurableName);

  try {
    if (req.method === "PUT") {
      const { successRate, processingDelay, maxDeliver, subject } = req.body;
      const data: Record<string, unknown> = {
        updatedAt: new Date().toISOString(),
      };

      if (successRate !== undefined)
        data.successRate = parseFloat(String(successRate));
      if (processingDelay !== undefined)
        data.processingDelay = parseInt(String(processingDelay));
      if (maxDeliver !== undefined)
        data.maxDeliver = parseInt(String(maxDeliver));
      if (subject !== undefined) data.subject = subject;

      const { data: updated, error } = await supabaseServer
        .from("ConsumerSimulatorConfig")
        .update(data)
        .eq("durableName", durableName)
        .select()
        .single();

      if (error) throw error;

      // If active, stop and restart so configuration is reloaded
      const { data: config } = await supabaseServer
        .from("ConsumerSimulatorConfig")
        .select("active")
        .eq("durableName", durableName)
        .single();

      if (config?.active) {
        await stopSimulator(durableName);
        await startSimulator(durableName);
      }

      return res.status(200).json(updated);
    } else if (req.method === "DELETE") {
      await stopSimulator(durableName);

      const { error } = await supabaseServer
        .from("ConsumerSimulatorConfig")
        .delete()
        .eq("durableName", durableName);

      if (error) throw error;

      return res.status(200).json({ success: true });
    } else {
      res.setHeader("Allow", ["PUT", "DELETE"]);
      return res
        .status(405)
        .json({ error: `Method ${req.method} Not Allowed` });
    }
  } catch (err: unknown) {
    console.error(
      `Error in /api/simulator/[durableName] handler for ${durableName}:`,
      err,
    );
    return res.status(500).json({ error: (err as Error).message });
  }
}
