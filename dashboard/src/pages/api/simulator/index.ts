import type { NextApiRequest, NextApiResponse } from "next";
import { supabase } from "@/lib/supabaseClient";

export default async function handler(
  req: NextApiRequest,
  res: NextApiResponse,
) {
  try {
    if (req.method === "GET") {
      const { data: configs, error } = await supabase
        .from("ConsumerSimulatorConfig")
        .select("*")
        .order("createdAt", { ascending: false });

      if (error) throw error;
      return res.status(200).json(configs || []);
    } else if (req.method === "POST") {
      const {
        stream,
        subject,
        durableName,
        successRate,
        processingDelay,
        maxDeliver,
      } = req.body;

      if (
        !stream ||
        !subject ||
        !durableName ||
        successRate === undefined ||
        processingDelay === undefined
      ) {
        return res.status(400).json({
          error:
            "Missing required fields: stream, subject, durableName, successRate (float), processingDelay (int)",
        });
      }

      // Check if durableName already exists
      const { data: existing } = await supabase
        .from("ConsumerSimulatorConfig")
        .select("id")
        .eq("durableName", durableName)
        .maybeSingle();

      if (existing) {
        return res.status(400).json({
          error: `A consumer group with name '${durableName}' is already registered`,
        });
      }

      const { data: config, error } = await supabase
        .from("ConsumerSimulatorConfig")
        .insert({
          stream,
          subject,
          durableName,
          successRate: parseFloat(String(successRate)),
          processingDelay: parseInt(String(processingDelay)),
          maxDeliver: maxDeliver ? parseInt(String(maxDeliver)) : 3,
          active: false,
          updatedAt: new Date().toISOString(),
        })
        .select()
        .single();

      if (error) throw error;

      return res.status(201).json(config);
    } else {
      res.setHeader("Allow", ["GET", "POST"]);
      return res
        .status(405)
        .json({ error: `Method ${req.method} Not Allowed` });
    }
  } catch (err: unknown) {
    console.error("Error in /api/simulator handler:", err);
    return res
      .status(500)
      .json({ error: (err as unknown as { message: string }).message });
  }
}
