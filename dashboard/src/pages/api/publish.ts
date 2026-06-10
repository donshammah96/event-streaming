import type { NextApiRequest, NextApiResponse } from "next";
import { headers as natsHeaders } from "nats";
import { initNats, getJetStream, codec } from "@/lib/nats";
import { ensureSchemaCache, validateMessage } from "@/lib/schema";

export default async function handler(
  req: NextApiRequest,
  res: NextApiResponse,
) {
  if (req.method !== "POST") {
    res.setHeader("Allow", ["POST"]);
    return res.status(405).json({ error: `Method ${req.method} Not Allowed` });
  }

  const { subject, payload, headers } = req.body;
  if (!subject || payload === undefined) {
    return res.status(400).json({
      error: "Missing required fields: subject (string), payload (any)",
    });
  }

  try {
    await initNats();
    await ensureSchemaCache();

    // Schema Validation
    const validation = validateMessage(subject, payload);
    if (!validation.valid) {
      return res.status(400).json({
        error: "Schema validation failed",
        errors: validation.errors,
        matchedPattern: validation.matchedPattern,
      });
    }

    const js = getJetStream();

    // Construct NATS headers
    const opt: any = {};
    if (headers && typeof headers === "object") {
      const nHeaders = natsHeaders();
      for (const [key, value] of Object.entries(headers)) {
        nHeaders.append(key, String(value));
      }
      opt.headers = nHeaders;
    }

    const pa = await js.publish(subject, codec.encode(payload), opt);

    return res.status(200).json({
      success: true,
      seq: pa.seq,
      stream: pa.stream,
      duplicate: pa.duplicate,
      matchedPattern: validation.matchedPattern || null,
    });
  } catch (err: unknown) {
    console.error("Error in /api/publish handler:", err);
    return res
      .status(500)
      .json({ error: (err as unknown as { message: string }).message });
  }
}
