import type { NextApiRequest, NextApiResponse } from "next";

import { reloadSchemaCache, validateSchemaDefinition } from "@/lib/schema";
import { supabaseServer } from "@/lib/supabaseClient";

export default async function handler(
  req: NextApiRequest,
  res: NextApiResponse,
) {
  const { schemaId } = req.query;
  const id = String(schemaId);

  try {
    if (req.method === "PUT") {
      const { schema, description } = req.body;
      if (!schema) {
        return res
          .status(400)
          .json({ error: "Missing required field: schema (object)" });
      }

      // Compile check with Ajv
      try {
        validateSchemaDefinition(schema);
      } catch (compileErr: unknown) {
        return res.status(400).json({
          error: `Invalid JSON Schema definition: ${(compileErr as unknown as { message: string }).message}`,
        });
      }

      // Fetch current version
      const { data: current, error: fetchErr } = await supabaseServer
        .from("Schema")
        .select("*")
        .eq("id", id)
        .single();

      if (fetchErr || !current) {
        return res.status(404).json({ error: "Schema not found" });
      }

      const { data: updated, error: updateErr } = await supabaseServer
        .from("Schema")
        .update({
          schema,
          description,
          version: (current.version || 1) + 1,
          updatedAt: new Date().toISOString(),
        })
        .eq("id", id)
        .select()
        .single();

      if (updateErr) throw updateErr;

      // Reload cache
      await reloadSchemaCache();

      return res.status(200).json(updated);
    } else if (req.method === "DELETE") {
      const { error } = await supabaseServer
        .from("Schema")
        .delete()
        .eq("id", id);
      if (error) throw error;

      // Reload cache
      await reloadSchemaCache();

      return res.status(200).json({ success: true });
    } else {
      res.setHeader("Allow", ["PUT", "DELETE"]);
      return res
        .status(405)
        .json({ error: `Method ${req.method} Not Allowed` });
    }
  } catch (err: unknown) {
    console.error(`Error in /api/schemas/[id] handler for ${id}:`, err);
    return res
      .status(500)
      .json({ error: (err as unknown as { message: string }).message });
  }
}
