import type { NextApiRequest, NextApiResponse } from "next";

import { reloadSchemaCache, validateSchemaDefinition } from "@/lib/schema";
import { supabaseServer } from "@/lib/supabaseClient";

export default async function handler(
  req: NextApiRequest,
  res: NextApiResponse,
) {
  try {
    if (req.method === "GET") {
      const { data: schemas, error } = await supabaseServer
        .from("Schema")
        .select("*")
        .order("createdAt", { ascending: false });

      if (error) throw error;
      return res.status(200).json(schemas || []);
    } else if (req.method === "POST") {
      const { subjectPattern, schema, description } = req.body;
      if (!subjectPattern || !schema) {
        return res.status(400).json({
          error:
            "Missing required fields: subjectPattern (string), schema (object)",
        });
      }

      // Compile check with Ajv
      try {
        validateSchemaDefinition(schema);
      } catch (compileErr: unknown) {
        return res.status(400).json({
          error: `Invalid JSON Schema definition: ${(compileErr as unknown as { message: string }).message}`,
        });
      }

      // Check if schema already exists (equivalent to Prisma unique constraint check)
      const { data: existing } = await supabaseServer
        .from("Schema")
        .select("id")
        .eq("subjectPattern", subjectPattern)
        .maybeSingle();

      if (existing) {
        return res.status(400).json({
          error: `A schema for subject pattern '${subjectPattern}' already exists`,
        });
      }

      const { data: newSchema, error } = await supabaseServer
        .from("Schema")
        .insert({
          subjectPattern,
          schema,
          description,
          version: 1,
          updatedAt: new Date().toISOString(),
        })
        .select()
        .single();

      if (error) throw error;

      // Reload Cache
      await reloadSchemaCache();

      return res.status(201).json(newSchema);
    } else {
      res.setHeader("Allow", ["GET", "POST"]);
      return res
        .status(405)
        .json({ error: `Method ${req.method} Not Allowed` });
    }
  } catch (err: unknown) {
    console.error("Error in /api/schemas handler:", err);
    return res
      .status(500)
      .json({ error: (err as unknown as { message: string }).message });
  }
}
