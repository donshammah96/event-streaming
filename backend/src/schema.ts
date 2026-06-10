import Ajv, { ValidateFunction } from "ajv";
import addFormats from "ajv-formats";
import { prisma } from "./database";

const ajv = new Ajv({ allErrors: true, strict: false });
addFormats(ajv);

interface CachedSchema {
  id: string;
  subjectPattern: string;
  schema: any;
  regex: RegExp;
  validator: ValidateFunction;
}

let schemaCache: CachedSchema[] = [];

// Helper to convert NATS subject wildcards to Regular Expressions
function wildcardToRegex(pattern: string): RegExp {
  const parts = pattern.split(".");
  const regexParts = parts.map(part => {
    if (part === "*") {
      return "[^.]+"; // Matches a single subject token
    } else if (part === ">") {
      return ".+";    // Matches one or more trailing tokens
    } else {
      // Escape special characters in exact matches
      return part.replace(/[-\/\\^$*+?.()|[\]{}]/g, "\\$&");
    }
  });
  return new RegExp(`^${regexParts.join("\\.")}$`);
}

// Reload and compile schemas from PostgreSQL
export async function reloadSchemaCache() {
  try {
    const dbSchemas = await prisma.schema.findMany();
    const newCache: CachedSchema[] = [];
    
    for (const dbSchema of dbSchemas) {
      try {
        const schemaObj = typeof dbSchema.schema === "string" 
          ? JSON.parse(dbSchema.schema) 
          : dbSchema.schema;
          
        const validator = ajv.compile(schemaObj);
        newCache.push({
          id: dbSchema.id,
          subjectPattern: dbSchema.subjectPattern,
          schema: schemaObj,
          regex: wildcardToRegex(dbSchema.subjectPattern),
          validator
        });
      } catch (compileErr) {
        console.error(`Failed to compile schema for pattern ${dbSchema.subjectPattern}:`, compileErr);
      }
    }
    
    // Sort cache so exact matches (no wildcards) are prioritized, then '*' wildcards, then '>' wildcards
    newCache.sort((a, b) => {
      const aHasStar = a.subjectPattern.includes("*");
      const aHasGreater = a.subjectPattern.includes(">");
      const bHasStar = b.subjectPattern.includes("*");
      const bHasGreater = b.subjectPattern.includes(">");
      
      if (!aHasStar && !aHasGreater && (bHasStar || bHasGreater)) return -1;
      if ((aHasStar || aHasGreater) && !bHasStar && !bHasGreater) return 1;
      if (aHasStar && bHasGreater) return -1;
      if (aHasGreater && bHasStar) return 1;
      return b.subjectPattern.length - a.subjectPattern.length; // Tie-breaker: longer pattern first
    });

    schemaCache = newCache;
    console.log(`Loaded and compiled ${schemaCache.length} validation schema(s) from database`);
  } catch (err) {
    console.error("Failed to load schema cache:", err);
  }
}

// Find matching schema in cache
export function findMatchingSchema(subject: string): CachedSchema | null {
  for (const cached of schemaCache) {
    if (cached.regex.test(subject)) {
      return cached;
    }
  }
  return null;
}

// Validate payload against matched schema
export interface ValidationResult {
  valid: boolean;
  errors?: string[];
  matchedPattern?: string;
}

export function validateMessage(subject: string, payload: any): ValidationResult {
  const match = findMatchingSchema(subject);
  if (!match) {
    // If no schema matches, we default to passing validation (schema-less publishing)
    return { valid: true };
  }
  
  const isValid = match.validator(payload);
  if (isValid) {
    return { valid: true, matchedPattern: match.subjectPattern };
  } else {
    const errors = match.validator.errors?.map(
      err => `${err.instancePath || "root"} ${err.message}`
    ) || ["Unknown validation error"];
    return {
      valid: false,
      errors,
      matchedPattern: match.subjectPattern
    };
  }
}
