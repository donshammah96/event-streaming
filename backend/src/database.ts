import { PrismaClient } from "@prisma/client";

/**
 * Production-hardened Prisma singleton.
 *
 * Connection pool configuration:
 *  - `connection_limit=10` prevents exhausting the Postgres max-connections
 *    under concurrent load. Tune via DATABASE_CONNECTION_LIMIT env var.
 *  - `pool_timeout=20` fails fast instead of queuing indefinitely.
 *
 * Development hot-reload protection:
 *  - Next.js and ts-node-dev restart the module on file changes, which would
 *    create a new PrismaClient (and new connection pool) on every reload.
 *    We store the singleton on the global object so it survives HMR cycles.
 */

const connectionLimit =
  parseInt(process.env.DATABASE_CONNECTION_LIMIT ?? "10", 10) || 10;

function createPrismaClient(): PrismaClient {
  const databaseUrl = process.env.DATABASE_URL;
  if (!databaseUrl) {
    throw new Error(
      "[database] DATABASE_URL environment variable is not set. " +
        "Configure it in your .env file or deployment environment.",
    );
  }

  // Append connection pool params to the URL if not already present.
  // PgBouncer / Supabase pooler users should set DATABASE_URL to the
  // pooler URL and rely on the pooler's own connection limits instead.
  const url = new URL(databaseUrl);
  if (!url.searchParams.has("connection_limit")) {
    url.searchParams.set("connection_limit", String(connectionLimit));
  }
  if (!url.searchParams.has("pool_timeout")) {
    url.searchParams.set("pool_timeout", "20");
  }

  return new PrismaClient({
    datasources: {
      db: { url: url.toString() },
    },
    log: process.env.NODE_ENV === "development" ? ["warn", "error"] : ["error"],
  });
}

// ── Dev singleton ────────────────────────────────────────────────────────────
// In development, attach the client to the global object so it persists across
// hot-module-reload cycles and avoids "too many clients" Postgres errors.
const globalForPrisma = global as typeof global & {
  _prismaClient?: PrismaClient;
};

export const prisma: PrismaClient =
  globalForPrisma._prismaClient ?? createPrismaClient();

if (process.env.NODE_ENV !== "production") {
  globalForPrisma._prismaClient = prisma;
}
