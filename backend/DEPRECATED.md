# DEPRECATED — Legacy Express Backend

> [!WARNING]
> This directory is **no longer active**. It is preserved for portfolio reference to demonstrate the pre-migration architecture. Do not use it in production.

## What this was

The `backend/` Express server was the original implementation of the NATS event streaming platform before the full migration to the Next.js API routes + Supabase stack in the `dashboard/` application.

It provided:

- REST API routes for streams, consumers, schemas, DLQ, and simulators
- A WebSocket gateway for real-time stats and log streaming
- Direct Prisma + PostgreSQL integration

## Why it was deprecated

| Concern | Legacy Express `backend/` | Current `dashboard/` |
|---------|--------------------------|---------------------|
| Runtime | Long-lived Node.js process | Next.js serverless (Vercel-compatible) |
| Real-time | Raw WebSocket server (port 3001) | Supabase Realtime channels |
| Database | Prisma + PostgreSQL (direct) | Supabase client (connection-pooled) |
| Deployment | Self-hosted only | Vercel, Fly.io, or self-hosted |
| Duplication | Separate codebase | Unified in `dashboard/` |

## Migration summary

All API surface area from `backend/src/routes.ts` is now exposed via `dashboard/src/pages/api/`. The database backend switched from Prisma + direct PostgreSQL to Supabase (PostgreSQL + Realtime). The WebSocket gateway was replaced by Supabase Realtime for log streaming and REST polling for stats.

## Credentials warning

The `backend/.env` file previously may have contained real Supabase or PostgreSQL credentials. That file is now gitignored via `backend/.gitignore`. **Rotate any credentials that were ever committed to this file.**
