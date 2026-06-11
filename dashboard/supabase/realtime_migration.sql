-- Supabase Realtime Migration for Simulator Logs
-- Execute this in your Supabase SQL Editor after running setup.sql.
--
-- This replaces the Node.js WebSocket-based log streaming with Supabase Realtime,
-- making the dashboard compatible with Vercel's serverless runtime.

-- 1. Create SimulatorLog table for real-time log streaming
CREATE TABLE IF NOT EXISTS "SimulatorLog" (
  id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  "durableName" TEXT NOT NULL,
  timestamp TIMESTAMP WITH TIME ZONE DEFAULT timezone('utc'::text, now()) NOT NULL,
  text TEXT NOT NULL,
  type TEXT NOT NULL CHECK (type IN ('info', 'success', 'warn', 'error')),
  "createdAt" TIMESTAMP WITH TIME ZONE DEFAULT timezone('utc'::text, now()) NOT NULL
);

-- Index for efficient filtering by simulator and timestamp
CREATE INDEX IF NOT EXISTS idx_simulatorlog_durablename ON "SimulatorLog" ("durableName");
CREATE INDEX IF NOT EXISTS idx_simulatorlog_createdat ON "SimulatorLog" ("createdAt" DESC);

-- Disable RLS for anon client access (consistent with other tables)
ALTER TABLE "SimulatorLog" DISABLE ROW LEVEL SECURITY;

-- 2. Prune old logs automatically — keep only the last 1000 rows per simulator
-- This function runs as a trigger after each insert to prevent unbounded table growth.
CREATE OR REPLACE FUNCTION prune_simulator_logs()
RETURNS TRIGGER AS $$
BEGIN
  DELETE FROM "SimulatorLog"
  WHERE "durableName" = NEW."durableName"
    AND id NOT IN (
      SELECT id FROM "SimulatorLog"
      WHERE "durableName" = NEW."durableName"
      ORDER BY "createdAt" DESC
      LIMIT 1000
    );
  RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER trigger_prune_simulator_logs
  AFTER INSERT ON "SimulatorLog"
  FOR EACH ROW
  EXECUTE FUNCTION prune_simulator_logs();

-- 3. Enable Supabase Realtime publication for SimulatorLog
-- This allows the frontend to subscribe to INSERT events on this table.
ALTER PUBLICATION supabase_realtime ADD TABLE "SimulatorLog";

-- 4. Add DlqStatus index for performance (if running this after initial setup)
CREATE INDEX IF NOT EXISTS idx_dlqevent_status ON "DlqEvent" (status);
CREATE INDEX IF NOT EXISTS idx_simulatorconfig_active ON "ConsumerSimulatorConfig" (active);
