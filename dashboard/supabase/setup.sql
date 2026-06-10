-- Setup SQL Script for Supabase Database
-- Execute this script in your Supabase SQL Editor to initialize the required tables.

-- Enable UUID extension if not enabled
CREATE EXTENSION IF NOT EXISTS "uuid-ossp";

-- 1. Create Schema Table
CREATE TABLE IF NOT EXISTS "Schema" (
  id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  "subjectPattern" TEXT UNIQUE NOT NULL,
  schema JSONB NOT NULL,
  version INT DEFAULT 1 NOT NULL,
  description TEXT,
  "createdAt" TIMESTAMP WITH TIME ZONE DEFAULT timezone('utc'::text, now()) NOT NULL,
  "updatedAt" TIMESTAMP WITH TIME ZONE DEFAULT timezone('utc'::text, now()) NOT NULL
);

-- 2. Create DlqEvent Table
CREATE TABLE IF NOT EXISTS "DlqEvent" (
  id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  stream TEXT NOT NULL,
  subject TEXT NOT NULL,
  "consumerGroup" TEXT NOT NULL,
  payload JSONB NOT NULL,
  headers JSONB,
  "errorReason" TEXT NOT NULL,
  "numDelivered" INT NOT NULL,
  status TEXT NOT NULL, -- 'PENDING', 'REPLAYED', 'DISMISSED'
  "createdAt" TIMESTAMP WITH TIME ZONE DEFAULT timezone('utc'::text, now()) NOT NULL,
  "updatedAt" TIMESTAMP WITH TIME ZONE DEFAULT timezone('utc'::text, now()) NOT NULL
);

-- 3. Create ConsumerSimulatorConfig Table
CREATE TABLE IF NOT EXISTS "ConsumerSimulatorConfig" (
  id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  stream TEXT NOT NULL,
  subject TEXT NOT NULL,
  "durableName" TEXT UNIQUE NOT NULL,
  "successRate" DOUBLE PRECISION NOT NULL,
  "processingDelay" INT NOT NULL,
  "maxDeliver" INT DEFAULT 3 NOT NULL,
  active BOOLEAN DEFAULT false NOT NULL,
  "createdAt" TIMESTAMP WITH TIME ZONE DEFAULT timezone('utc'::text, now()) NOT NULL,
  "updatedAt" TIMESTAMP WITH TIME ZONE DEFAULT timezone('utc'::text, now()) NOT NULL
);

-- Disable Row Level Security (RLS) or enable public read/write access for simplicity on anon client
ALTER TABLE "Schema" DISABLE ROW LEVEL SECURITY;
ALTER TABLE "DlqEvent" DISABLE ROW LEVEL SECURITY;
ALTER TABLE "ConsumerSimulatorConfig" DISABLE ROW LEVEL SECURITY;

-- Alternatively, if you wish to keep RLS active, execute the following policies:
-- ALTER TABLE "Schema" ENABLE ROW LEVEL SECURITY;
-- CREATE POLICY "Public All Schema" ON "Schema" FOR ALL USING (true) WITH CHECK (true);
-- ALTER TABLE "DlqEvent" ENABLE ROW LEVEL SECURITY;
-- CREATE POLICY "Public All DlqEvent" ON "DlqEvent" FOR ALL USING (true) WITH CHECK (true);
-- ALTER TABLE "ConsumerSimulatorConfig" ENABLE ROW LEVEL SECURITY;
-- CREATE POLICY "Public All Simulator" ON "ConsumerSimulatorConfig" FOR ALL USING (true) WITH CHECK (true);
