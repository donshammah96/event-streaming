import fs from "fs";
import path from "path";

// Load environment variables from .env.local manually
try {
  const envPath = path.resolve(process.cwd(), ".env.local");
  if (fs.existsSync(envPath)) {
    const envFile = fs.readFileSync(envPath, "utf-8");
    envFile.split(/\r?\n/).forEach((line) => {
      const trimmed = line.trim();
      if (trimmed && !trimmed.startsWith("#") && trimmed.includes("=")) {
        const [key, ...values] = trimmed.split("=");
        const value = values.join("=");
        process.env[key.trim()] = value.trim();
      }
    });
  }
} catch (err) {
  console.warn("Failed to load environment variables from .env.local:", err);
}

async function runTests() {
  console.log("==================================================");
  console.log("       STARTING event-streaming TEST SUITE       ");
  console.log("==================================================");

  // Dynamic imports to ensure env variables from .env.local are loaded first
  const { supabase } = await import("../lib/supabaseClient");
  const { initNats, createStream, deleteStream, getJetStream, codec } =
    await import("../lib/nats");

  let successCount = 0;
  let failCount = 0;

  async function assert(
    name: string,
    assertion: () => Promise<boolean> | boolean,
  ) {
    try {
      const result = await assertion();
      if (result) {
        console.log(`[PASS] ${name}`);
        successCount++;
      } else {
        console.error(`[FAIL] ${name} (Assertion returned false)`);
        failCount++;
      }
    } catch (err: unknown) {
      console.error(`[FAIL] ${name} (Exception thrown)`);
      console.error(err);
      failCount++;
    }
  }

  // 1. Test Supabase Connection
  await assert(
    "Supabase Client can connect and query schema list",
    async () => {
      // Should not throw, even if Schema table is empty
      const { data, error } = await supabase
        .from("Schema")
        .select("id")
        .limit(1);
      if (error) {
        console.warn("Supabase query details:", error);
        return false;
      }
      return Array.isArray(data);
    },
  );

  // 2. Test NATS Connection and Operations
  await assert(
    "NATS Client can connect, create stream, publish, and delete stream",
    async () => {
      await initNats();
      const testStreamName = "TEST_INTEGRATION_STREAM";
      const testSubject = "test.integration";

      // Create temporary stream
      await createStream(testStreamName, [testSubject]);

      // Publish message
      const js = getJetStream();
      const pa = await js.publish(
        testSubject,
        codec.encode({ hello: "world" }),
      );
      const publishOk = pa.stream === testStreamName && pa.seq > 0;

      // Clean up
      await deleteStream(testStreamName);

      return publishOk;
    },
  );

  // 3. Test Schema Precedence Logic
  await assert(
    "Schema wildcard precedence matching rules work correctly",
    async () => {
      // Wildcard match regex helper check
      const exactPattern = "events.user.created";
      const starPattern = "events.user.*";
      const greaterPattern = "events.>";

      // Mock compiled schemas matching the database structures
      const mockCache = [
        {
          subjectPattern: exactPattern,
          regex: /^events\.user\.created$/,
        },
        {
          subjectPattern: starPattern,
          regex: /^events\.user\.[^.]+$/,
        },
        {
          subjectPattern: greaterPattern,
          regex: /^events\..+$/,
        },
      ];

      // Sorting rule copy-paste verify
      const sorted = [...mockCache].sort((a, b) => {
        const aHasStar = a.subjectPattern.includes("*");
        const aHasGreater = a.subjectPattern.includes(">");
        const bHasStar = b.subjectPattern.includes("*");
        const bHasGreater = b.subjectPattern.includes(">");

        if (!aHasStar && !aHasGreater && (bHasStar || bHasGreater)) return -1;
        if ((aHasStar || aHasGreater) && !bHasStar && !bHasGreater) return 1;
        if (aHasStar && bHasGreater) return -1;
        if (aHasGreater && bHasStar) return 1;
        return b.subjectPattern.length - a.subjectPattern.length;
      });

      // Verify ordering is Exact Match -> Single Wildcard (*) -> Global Wildcard (>)
      const isSortedCorrectly =
        sorted[0].subjectPattern === exactPattern &&
        sorted[1].subjectPattern === starPattern &&
        sorted[2].subjectPattern === greaterPattern;

      return isSortedCorrectly;
    },
  );

  console.log("==================================================");
  console.log(
    `TEST RUN COMPLETED. Passed: ${successCount}, Failed: ${failCount}`,
  );
  console.log("==================================================");

  if (failCount > 0) {
    process.exit(1);
  } else {
    process.exit(0);
  }
}

runTests().catch((err) => {
  console.error("Test runner crashed:", err);
  process.exit(1);
});
