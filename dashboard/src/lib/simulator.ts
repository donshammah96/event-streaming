import { EventEmitter } from "events";

import { JetStreamSubscription, consumerOpts } from "nats";

import { getJetStream, codec, createConsumer } from "./nats";
import { supabaseServer } from "./supabaseClient";

/**
 * Internal metrics emitter — used only for in-process metrics events.
 * Simulator logs are now persisted to Supabase `SimulatorLog` table and
 * streamed to the frontend via Supabase Realtime instead of WebSocket.
 */
export let simulatorEvents = new EventEmitter();
let activeSubscriptions: Record<string, JetStreamSubscription> = {};

// Hot-reload protection for simulator singleton states in development
if (process.env.NODE_ENV === "development") {
  const g = global as unknown as {
    _simulatorEvents?: EventEmitter;
    _activeSubscriptions?: Record<string, JetStreamSubscription>;
  };
  if (!g._simulatorEvents) {
    g._simulatorEvents = new EventEmitter();
    g._activeSubscriptions = {};
  }
  simulatorEvents = g._simulatorEvents;
  activeSubscriptions = g._activeSubscriptions ?? {};
}

type LogType = "info" | "success" | "warn" | "error";

/**
 * Writes a simulator log entry to Supabase `SimulatorLog`.
 * The frontend subscribes to this table via Supabase Realtime.
 * Failures are swallowed to never interrupt the simulator processing loop.
 */
async function logToDashboard(
  durableName: string,
  text: string,
  type: LogType = "info",
): Promise<void> {
  try {
    await supabaseServer.from("SimulatorLog").insert({
      durableName,
      timestamp: new Date().toISOString(),
      text,
      type,
    });
  } catch (err) {
    // Logging must never crash the simulator loop
    console.error(`[simulator] Failed to write log for ${durableName}:`, err);
  }
}

export async function startSimulator(durableName: string): Promise<void> {
  if (activeSubscriptions[durableName]) {
    await logToDashboard(durableName, "Simulator is already running", "warn");
    return;
  }

  // Fetch config from Supabase using the server client
  const { data: config, error: configError } = await supabaseServer
    .from("ConsumerSimulatorConfig")
    .select("*")
    .eq("durableName", durableName)
    .single();

  if (configError || !config) {
    throw new Error(
      `Consumer simulator config not found for durable name: ${durableName}`,
    );
  }

  // Ensure the NATS durable consumer exists
  try {
    await createConsumer(config.stream, {
      durableName: config.durableName,
      filterSubject: config.subject,
      maxDeliver: config.maxDeliver,
    });
  } catch (err) {
    // Consumer may already exist — log and continue
    console.warn(
      `[simulator] Could not ensure NATS consumer for ${durableName}:`,
      err,
    );
  }

  try {
    const js = getJetStream();
    const opts = consumerOpts();
    opts.durable(config.durableName);
    opts.manualAck();
    opts.ackExplicit();
    opts.maxDeliver(config.maxDeliver);

    const sub = await js.subscribe(config.subject, opts);
    activeSubscriptions[durableName] = sub;

    // Mark active in Supabase
    await supabaseServer
      .from("ConsumerSimulatorConfig")
      .update({ active: true, updatedAt: new Date().toISOString() })
      .eq("durableName", durableName);

    await logToDashboard(
      durableName,
      `Simulator started. Listening on subject '${config.subject}'...`,
      "info",
    );

    // Background message processing loop — fire and forget
    void (async () => {
      try {
        for await (const msg of sub) {
          // Re-fetch config to pick up live changes (active flag, rates)
          const { data: freshConfig } = await supabaseServer
            .from("ConsumerSimulatorConfig")
            .select("*")
            .eq("durableName", durableName)
            .single();

          if (!freshConfig || !freshConfig.active) {
            msg.nak();
            break;
          }

          let payload: unknown = null;
          try {
            payload = codec.decode(msg.data);
          } catch {
            payload = msg.data.toString();
          }

          const attempts = msg.info?.redeliveryCount ?? 1;
          await logToDashboard(
            durableName,
            `[Received] Event seq ${msg.seq} on subject '${msg.subject}' (Attempt ${attempts}/${freshConfig.maxDeliver})`,
            "info",
          );

          // Simulate processing delay
          if (freshConfig.processingDelay > 0) {
            await new Promise((resolve) =>
              setTimeout(resolve, freshConfig.processingDelay),
            );
          }

          const isSuccess = Math.random() < freshConfig.successRate;

          if (isSuccess) {
            msg.ack();
            await logToDashboard(
              durableName,
              `[Success] Event seq ${msg.seq} processed successfully`,
              "success",
            );
            simulatorEvents.emit("metrics", {
              durableName,
              event: "success",
              stream: config.stream,
            });
          } else {
            if (attempts < freshConfig.maxDeliver) {
              msg.nak();
              await logToDashboard(
                durableName,
                `[Error] Processing failed. Sent NAK for redelivery.`,
                "warn",
              );
              simulatorEvents.emit("metrics", {
                durableName,
                event: "retry",
                stream: config.stream,
              });
            } else {
              // Dead-letter the event
              const headers: Record<string, string> = {};
              if (msg.headers) {
                for (const key of msg.headers.keys()) {
                  headers[key] = msg.headers.values(key).join(", ");
                }
              }

              await supabaseServer.from("DlqEvent").insert({
                stream: freshConfig.stream,
                subject: msg.subject,
                consumerGroup: durableName,
                payload,
                headers,
                errorReason: "Simulated consumer processing limit reached",
                numDelivered: attempts,
                status: "PENDING",
                updatedAt: new Date().toISOString(),
              });

              msg.ack();

              await logToDashboard(
                durableName,
                `[DLQ] Event seq ${msg.seq} exceeded retry limit. Logged to Dead-Letter Queue.`,
                "error",
              );
              simulatorEvents.emit("metrics", {
                durableName,
                event: "dlq",
                stream: config.stream,
              });
            }
          }
        }
      } catch (err) {
        const message = err instanceof Error ? err.message : String(err);
        await logToDashboard(
          durableName,
          `Consumer loop error: ${message}`,
          "error",
        );
      }
    })();
  } catch (err) {
    const message = err instanceof Error ? err.message : String(err);
    await logToDashboard(
      durableName,
      `Failed to subscribe: ${message}`,
      "error",
    );
    delete activeSubscriptions[durableName];

    await supabaseServer
      .from("ConsumerSimulatorConfig")
      .update({ active: false, updatedAt: new Date().toISOString() })
      .eq("durableName", durableName);
  }
}

export async function stopSimulator(durableName: string): Promise<void> {
  const sub = activeSubscriptions[durableName];
  if (sub) {
    try {
      sub.unsubscribe();
    } catch (err) {
      console.warn(`[simulator] Unsubscribe error for ${durableName}:`, err);
    }
    delete activeSubscriptions[durableName];
  }

  await supabaseServer
    .from("ConsumerSimulatorConfig")
    .update({ active: false, updatedAt: new Date().toISOString() })
    .eq("durableName", durableName);

  await logToDashboard(durableName, "Simulator stopped", "info");
}

export async function initializeSimulators(): Promise<void> {
  try {
    const { data: activeConfigs, error } = await supabaseServer
      .from("ConsumerSimulatorConfig")
      .select("*")
      .eq("active", true);

    if (error || !activeConfigs) return;

    /* eslint-disable-next-line no-console */
    console.log(
      `[simulator] Starting ${activeConfigs.length} simulator worker(s) on bootstrap...`,
    );

    for (const config of activeConfigs) {
      // Temporarily mark as inactive so startSimulator's guard doesn't skip it
      await supabaseServer
        .from("ConsumerSimulatorConfig")
        .update({ active: false })
        .eq("id", config.id);

      await startSimulator(config.durableName);
    }
  } catch (err) {
    console.error("[simulator] Failed to initialize active simulators:", err);
  }
}
