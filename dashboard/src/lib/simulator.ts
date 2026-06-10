import { EventEmitter } from "events";
import { JetStreamSubscription, consumerOpts } from "nats";
import { supabase } from "./supabaseClient";
import { getJetStream, codec, createConsumer } from "./nats";

export let simulatorEvents = new EventEmitter();
let activeSubscriptions: Record<string, JetStreamSubscription> = {};

// Hot-reload protection for simulator singleton states
if (process.env.NODE_ENV === "development") {
  const g = global as any;
  if (!g._simulatorEvents) {
    g._simulatorEvents = new EventEmitter();
    g._activeSubscriptions = {};
  }
  simulatorEvents = g._simulatorEvents;
  activeSubscriptions = g._activeSubscriptions;
}

function logToDashboard(
  durableName: string,
  text: string,
  type: "info" | "success" | "warn" | "error" = "info"
) {
  simulatorEvents.emit("log", {
    durableName,
    timestamp: new Date().toISOString(),
    text,
    type,
  });
}

export async function startSimulator(durableName: string) {
  if (activeSubscriptions[durableName]) {
    logToDashboard(durableName, "Simulator is already running", "warn");
    return;
  }

  // Fetch config from Supabase
  const { data: config, error: configError } = await supabase
    .from("ConsumerSimulatorConfig")
    .select("*")
    .eq("durableName", durableName)
    .single();

  if (configError || !config) {
    throw new Error(
      `Consumer simulator config not found for durable name: ${durableName}`
    );
  }

  // Ensure consumer exists in NATS JetStream
  try {
    await createConsumer(config.stream, {
      durableName: config.durableName,
      filterSubject: config.subject,
      maxDeliver: config.maxDeliver,
    });
  } catch (err: any) {
    console.error(`Error ensuring NATS consumer for ${durableName}:`, err);
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

    // Update active status in Supabase
    await supabase
      .from("ConsumerSimulatorConfig")
      .update({ active: true, updatedAt: new Date().toISOString() })
      .eq("durableName", durableName);

    logToDashboard(
      durableName,
      `Simulator started. Listening on subject '${config.subject}'...`,
      "info"
    );

    // Background processing loop
    (async () => {
      try {
        for await (const msg of sub) {
          // Check if configuration has changed or stopped
          const { data: freshConfig } = await supabase
            .from("ConsumerSimulatorConfig")
            .select("*")
            .eq("durableName", durableName)
            .single();

          if (!freshConfig || !freshConfig.active) {
            msg.nak(); // Return message to queue
            break;
          }

          let payload: any = null;
          try {
            payload = codec.decode(msg.data);
          } catch {
            payload = msg.data.toString();
          }

          const attempts = msg.info?.redeliveryCount || 1;
          logToDashboard(
            durableName,
            `[Received] Event seq ${msg.seq} on subject '${msg.subject}' (Attempt ${attempts}/${freshConfig.maxDeliver})`,
            "info"
          );

          // Simulate processing delay
          if (freshConfig.processingDelay > 0) {
            await new Promise((resolve) =>
              setTimeout(resolve, freshConfig.processingDelay)
            );
          }

          // Success roll
          const roll = Math.random();
          const isSuccess = roll < freshConfig.successRate;

          if (isSuccess) {
            msg.ack();
            logToDashboard(
              durableName,
              `[Success] Event seq ${msg.seq} processed successfully`,
              "success"
            );

            simulatorEvents.emit("metrics", {
              durableName,
              event: "success",
              stream: config.stream,
            });
          } else {
            if (attempts < freshConfig.maxDeliver) {
              msg.nak();
              logToDashboard(
                durableName,
                `[Error] Processing failed. Sent NAK for redelivery.`,
                "warn"
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

              // Create DLQ event in Supabase
              await supabase.from("DlqEvent").insert({
                stream: freshConfig.stream,
                subject: msg.subject,
                consumerGroup: durableName,
                payload: payload,
                headers: headers,
                errorReason: "Simulated consumer processing limit reached",
                numDelivered: attempts,
                status: "PENDING",
                updatedAt: new Date().toISOString(),
              });

              // Acknowledge in NATS
              msg.ack();

              logToDashboard(
                durableName,
                `[DLQ] Event seq ${msg.seq} exceeded retry limit. Logged to Dead-Letter Queue database.`,
                "error"
              );

              simulatorEvents.emit("metrics", {
                durableName,
                event: "dlq",
                stream: config.stream,
              });
            }
          }
        }
      } catch (err: any) {
        logToDashboard(
          durableName,
          `Consumer loop error: ${err.message}`,
          "error"
        );
      }
    })();
  } catch (err: any) {
    logToDashboard(durableName, `Failed to subscribe: ${err.message}`, "error");
    delete activeSubscriptions[durableName];

    await supabase
      .from("ConsumerSimulatorConfig")
      .update({ active: false, updatedAt: new Date().toISOString() })
      .eq("durableName", durableName);
  }
}

export async function stopSimulator(durableName: string) {
  const sub = activeSubscriptions[durableName];
  if (sub) {
    try {
      sub.unsubscribe();
    } catch (err) {
      console.warn(`Unsubscribe error for ${durableName}:`, err);
    }
    delete activeSubscriptions[durableName];
  }

  await supabase
    .from("ConsumerSimulatorConfig")
    .update({ active: false, updatedAt: new Date().toISOString() })
    .eq("durableName", durableName);

  logToDashboard(durableName, "Simulator stopped", "info");
}

export async function initializeSimulators() {
  try {
    // Restart any active simulators from DB state
    const { data: activeConfigs, error } = await supabase
      .from("ConsumerSimulatorConfig")
      .select("*")
      .eq("active", true);

    if (error || !activeConfigs) return;

    console.log(
      `Starting ${activeConfigs.length} simulator worker(s) on bootstrap...`
    );
    for (const config of activeConfigs) {
      // Temporarily mark config as inactive so startSimulator doesn't skip
      await supabase
        .from("ConsumerSimulatorConfig")
        .update({ active: false })
        .eq("id", config.id);

      await startSimulator(config.durableName);
    }
  } catch (err) {
    console.error("Failed to initialize active simulators:", err);
  }
}
