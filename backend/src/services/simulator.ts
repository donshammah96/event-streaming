import { EventEmitter } from "events";
import { JetStreamSubscription, consumerOpts } from "nats";
import { prisma } from "../database";
import { getJetStream, getJetStreamManager, codec, createConsumer } from "../nats";

export const simulatorEvents = new EventEmitter();

const activeSubscriptions: Record<string, JetStreamSubscription> = {};

function logToDashboard(durableName: string, text: string, type: "info" | "success" | "warn" | "error" = "info") {
  simulatorEvents.emit("log", {
    durableName,
    timestamp: new Date().toISOString(),
    text,
    type
  });
}

export async function startSimulator(durableName: string) {
  if (activeSubscriptions[durableName]) {
    logToDashboard(durableName, "Simulator is already running", "warn");
    return;
  }

  const config = await prisma.consumerSimulatorConfig.findUnique({
    where: { durableName }
  });

  if (!config) {
    throw new Error(`Consumer simulator config not found for durable name: ${durableName}`);
  }

  // Ensure consumer exists in NATS
  try {
    await createConsumer(config.stream, {
      durableName: config.durableName,
      filterSubject: config.subject,
      maxDeliver: config.maxDeliver
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
    
    // Update DB status
    await prisma.consumerSimulatorConfig.update({
      where: { durableName },
      data: { active: true }
    });

    logToDashboard(durableName, `Simulator started. Listening on subject '${config.subject}'...`, "info");
    
    // Background message processing loop
    (async () => {
      try {
        for await (const msg of sub) {
          // Double-check if still active and configuration has changed
          const freshConfig = await prisma.consumerSimulatorConfig.findUnique({
            where: { durableName }
          });
          
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
            await new Promise(resolve => setTimeout(resolve, freshConfig.processingDelay));
          }

          // Random success/failure roll
          const roll = Math.random();
          const isSuccess = roll < freshConfig.successRate;

          if (isSuccess) {
            msg.ack();
            logToDashboard(
              durableName, 
              `[Success] Event seq ${msg.seq} processed successfully`, 
              "success"
            );
            
            // Broadcast consumer progress metrics
            simulatorEvents.emit("metrics", {
              durableName,
              event: "success",
              stream: config.stream
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
                stream: config.stream
              });
            } else {
              // Dead-letter the event
              // Parse headers if available
              const headers: Record<string, string> = {};
              if (msg.headers) {
                for (const key of msg.headers.keys()) {
                  headers[key] = msg.headers.values(key).join(", ");
                }
              }

              await prisma.dlqEvent.create({
                data: {
                  stream: freshConfig.stream,
                  subject: msg.subject,
                  consumerGroup: durableName,
                  payload: payload,
                  headers: headers,
                  errorReason: "Simulated consumer processing limit reached",
                  numDelivered: attempts,
                  status: "PENDING"
                }
              });

              // Acknowledge in NATS so it doesn't continue retrying
              msg.ack();
              
              logToDashboard(
                durableName, 
                `[DLQ] Event seq ${msg.seq} exceeded retry limit. Logged to Dead-Letter Queue database.`, 
                "error"
              );
              
              simulatorEvents.emit("metrics", {
                durableName,
                event: "dlq",
                stream: config.stream
              });
            }
          }
        }
      } catch (err: any) {
        logToDashboard(durableName, `Consumer loop error: ${err.message}`, "error");
      }
    })();

  } catch (err: any) {
    logToDashboard(durableName, `Failed to subscribe: ${err.message}`, "error");
    delete activeSubscriptions[durableName];
    
    await prisma.consumerSimulatorConfig.update({
      where: { durableName },
      data: { active: false }
    });
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

  await prisma.consumerSimulatorConfig.update({
    where: { durableName },
    data: { active: false }
  });

  logToDashboard(durableName, "Simulator stopped", "info");
}

export async function initializeSimulators() {
  try {
    // Stop any dangling active configs in DB first to be clean, or auto-start them
    const activeConfigs = await prisma.consumerSimulatorConfig.findMany({
      where: { active: true }
    });
    
    console.log(`Starting ${activeConfigs.length} simulator worker(s) on bootstrap...`);
    for (const config of activeConfigs) {
      // Set DB active to false temporarily so startSimulator doesn't skip
      await prisma.consumerSimulatorConfig.update({
        where: { id: config.id },
        data: { active: false }
      });
      await startSimulator(config.durableName);
    }
  } catch (err) {
    console.error("Failed to initialize active simulators:", err);
  }
}
