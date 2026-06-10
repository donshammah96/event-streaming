import {
  connect,
  JSONCodec,
  JetStreamClient,
  JetStreamManager,
  NatsConnection,
  RetentionPolicy,
  DiscardPolicy,
  AckPolicy,
} from "nats";

let nc: NatsConnection | null = null;
let js: JetStreamClient | null = null;
let jsm: JetStreamManager | null = null;

export const codec = JSONCodec();

// Dev singleton hot-reload prevention
if (process.env.NODE_ENV === "development") {
  const g = global as any;
  if (g._natsNc) {
    nc = g._natsNc;
    js = g._natsJs;
    jsm = g._natsJsm;
  }
}

export async function initNats(): Promise<NatsConnection> {
  if (nc) return nc;

  const url = process.env.NATS_URL || "nats://localhost:4222";
  try {
    nc = await connect({ servers: url });
    js = nc.jetstream();
    jsm = await nc.jetstreamManager();
    console.log(`Connected to NATS at ${url}`);

    if (process.env.NODE_ENV === "development") {
      const g = global as any;
      g._natsNc = nc;
      g._natsJs = js;
      g._natsJsm = jsm;
    }

    // Auto-create a default demonstration stream if none exists
    await ensureDefaultStream();

    return nc;
  } catch (error) {
    console.error(`Failed to connect to NATS at ${url}:`, error);
    throw error;
  }
}

export function getNatsConnection(): NatsConnection {
  const connection =
    nc ||
    (process.env.NODE_ENV === "development" ? (global as any)._natsNc : null);
  if (!connection) throw new Error("NATS is not initialized");
  return connection;
}

export function getJetStream(): JetStreamClient {
  const jetstream =
    js ||
    (process.env.NODE_ENV === "development" ? (global as any)._natsJs : null);
  if (!jetstream) throw new Error("JetStream is not initialized");
  return jetstream;
}

export function getJetStreamManager(): JetStreamManager {
  const manager =
    jsm ||
    (process.env.NODE_ENV === "development" ? (global as any)._natsJsm : null);
  if (!manager) throw new Error("JetStreamManager is not initialized");
  return manager;
}

async function ensureDefaultStream() {
  const manager = getJetStreamManager();
  try {
    const streams = await manager.streams.list().next();
    const hasDefault = streams.some(
      (s: any) => s.config.name === "DEMO_STREAM",
    );
    if (!hasDefault) {
      await manager.streams.add({
        name: "DEMO_STREAM",
        subjects: ["events.>"],
        retention: RetentionPolicy.Limits,
        max_msgs: 10000,
        discard: DiscardPolicy.Old,
      });
      console.log(
        "Created default NATS stream: DEMO_STREAM targeting 'events.>'",
      );
    }
  } catch (err) {
    console.warn("Failed to ensure default NATS stream:", err);
  }
}

// Stream admin helper wrappers
export async function listStreams() {
  const manager = getJetStreamManager();
  const list = await manager.streams.list().next();
  return list.map((s) => s.config);
}

export async function getStreamInfo(name: string) {
  const manager = getJetStreamManager();
  try {
    const info = await manager.streams.info(name);
    return info;
  } catch (err) {
    return null;
  }
}

export async function createStream(name: string, subjects: string[]) {
  const manager = getJetStreamManager();
  return await manager.streams.add({
    name,
    subjects,
    max_msgs: 10000,
  });
}

export async function deleteStream(name: string) {
  const manager = getJetStreamManager();
  return await manager.streams.delete(name);
}

// Consumer admin helper wrappers
export async function listConsumers(stream: string) {
  const manager = getJetStreamManager();
  const list = await manager.consumers.list(stream).next();
  return list;
}

export async function getConsumerInfo(stream: string, consumer: string) {
  const manager = getJetStreamManager();
  try {
    return await manager.consumers.info(stream, consumer);
  } catch (err) {
    return null;
  }
}

export async function createConsumer(
  stream: string,
  config: { durableName: string; filterSubject?: string; maxDeliver?: number },
) {
  const manager = getJetStreamManager();
  return await manager.consumers.add(stream, {
    durable_name: config.durableName,
    filter_subject: config.filterSubject || ">",
    ack_policy: AckPolicy.Explicit,
    max_deliver: config.maxDeliver || 3,
  });
}

export async function deleteConsumer(stream: string, consumer: string) {
  const manager = getJetStreamManager();
  return await manager.consumers.delete(stream, consumer);
}

// Get stream message details
export async function getStreamMessage(stream: string, seq: number) {
  const manager = getJetStreamManager();
  try {
    const msg = await manager.streams.getMessage(stream, { seq });
    if (!msg) return null;

    // Decode data payload
    let decodedData: any = null;
    try {
      decodedData = codec.decode(msg.data);
    } catch {
      decodedData = msg.data.toString();
    }

    // Decode headers if any
    const headers: Record<string, string> = {};
    if (msg.header) {
      for (const key of msg.header.keys()) {
        headers[key] = msg.header.values(key).join(", ");
      }
    }

    return {
      sequence: msg.seq,
      subject: msg.subject,
      data: decodedData,
      time: msg.time,
      headers,
    };
  } catch (err) {
    return null;
  }
}
