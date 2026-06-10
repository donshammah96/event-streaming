import React, { useState, useEffect, useRef } from "react";

// Types matching Backend Models
interface NatsStream {
  name: string;
  subjects: string[];
}

interface NatsMessage {
  sequence: number;
  subject: string;
  time: string;
  data: unknown;
  headers?: Record<string, unknown>;
}

interface MessagesResponse {
  messages?: NatsMessage[];
  nextOffsetSeq?: number | null;
}

interface PublishResponse {
  stream?: string;
  seq?: number;
  error?: string;
  errors?: string[];
}

interface DbSchema {
  id: string;
  subjectPattern: string;
  schema: Record<string, unknown>;
  version: number;
  description?: string;
  createdAt: string;
}

interface DlqEvent {
  id: string;
  stream: string;
  subject: string;
  consumerGroup: string;
  payload: unknown;
  headers?: Record<string, unknown>;
  errorReason: string;
  numDelivered: number;
  status: "PENDING" | "REPLAYED" | "DISMISSED";
  createdAt: string;
}

interface SimulatorConfig {
  id: string;
  stream: string;
  subject: string;
  durableName: string;
  successRate: number;
  processingDelay: number;
  maxDeliver: number;
  active: boolean;
  createdAt: string;
}

interface ConsoleLog {
  durableName: string;
  timestamp: string;
  text: string;
  type: "info" | "success" | "warn" | "error";
}

interface WsStatsMessage {
  type: "STATS";
  data: {
    streams: NatsStream[];
    activeSimulators: number;
    pendingDlq: number;
    registeredSchemas: number;
  };
}

interface WsLogMessage {
  type: "LOG";
  data: ConsoleLog;
}

type WsMessage = WsStatsMessage | WsLogMessage;

const BACKEND_URL = "/api";
const WS_URL =
  typeof window !== "undefined"
    ? `${window.location.protocol === "https:" ? "wss:" : "ws:"}//${window.location.host}/api/ws`
    : "";

export default function App() {
  const [activeTab, setActiveTab] = useState<
    "overview" | "streams" | "consumers" | "dlq" | "schemas"
  >("overview");

  // Real-time Dashboard Statistics
  const [stats, setStats] = useState({
    streams: [] as NatsStream[],
    activeSimulators: 0,
    pendingDlq: 0,
    registeredSchemas: 0,
  });

  const [wsConnected, setWsConnected] = useState(false);
  const [logs, setLogs] = useState<ConsoleLog[]>([]);
  const consoleEndRef = useRef<HTMLDivElement>(null);

  // Entities Data State
  const [streams, setStreams] = useState<NatsStream[]>([]);
  const [schemas, setSchemas] = useState<DbSchema[]>([]);
  const [simulators, setSimulators] = useState<SimulatorConfig[]>([]);
  const [dlqEvents, setDlqEvents] = useState<DlqEvent[]>([]);

  // Selected details
  const [activeStream, setActiveStream] = useState<string | null>(null);
  const [streamMessages, setStreamMessages] = useState<NatsMessage[]>([]);
  const [nextOffsetSeq, setNextOffsetSeq] = useState<number | null>(null);
  const [expandedDlqId, setExpandedDlqId] = useState<string | null>(null);

  // Loading indicator
  const [isLoading, setIsLoading] = useState(false);

  // Modal forms
  const [showCreateStream, setShowCreateStream] = useState(false);
  const [newStreamName, setNewStreamName] = useState("");
  const [newStreamSubjects, setNewStreamSubjects] = useState("events.user.*");

  const [showCreateSchema, setShowCreateSchema] = useState(false);
  const [newSchemaSubject, setNewSchemaSubject] = useState("events.user.*");
  const [newSchemaDesc, setNewSchemaDesc] = useState(
    "Validates user payload items",
  );
  const [newSchemaDef, setNewSchemaDef] = useState(
    JSON.stringify(
      {
        type: "object",
        properties: {
          userId: { type: "string" },
          name: { type: "string" },
          email: { type: "string", format: "email" },
        },
        required: ["userId", "name"],
      },
      null,
      2,
    ),
  );

  const [showCreateSimulator, setShowCreateSimulator] = useState(false);
  const [newSimStream, setNewSimStream] = useState("DEMO_STREAM");
  const [newSimSubject, setNewSimSubject] = useState("events.user.created");
  const [newSimDurableName, setNewSimDurableName] = useState(
    "user-registration-worker",
  );
  const [newSimSuccessRate, setNewSimSuccessRate] = useState("0.8");
  const [newSimDelay, setNewSimDelay] = useState("500");
  const [newSimMaxDeliver, setNewSimMaxDeliver] = useState("3");

  // Publishing form
  const [pubSubject, setPubSubject] = useState("events.user.created");
  const [pubPayload, setPubPayload] = useState(
    JSON.stringify(
      {
        userId: "usr_1001",
        name: "John Doe",
        email: "john.doe@example.com",
      },
      null,
      2,
    ),
  );
  const [pubHeaders, setPubHeaders] = useState(
    '{\n  "X-Source": "Dashboard"\n}',
  );
  const [pubStatus, setPubStatus] = useState<{
    type: "success" | "error";
    message: string;
    details?: string[];
  } | null>(null);

  // Fetch lists for views
  const fetchInitialData = async () => {
    setIsLoading(true);
    try {
      const [streamsRes, schemasRes, simsRes, dlqRes] = await Promise.all([
        fetch(`${BACKEND_URL}/streams`).then(
          (r) => r.json() as Promise<NatsStream[]>,
        ),
        fetch(`${BACKEND_URL}/schemas`).then(
          (r) => r.json() as Promise<DbSchema[]>,
        ),
        fetch(`${BACKEND_URL}/simulator`).then(
          (r) => r.json() as Promise<SimulatorConfig[]>,
        ),
        fetch(`${BACKEND_URL}/dlq`).then(
          (r) => r.json() as Promise<DlqEvent[]>,
        ),
      ]);

      if (Array.isArray(streamsRes)) setStreams(streamsRes);
      if (Array.isArray(schemasRes)) setSchemas(schemasRes);
      if (Array.isArray(simsRes)) setSimulators(simsRes);
      if (Array.isArray(dlqRes)) setDlqEvents(dlqRes);
    } catch (err) {
      console.error("Failed to fetch initial application data:", err);
    } finally {
      setIsLoading(false);
    }
  };

  // WebSocket connection & statistics setup
  useEffect(() => {
    let ws: WebSocket;
    let active = true;

    async function initWs() {
      try {
        // Trigger Next.js API route to boot WebSocket gateway
        await fetch("/api/ws");
      } catch (err) {
        console.warn("WebSocket bootstrapping check failed:", err);
      }

      if (!active) return;
      connectWs();
    }

    function connectWs() {
      if (!WS_URL) return;
      ws = new WebSocket(WS_URL);

      ws.onopen = () => {
        setWsConnected(true);
        console.log("WebSocket connected to gateway");
      };

      ws.onmessage = (event) => {
        try {
          if (typeof event.data !== "string") return;
          const payload = JSON.parse(event.data) as WsMessage;

          if (payload.type === "STATS") {
            setStats(payload.data);
            if (payload.data.streams) {
              setStreams(payload.data.streams);
            }
          } else if (payload.type === "LOG") {
            setLogs((prev) => {
              const updated = [...prev, payload.data];
              return updated.slice(-150); // Cap log size at 150 items
            });
          }
        } catch (err) {
          console.warn("WebSocket parse error:", err);
        }
      };

      ws.onclose = () => {
        setWsConnected(false);
        console.log("WebSocket disconnected from gateway. Reconnecting...");
        setTimeout(() => {
          if (active) connectWs();
        }, 3000); // Auto reconnect in 3s
      };

      ws.onerror = (err) => {
        console.error("WebSocket error:", err);
        ws.close();
      };
    }

    void initWs();
    void Promise.resolve().then(() => fetchInitialData());

    return () => {
      active = false;
      if (ws) ws.close();
    };
  }, []);

  // Auto-scroll simulator logs to bottom
  useEffect(() => {
    if (consoleEndRef.current) {
      consoleEndRef.current.scrollIntoView({ behavior: "smooth" });
    }
  }, [logs]);

  const refreshTab = async (tab: typeof activeTab) => {
    try {
      if (tab === "streams") {
        const res = await fetch(`${BACKEND_URL}/streams`).then(
          (r) => r.json() as Promise<NatsStream[]>,
        );
        if (Array.isArray(res)) setStreams(res);
      } else if (tab === "schemas") {
        const res = await fetch(`${BACKEND_URL}/schemas`).then(
          (r) => r.json() as Promise<DbSchema[]>,
        );
        if (Array.isArray(res)) setSchemas(res);
      } else if (tab === "consumers") {
        const res = await fetch(`${BACKEND_URL}/simulator`).then(
          (r) => r.json() as Promise<SimulatorConfig[]>,
        );
        if (Array.isArray(res)) setSimulators(res);
      } else if (tab === "dlq") {
        const res = await fetch(`${BACKEND_URL}/dlq`).then(
          (r) => r.json() as Promise<DlqEvent[]>,
        );
        if (Array.isArray(res)) setDlqEvents(res);
      }
    } catch (err) {
      console.warn("Failed to refresh tab details:", err);
    }
  };

  const selectTab = (tab: typeof activeTab) => {
    setActiveTab(tab);
    void refreshTab(tab);
  };

  // NATS message browser
  const selectStream = async (streamName: string) => {
    setActiveStream(streamName);
    setIsLoading(true);
    try {
      const res = await fetch(
        `${BACKEND_URL}/streams/${streamName}/messages?limit=20`,
      ).then((r) => r.json() as Promise<MessagesResponse>);
      setStreamMessages(res.messages || []);
      setNextOffsetSeq(res.nextOffsetSeq ?? null);
    } catch (err) {
      console.error("Failed to load stream messages:", err);
      setStreamMessages([]);
      setNextOffsetSeq(null);
    } finally {
      setIsLoading(false);
    }
  };

  const loadMoreMessages = async () => {
    if (!activeStream || nextOffsetSeq === null) return;
    try {
      const res = await fetch(
        `${BACKEND_URL}/streams/${activeStream}/messages?limit=20&offsetSeq=${nextOffsetSeq}`,
      ).then((r) => r.json() as Promise<MessagesResponse>);
      setStreamMessages((prev) => [...prev, ...(res.messages || [])]);
      setNextOffsetSeq(res.nextOffsetSeq ?? null);
    } catch (err) {
      console.error("Failed to load more stream messages:", err);
    }
  };

  // Actions: Stream CRUD
  const handleCreateStream = async (e: React.FormEvent) => {
    e.preventDefault();
    if (!newStreamName) return;
    const subjects = newStreamSubjects.split(",").map((s) => s.trim());
    try {
      const res = await fetch(`${BACKEND_URL}/streams`, {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ name: newStreamName.toUpperCase(), subjects }),
      });
      if (res.ok) {
        setShowCreateStream(false);
        setNewStreamName("");
        selectTab("streams");
      } else {
        const err = (await res.json()) as { error: string };
        alert(`Failed to create stream: ${err.error}`);
      }
    } catch (err) {
      console.error(err);
    }
  };

  const handleDeleteStream = async (name: string) => {
    if (
      !confirm(
        `Are you sure you want to delete stream ${name}? This will clear all its messages.`,
      )
    )
      return;
    try {
      const res = await fetch(`${BACKEND_URL}/streams/${name}`, {
        method: "DELETE",
      });
      if (res.ok) {
        if (activeStream === name) {
          setActiveStream(null);
          setStreamMessages([]);
        }
        selectTab("streams");
      }
    } catch (err) {
      console.error(err);
    }
  };

  // Actions: Schema CRUD
  const handleCreateSchema = async (e: React.FormEvent) => {
    e.preventDefault();
    let parsedSchema: Record<string, unknown>;
    try {
      parsedSchema = JSON.parse(newSchemaDef) as Record<string, unknown>;
    } catch {
      alert("Invalid JSON format in schema definition");
      return;
    }

    try {
      const res = await fetch(`${BACKEND_URL}/schemas`, {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({
          subjectPattern: newSchemaSubject,
          schema: parsedSchema,
          description: newSchemaDesc,
        }),
      });
      if (res.ok) {
        setShowCreateSchema(false);
        setNewSchemaSubject("events.user.*");
        setNewSchemaDesc("");
        selectTab("schemas");
      } else {
        const err = (await res.json()) as { error: string };
        alert(`Failed to register schema: ${err.error}`);
      }
    } catch (err) {
      console.error(err);
    }
  };

  const handleDeleteSchema = async (id: string) => {
    if (
      !confirm(
        "Are you sure you want to delete this schema? Message validation on this subject will stop.",
      )
    )
      return;
    try {
      const res = await fetch(`${BACKEND_URL}/schemas/${id}`, {
        method: "DELETE",
      });
      if (res.ok) {
        selectTab("schemas");
      }
    } catch (err) {
      console.error(err);
    }
  };

  // Actions: Simulator Config & Workers
  const handleCreateSimulator = async (e: React.FormEvent) => {
    e.preventDefault();
    try {
      const res = await fetch(`${BACKEND_URL}/simulator`, {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({
          stream: newSimStream,
          subject: newSimSubject,
          durableName: newSimDurableName,
          successRate: parseFloat(newSimSuccessRate),
          processingDelay: parseInt(newSimDelay),
          maxDeliver: parseInt(newSimMaxDeliver),
        }),
      });
      if (res.ok) {
        setShowCreateSimulator(false);
        selectTab("consumers");
      } else {
        const err = (await res.json()) as { error: string };
        alert(`Failed to configure simulator: ${err.error}`);
      }
    } catch (err) {
      console.error(err);
    }
  };

  const toggleSimulator = async (sim: SimulatorConfig) => {
    const action = sim.active ? "stop" : "start";
    try {
      const res = await fetch(
        `${BACKEND_URL}/simulator/${sim.durableName}/${action}`,
        { method: "POST" },
      );
      if (res.ok) {
        await refreshTab("consumers");
      }
    } catch (err) {
      console.error(err);
    }
  };

  const handleDeleteSimulator = async (durableName: string) => {
    if (!confirm(`Are you sure you want to delete simulator ${durableName}?`))
      return;
    try {
      const res = await fetch(`${BACKEND_URL}/simulator/${durableName}`, {
        method: "DELETE",
      });
      if (res.ok) {
        await refreshTab("consumers");
      }
    } catch (err) {
      console.error(err);
    }
  };

  // Actions: Event Publishing with validation
  const handlePublish = async (e: React.FormEvent) => {
    e.preventDefault();
    setPubStatus(null);
    let payloadObj: unknown;
    let headersObj: Record<string, unknown> = {};

    try {
      payloadObj = JSON.parse(pubPayload) as unknown;
    } catch {
      setPubStatus({ type: "error", message: "Invalid JSON payload format" });
      return;
    }

    try {
      if (pubHeaders.trim()) {
        headersObj = JSON.parse(pubHeaders) as Record<string, unknown>;
      }
    } catch {
      setPubStatus({ type: "error", message: "Invalid JSON headers format" });
      return;
    }

    try {
      const res = await fetch(`${BACKEND_URL}/publish`, {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({
          subject: pubSubject,
          payload: payloadObj,
          headers: headersObj,
        }),
      });

      const data = (await res.json()) as PublishResponse;
      if (res.ok) {
        setPubStatus({
          type: "success",
          message: `Successfully published message to stream '${data.stream ?? ""}' at sequence ${data.seq ?? 0}.`,
        });
        if (activeStream === data.stream) {
          void selectStream(data.stream ?? "");
        }
      } else {
        setPubStatus({
          type: "error",
          message: data.error || "Failed to publish message",
          details: data.errors,
        });
      }
    } catch (err) {
      const message = err instanceof Error ? err.message : String(err);
      setPubStatus({ type: "error", message });
    }
  };

  // Actions: DLQ management
  const handleReplayDlq = async (id: string, customSubject?: string) => {
    try {
      const res = await fetch(`${BACKEND_URL}/dlq/${id}/replay`, {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ targetSubject: customSubject }),
      });
      if (res.ok) {
        await refreshTab("dlq");
        alert("Event replayed successfully");
      } else {
        const err = (await res.json()) as { error: string };
        alert(`Failed to replay event: ${err.error}`);
      }
    } catch (err) {
      console.error(err);
    }
  };

  const handleDismissDlq = async (id: string) => {
    try {
      const res = await fetch(`${BACKEND_URL}/dlq/${id}/dismiss`, {
        method: "POST",
      });
      if (res.ok) {
        await refreshTab("dlq");
      }
    } catch (err) {
      console.error(err);
    }
  };

  const handlePurgeDlq = async () => {
    if (
      !confirm(
        "Are you sure you want to purge all DLQ messages? This will wipe the DLQ database table.",
      )
    )
      return;
    try {
      const res = await fetch(`${BACKEND_URL}/dlq/purge`, { method: "POST" });
      if (res.ok) {
        await refreshTab("dlq");
      }
    } catch (err) {
      console.error(err);
    }
  };

  return (
    <div className="app-container">
      {/* Sidebar Navigation */}
      <aside className="sidebar">
        <div className="logo-container">
          <div className="logo-icon">N</div>
          <span className="logo-text">NATS EventStream</span>
        </div>
        <ul className="nav-links">
          <li
            className={`nav-item ${activeTab === "overview" ? "active" : ""}`}
            onClick={() => selectTab("overview")}
          >
            <svg
              xmlns="http://www.w3.org/2000/svg"
              fill="none"
              viewBox="0 0 24 24"
              strokeWidth="2"
              stroke="currentColor"
            >
              <path
                strokeLinecap="round"
                strokeLinejoin="round"
                d="M3.75 6A2.25 2.25 0 0 1 6 3.75h2.25A2.25 2.25 0 0 1 10.5 6v2.25a2.25 2.25 0 0 1-2.25 2.25H6a2.25 2.25 0 0 1-2.25-2.25V6ZM3.75 15.75A2.25 2.25 0 0 1 6 13.5h2.25a2.25 2.25 0 0 1 2.25 2.25V18a2.25 2.25 0 0 1-2.25 2.25H6A2.25 2.25 0 0 1 3.75 18v-2.25ZM13.5 6a2.25 2.25 0 0 1 2.25-2.25H18A2.25 2.25 0 0 1 20.25 6v2.25A2.25 2.25 0 0 1 18 10.5h-2.25A2.25 2.25 0 0 1 13.5 8.25V6ZM13.5 15.75a2.25 2.25 0 0 1 2.25-2.25H18a2.25 2.25 0 0 1 2.25 2.25V18A2.25 2.25 0 0 1 18 20.25h-2.25A2.25 2.25 0 0 1 13.5 18v-2.25Z"
              />
            </svg>
            Overview
          </li>
          <li
            className={`nav-item ${activeTab === "streams" ? "active" : ""}`}
            onClick={() => selectTab("streams")}
          >
            <svg
              xmlns="http://www.w3.org/2000/svg"
              fill="none"
              viewBox="0 0 24 24"
              strokeWidth="2"
              stroke="currentColor"
            >
              <path
                strokeLinecap="round"
                strokeLinejoin="round"
                d="M8.25 6.75h12M8.25 12h12m-12 5.25h12M3.75 6.75h.007v.008H3.75V6.75Zm.007 8.25h-.007v-.008h.007V15Zm-.007 3h.007v.008H3.75V18Zm-.007-6h.007v.008H3.75V12Z"
              />
            </svg>
            Streams & Inspector
          </li>
          <li
            className={`nav-item ${activeTab === "consumers" ? "active" : ""}`}
            onClick={() => selectTab("consumers")}
          >
            <svg
              xmlns="http://www.w3.org/2000/svg"
              fill="none"
              viewBox="0 0 24 24"
              strokeWidth="2"
              stroke="currentColor"
            >
              <path
                strokeLinecap="round"
                strokeLinejoin="round"
                d="M15.59 14.37a6 6 0 0 1-5.84 7.38v-4.8m5.84-2.58a14.98 14.98 0 0 0 6.16-12.12A14.98 14.98 0 0 0 9.61 9.51m5.98 4.86a14.98 14.98 0 0 1-5.98-4.86m0 0a14.98 14.98 0 0 0-6.16 12.12A14.98 14.98 0 0 0 9.61 9.51m0 0a6 6 0 0 1 5.84-7.38v4.8"
              />
            </svg>
            Workers & Simulator
          </li>
          <li
            className={`nav-item ${activeTab === "dlq" ? "active" : ""}`}
            onClick={() => selectTab("dlq")}
          >
            <svg
              xmlns="http://www.w3.org/2000/svg"
              fill="none"
              viewBox="0 0 24 24"
              strokeWidth="2"
              stroke="currentColor"
            >
              <path
                strokeLinecap="round"
                strokeLinejoin="round"
                d="M12 9v3.75m-9.303 3.376c-.866 1.5.217 3.374 1.948 3.374h14.71c1.73 0 2.813-1.874 1.948-3.374L13.949 3.378c-.866-1.5-3.032-1.5-3.898 0L2.697 16.126ZM12 15.75h.007v.008H12v-.008Z"
              />
            </svg>
            Dead-Letter Queue
            {stats.pendingDlq > 0 && (
              <span
                className="badge badge-danger"
                style={{ marginLeft: "auto" }}
              >
                {stats.pendingDlq}
              </span>
            )}
          </li>
          <li
            className={`nav-item ${activeTab === "schemas" ? "active" : ""}`}
            onClick={() => selectTab("schemas")}
          >
            <svg
              xmlns="http://www.w3.org/2000/svg"
              fill="none"
              viewBox="0 0 24 24"
              strokeWidth="2"
              stroke="currentColor"
            >
              <path
                strokeLinecap="round"
                strokeLinejoin="round"
                d="M9 12h3.75M9 15h3.75M9 18h3.75m3 .75H18a2.25 2.25 0 0 0 2.25-2.25V6.108c0-1.135-.845-2.098-1.976-2.192a48.424 48.424 0 0 0-1.123-.08m-5.801 0c-.065.21-.1.433-.1.664 0 .414.336.75.75.75h4.5a.75.75 0 0 0 .75-.75 2.25 2.25 0 0 0-.1-.664m-5.8 0A2.251 2.251 0 0 1 13.5 2.25H15c1.03 0 1.9.693 2.166 1.638m-7.377 0A48.536 48.536 0 0 1 12 3m0 0c2.917 0 5.747.294 8.5.862m-21 10.398c0-.552.448-1 1-1h6.25a1 1 0 0 1 1 1v3.83a1 1 0 0 1-1 1H2.5a1 1 0 0 1-1-1v-3.83Z"
              />
            </svg>
            Schema Registry
          </li>
        </ul>

        {/* Connection status overlay */}
        <div className="connection-status">
          <div
            className={`status-dot ${wsConnected ? "connected" : "disconnected"}`}
          />
          <span>
            {wsConnected ? "System Status: Online" : "System Status: Offline"}
          </span>
        </div>
      </aside>

      {/* Main Dashboard Panel */}
      <main className="main-content">
        {/* Header section */}
        <header className="header">
          <div className="header-title">
            <h1>
              {activeTab === "overview" && "System Health Overview"}
              {activeTab === "streams" && "Streams & Event Inspector"}
              {activeTab === "consumers" && "Consumer Group Simulators"}
              {activeTab === "dlq" && "Dead-Letter Queue Hub"}
              {activeTab === "schemas" && "JSON Schema Registry"}
            </h1>
            <p>
              {activeTab === "overview" &&
                "Real-time pub/sub bus telemetry logs and core platform stats"}
              {activeTab === "streams" &&
                "Browse NATS JetStream topics and publish schema-validated events"}
              {activeTab === "consumers" &&
                "Configure background worker success profiles and observe processing load"}
              {activeTab === "dlq" &&
                "Inspect failed consumer messages, review raw errors, and trigger replays"}
              {activeTab === "schemas" &&
                "Manage contract constraints and subject pattern validation bounds"}
            </p>
          </div>
          <button
            className="btn btn-secondary"
            onClick={() => {
              void fetchInitialData();
            }}
          >
            Sync telemetry
          </button>
        </header>

        {/* Global Statistics Cards */}
        <section className="metrics-grid">
          <div className="metric-card">
            <div className="metric-header">
              <span>Streams Registered</span>
              <svg
                style={{ width: "16px", color: "var(--color-accent)" }}
                fill="none"
                viewBox="0 0 24 24"
                stroke="currentColor"
              >
                <path
                  strokeWidth="2"
                  d="M19 11H5m14 0a2 2 0 012 2v6a2 2 0 01-2 2H5a2 2 0 01-2-2v-6a2 2 0 012-2m14 0V9a2 2 0 00-2-2M5 11V9a2 2 0 012-2m0 0V5a2 2 0 012-2h6a2 2 0 012 2v2M7 7h10"
                />
              </svg>
            </div>
            <div className="metric-value">
              {stats.streams ? stats.streams.length : 0}
            </div>
            <div className="metric-desc">Active NATS JetStream channels</div>
          </div>
          <div className="metric-card">
            <div className="metric-header">
              <span>Active Simulator Groups</span>
              <svg
                style={{ width: "16px", color: "var(--color-purple)" }}
                fill="none"
                viewBox="0 0 24 24"
                stroke="currentColor"
              >
                <path
                  strokeWidth="2"
                  d="M12 6V4m0 2a2 2 0 100 4m0-4a2 2 0 110 4m-6 8a2 2 0 100-4m0 4a2 2 0 110-4m0 4v2m0-6V4m6 6v10m6-2a2 2 0 100-4m0 4a2 2 0 110-4m0 4v2m0-6V4"
                />
              </svg>
            </div>
            <div className="metric-value">{stats.activeSimulators}</div>
            <div className="metric-desc">
              Mock worker processes pulling events
            </div>
          </div>
          <div
            className="metric-card"
            style={{
              boxShadow:
                stats.pendingDlq > 0 ? "var(--shadow-danger-glow)" : "none",
            }}
          >
            <div className="metric-header">
              <span>DLQ Backlog</span>
              <svg
                style={{
                  width: "16px",
                  color:
                    stats.pendingDlq > 0
                      ? "var(--color-danger)"
                      : "var(--text-muted)",
                }}
                fill="none"
                viewBox="0 0 24 24"
                stroke="currentColor"
              >
                <path
                  strokeWidth="2"
                  d="M12 9v2m0 4h.01m-6.938 4h13.856c1.54 0 2.502-1.667 1.732-3L13.732 4c-.77-1.333-2.694-1.333-3.464 0L3.34 16c-.77 1.333.192 3 1.732 3z"
                />
              </svg>
            </div>
            <div
              className="metric-value"
              style={{
                color: stats.pendingDlq > 0 ? "var(--color-danger)" : "inherit",
              }}
            >
              {stats.pendingDlq}
            </div>
            <div className="metric-desc">
              Messages awaiting processing repair
            </div>
          </div>
          <div className="metric-card">
            <div className="metric-header">
              <span>Active Schemas</span>
              <svg
                style={{ width: "16px", color: "var(--color-success)" }}
                fill="none"
                viewBox="0 0 24 24"
                stroke="currentColor"
              >
                <path
                  strokeWidth="2"
                  d="M9 12l2 2 4-4m5.618-4.016A11.955 11.955 0 0112 2.944a11.955 11.955 0 01-8.618 3.04A12.02 12.02 0 003 9c0 5.591 3.824 10.29 9 11.622 5.176-1.332 9-6.03 9-11.622 0-1.042-.133-2.052-.382-3.016z"
                />
              </svg>
            </div>
            <div className="metric-value">{stats.registeredSchemas}</div>
            <div className="metric-desc">
              Subject constraints validated at publish
            </div>
          </div>
        </section>

        {isLoading && (
          <div style={{ color: "var(--color-accent)", margin: "1rem 0" }}>
            Syncing data structures...
          </div>
        )}

        {/* 1. VIEW: OVERVIEW */}
        {activeTab === "overview" && (
          <div>
            <div className="panel">
              <div className="panel-header">
                <div className="panel-title">
                  Real-time Pub/Sub Telemetry console
                </div>
                <button
                  className="btn btn-secondary"
                  onClick={() => setLogs([])}
                >
                  Clear logs
                </button>
              </div>
              <div className="console-log">
                {logs.length === 0 ? (
                  <div
                    style={{ color: "var(--text-muted)", fontStyle: "italic" }}
                  >
                    No simulation event logs. Start a simulator worker and
                    publish events to populate logs...
                  </div>
                ) : (
                  logs.map((log, index) => (
                    <div
                      key={index}
                      className={`console-line console-line-${log.type}`}
                    >
                      <span
                        style={{
                          color: "var(--text-muted)",
                          marginRight: "0.5rem",
                        }}
                      >
                        [{log.timestamp.split("T")[1]?.slice(0, 8) || ""}]
                      </span>
                      <strong
                        style={{
                          color: "var(--text-primary)",
                          marginRight: "0.5rem",
                        }}
                      >
                        [{log.durableName}]
                      </strong>
                      {log.text}
                    </div>
                  ))
                )}
                <div ref={consoleEndRef} />
              </div>
            </div>

            <div className="panel">
              <div className="panel-title" style={{ marginBottom: "1rem" }}>
                End-to-End Walkthrough Demonstration Flow
              </div>
              <p
                style={{
                  color: "var(--text-secondary)",
                  fontSize: "0.95rem",
                  lineHeight: "1.6",
                  marginBottom: "1rem",
                }}
              >
                1. Navigate to <strong>Schema Registry</strong> and register a
                schema for your topic pattern (e.g. <code>events.user.*</code>).
                <br />
                2. Navigate to <strong>Workers & Simulator</strong>, and create
                a mock worker matching that subject (e.g. durable name{" "}
                <code>worker-group-A</code> listening to{" "}
                <code>events.user.created</code>). Configure a failure rate
                (e.g., <code>0.4</code> or 40% failure) to test dead-lettering.
                <br />
                3. Go to <strong>Streams & Inspector</strong>, enter subject{" "}
                <code>events.user.created</code> and publish some payloads.
                Watch the validator block invalid items, and pass valid ones.
                <br />
                4. Watch the <strong>Telemetry Console</strong> above process
                the message, run retries, and output failed events into the{" "}
                <strong>Dead-Letter Queue</strong>.
                <br />
                5. Open the <strong>Dead-Letter Queue</strong> panel, review the
                error traces, adjust the worker&apos;s success rate to 100%, and
                click **Replay** to clear the DLQ!
              </p>
            </div>
          </div>
        )}

        {/* 2. VIEW: STREAMS & INSPECTOR */}
        {activeTab === "streams" && (
          <div className="split-layout">
            {/* Left Column: Streams List */}
            <div>
              <div className="panel" style={{ padding: "1.25rem" }}>
                <div className="flex-between" style={{ marginBottom: "1rem" }}>
                  <h3
                    style={{
                      fontSize: "1rem",
                      fontFamily: "var(--font-display)",
                    }}
                  >
                    Streams
                  </h3>
                  <button
                    className="btn btn-primary"
                    style={{ padding: "0.4rem 0.8rem", fontSize: "0.8rem" }}
                    onClick={() => setShowCreateStream(true)}
                  >
                    + Create Stream
                  </button>
                </div>
                <div className="item-list">
                  {streams.map((s) => (
                    <div
                      key={s.name}
                      className={`item-list-card ${activeStream === s.name ? "active" : ""}`}
                      onClick={() => {
                        void selectStream(s.name);
                      }}
                    >
                      <div className="flex-between">
                        <strong style={{ fontSize: "0.95rem" }}>
                          {s.name}
                        </strong>
                        <button
                          className="btn btn-secondary"
                          style={{
                            padding: "2px 6px",
                            fontSize: "0.7rem",
                            color: "var(--color-danger)",
                          }}
                          onClick={(e) => {
                            e.stopPropagation();
                            void handleDeleteStream(s.name);
                          }}
                        >
                          delete
                        </button>
                      </div>
                      <div
                        style={{
                          color: "var(--text-muted)",
                          fontSize: "0.75rem",
                          marginTop: "0.5rem",
                        }}
                      >
                        Subjects: {s.subjects.join(", ")}
                      </div>
                    </div>
                  ))}
                </div>
              </div>

              {/* Publisher Panel */}
              <div
                className="panel"
                style={{ padding: "1.25rem", marginTop: "1.5rem" }}
              >
                <h3
                  style={{
                    fontSize: "1rem",
                    fontFamily: "var(--font-display)",
                    marginBottom: "1rem",
                  }}
                >
                  Event Publisher
                </h3>
                <form
                  onSubmit={(e) => {
                    void handlePublish(e);
                  }}
                >
                  <div className="form-group">
                    <label className="form-label">Publish Subject</label>
                    <input
                      className="input"
                      type="text"
                      value={pubSubject}
                      onChange={(e) => setPubSubject(e.target.value)}
                      required
                    />
                  </div>
                  <div className="form-group">
                    <label className="form-label">Payload JSON</label>
                    <textarea
                      className="textarea"
                      style={{
                        fontFamily: "var(--font-mono)",
                        fontSize: "0.8rem",
                      }}
                      value={pubPayload}
                      onChange={(e) => setPubPayload(e.target.value)}
                      required
                    />
                  </div>
                  <div className="form-group">
                    <label className="form-label">Optional Headers JSON</label>
                    <textarea
                      className="textarea"
                      style={{
                        fontFamily: "var(--font-mono)",
                        fontSize: "0.8rem",
                        minHeight: "60px",
                      }}
                      value={pubHeaders}
                      onChange={(e) => setPubHeaders(e.target.value)}
                    />
                  </div>
                  <button
                    className="btn btn-primary"
                    style={{ width: "100%" }}
                    type="submit"
                  >
                    Publish to Stream
                  </button>
                </form>

                {pubStatus && (
                  <div
                    className="badge"
                    style={{
                      display: "block",
                      marginTop: "1rem",
                      padding: "0.75rem",
                      textTransform: "none",
                      whiteSpace: "pre-wrap",
                      textAlign: "left",
                      backgroundColor:
                        pubStatus.type === "success"
                          ? "rgba(16, 185, 129, 0.1)"
                          : "rgba(239, 68, 68, 0.1)",
                      color:
                        pubStatus.type === "success"
                          ? "var(--color-success)"
                          : "var(--color-danger)",
                      border: `1px solid ${pubStatus.type === "success" ? "rgba(16, 185, 129, 0.2)" : "rgba(239, 68, 68, 0.2)"}`,
                    }}
                  >
                    <strong>{pubStatus.message}</strong>
                    {pubStatus.details && (
                      <ul
                        style={{
                          marginTop: "0.5rem",
                          paddingLeft: "1.25rem",
                          fontSize: "0.8rem",
                        }}
                      >
                        {pubStatus.details.map((d, i) => (
                          <li key={i}>{d}</li>
                        ))}
                      </ul>
                    )}
                  </div>
                )}
              </div>
            </div>

            {/* Right Column: Messages Inspector */}
            <div className="panel" style={{ minHeight: "600px" }}>
              <div className="panel-header">
                <div className="panel-title">
                  {activeStream
                    ? `Stream Messages Inspector [${activeStream}]`
                    : "Select a Stream to Inspect"}
                </div>
              </div>

              {!activeStream ? (
                <div
                  style={{
                    textAlign: "center",
                    color: "var(--text-muted)",
                    marginTop: "4rem",
                  }}
                >
                  Select a stream from the left list to browse, filter, and page
                  historical stream sequences.
                </div>
              ) : (
                <div>
                  <div
                    className="table-container"
                    style={{ maxHeight: "500px", overflowY: "auto" }}
                  >
                    <table className="table">
                      <thead>
                        <tr>
                          <th>Sequence</th>
                          <th>Subject</th>
                          <th>Timestamp</th>
                          <th>Payload View</th>
                        </tr>
                      </thead>
                      <tbody>
                        {streamMessages.length === 0 ? (
                          <tr>
                            <td
                              colSpan={4}
                              style={{
                                textAlign: "center",
                                color: "var(--text-muted)",
                              }}
                            >
                              No messages found in this stream. Use the Event
                              Publisher panel to publish events.
                            </td>
                          </tr>
                        ) : (
                          streamMessages.map((m) => (
                            <tr key={m.sequence}>
                              <td style={{ fontFamily: "var(--font-mono)" }}>
                                #{m.sequence}
                              </td>
                              <td>
                                <span className="badge badge-info">
                                  {m.subject}
                                </span>
                              </td>
                              <td
                                style={{
                                  fontSize: "0.8rem",
                                  color: "var(--text-secondary)",
                                }}
                              >
                                {new Date(m.time).toLocaleString()}
                              </td>
                              <td>
                                <pre
                                  style={{
                                    fontFamily: "var(--font-mono)",
                                    fontSize: "0.75rem",
                                    backgroundColor: "rgba(0,0,0,0.3)",
                                    padding: "0.5rem",
                                    borderRadius: "4px",
                                    whiteSpace: "pre-wrap",
                                    wordBreak: "break-all",
                                  }}
                                >
                                  {JSON.stringify(m.data, null, 2)}
                                </pre>
                                {Object.keys(m.headers || {}).length > 0 && (
                                  <div
                                    style={{
                                      fontSize: "0.7rem",
                                      color: "var(--text-muted)",
                                      marginTop: "4px",
                                    }}
                                  >
                                    Headers: {JSON.stringify(m.headers)}
                                  </div>
                                )}
                              </td>
                            </tr>
                          ))
                        )}
                      </tbody>
                    </table>
                  </div>

                  {nextOffsetSeq !== null && (
                    <div style={{ textAlign: "center", marginTop: "1rem" }}>
                      <button
                        className="btn btn-secondary"
                        onClick={() => {
                          void loadMoreMessages();
                        }}
                      >
                        Load Older Messages
                      </button>
                    </div>
                  )}
                </div>
              )}
            </div>
          </div>
        )}

        {/* 3. VIEW: WORKERS & SIMULATOR */}
        {activeTab === "consumers" && (
          <div>
            <div className="panel">
              <div className="panel-header">
                <div className="panel-title">Consumer Simulators</div>
                <button
                  className="btn btn-primary"
                  onClick={() => setShowCreateSimulator(true)}
                >
                  + Configure Simulator Group
                </button>
              </div>

              <div className="table-container">
                <table className="table">
                  <thead>
                    <tr>
                      <th>Durable Consumer Group</th>
                      <th>Stream Target</th>
                      <th>Subject Filter</th>
                      <th>Success Rate</th>
                      <th>Process Delay</th>
                      <th>Max Retries</th>
                      <th>Status</th>
                      <th>Actions</th>
                    </tr>
                  </thead>
                  <tbody>
                    {simulators.length === 0 ? (
                      <tr>
                        <td
                          colSpan={8}
                          style={{
                            textAlign: "center",
                            color: "var(--text-muted)",
                          }}
                        >
                          No simulated consumer groups configured. Add one below
                          to process events.
                        </td>
                      </tr>
                    ) : (
                      simulators.map((sim) => (
                        <tr key={sim.id}>
                          <td style={{ fontWeight: 600 }}>{sim.durableName}</td>
                          <td>{sim.stream}</td>
                          <td>
                            <span className="badge badge-info">
                              {sim.subject}
                            </span>
                          </td>
                          <td>
                            <div
                              style={{
                                display: "flex",
                                alignItems: "center",
                                gap: "0.5rem",
                              }}
                            >
                              <span>{(sim.successRate * 100).toFixed(0)}%</span>
                              <input
                                type="range"
                                min="0"
                                max="1"
                                step="0.1"
                                value={sim.successRate}
                                onChange={(e) => {
                                  const val = parseFloat(e.target.value);
                                  void (async () => {
                                    await fetch(
                                      `${BACKEND_URL}/simulator/${sim.durableName}`,
                                      {
                                        method: "PUT",
                                        headers: {
                                          "Content-Type": "application/json",
                                        },
                                        body: JSON.stringify({
                                          successRate: val,
                                        }),
                                      },
                                    );
                                    await refreshTab("consumers");
                                  })();
                                }}
                                style={{ width: "60px", cursor: "pointer" }}
                              />
                            </div>
                          </td>
                          <td>
                            <select
                              className="select"
                              style={{
                                padding: "0.25rem",
                                width: "100px",
                                fontSize: "0.8rem",
                              }}
                              value={sim.processingDelay}
                              onChange={(e) => {
                                const val = parseInt(e.target.value);
                                void (async () => {
                                  await fetch(
                                    `${BACKEND_URL}/simulator/${sim.durableName}`,
                                    {
                                      method: "PUT",
                                      headers: {
                                        "Content-Type": "application/json",
                                      },
                                      body: JSON.stringify({
                                        processingDelay: val,
                                      }),
                                    },
                                  );
                                  await refreshTab("consumers");
                                })();
                              }}
                            >
                              <option value="0">Instant</option>
                              <option value="100">100ms</option>
                              <option value="500">500ms</option>
                              <option value="1500">1.5s</option>
                              <option value="3000">3s</option>
                            </select>
                          </td>
                          <td>{sim.maxDeliver}</td>
                          <td>
                            <span
                              className={`badge ${sim.active ? "badge-success" : "badge-warning"}`}
                            >
                              {sim.active ? "running" : "idle"}
                            </span>
                          </td>
                          <td>
                            <div style={{ display: "flex", gap: "0.5rem" }}>
                              <button
                                className={`btn ${sim.active ? "btn-danger" : "btn-success"}`}
                                style={{
                                  padding: "0.3rem 0.6rem",
                                  fontSize: "0.75rem",
                                }}
                                onClick={() => {
                                  void toggleSimulator(sim);
                                }}
                              >
                                {sim.active ? "Stop" : "Start"}
                              </button>
                              <button
                                className="btn btn-secondary"
                                style={{
                                  padding: "0.3rem 0.6rem",
                                  fontSize: "0.75rem",
                                  color: "var(--color-danger)",
                                }}
                                onClick={() => {
                                  void handleDeleteSimulator(sim.durableName);
                                }}
                              >
                                Delete
                              </button>
                            </div>
                          </td>
                        </tr>
                      ))
                    )}
                  </tbody>
                </table>
              </div>
            </div>

            <div className="panel">
              <div className="panel-title" style={{ marginBottom: "1rem" }}>
                Telemetry logs for active consumers
              </div>
              <div className="console-log" style={{ maxHeight: "250px" }}>
                {logs.filter((l) =>
                  simulators.some((s) => s.durableName === l.durableName),
                ).length === 0 ? (
                  <div
                    style={{ color: "var(--text-muted)", fontStyle: "italic" }}
                  >
                    No consumer telemetry logs recorded yet. Start a consumer
                    above and publish events to populate.
                  </div>
                ) : (
                  logs
                    .filter((l) =>
                      simulators.some((s) => s.durableName === l.durableName),
                    )
                    .map((log, index) => (
                      <div
                        key={index}
                        className={`console-line console-line-${log.type}`}
                      >
                        <span
                          style={{
                            color: "var(--text-muted)",
                            marginRight: "0.5rem",
                          }}
                        >
                          [{log.timestamp.split("T")[1]?.slice(0, 8) || ""}]
                        </span>
                        <strong
                          style={{
                            color: "var(--text-primary)",
                            marginRight: "0.5rem",
                          }}
                        >
                          [{log.durableName}]
                        </strong>
                        {log.text}
                      </div>
                    ))
                )}
              </div>
            </div>
          </div>
        )}

        {/* 4. VIEW: DEAD-LETTER QUEUE (DLQ) */}
        {activeTab === "dlq" && (
          <div className="panel">
            <div className="panel-header">
              <div className="panel-title">
                Dead-Letter Queue Log (Database state)
              </div>
              {dlqEvents.length > 0 && (
                <button
                  className="btn btn-danger"
                  onClick={() => {
                    void handlePurgeDlq();
                  }}
                >
                  Purge DLQ Table
                </button>
              )}
            </div>

            <div className="table-container">
              <table className="table">
                <thead>
                  <tr>
                    <th>Event ID</th>
                    <th>Orig. Subject</th>
                    <th>Consumer Group</th>
                    <th>Err Trace</th>
                    <th>Attempts</th>
                    <th>Status</th>
                    <th>Log Time</th>
                    <th>Actions</th>
                  </tr>
                </thead>
                <tbody>
                  {dlqEvents.length === 0 ? (
                    <tr>
                      <td
                        colSpan={8}
                        style={{
                          textAlign: "center",
                          color: "var(--text-muted)",
                        }}
                      >
                        No dead-letter events found. Connect a worker with
                        simulated failures to generate backlog.
                      </td>
                    </tr>
                  ) : (
                    dlqEvents.map((event) => (
                      <React.Fragment key={event.id}>
                        <tr>
                          <td
                            style={{
                              fontFamily: "var(--font-mono)",
                              fontSize: "0.8rem",
                            }}
                          >
                            {event.id.slice(0, 8)}...
                          </td>
                          <td>
                            <span className="badge badge-info">
                              {event.subject}
                            </span>
                          </td>
                          <td>
                            <strong>{event.consumerGroup}</strong>
                          </td>
                          <td style={{ color: "var(--color-danger)" }}>
                            {event.errorReason}
                          </td>
                          <td>{event.numDelivered}</td>
                          <td>
                            <span
                              className={`badge ${
                                event.status === "PENDING"
                                  ? "badge-danger"
                                  : event.status === "REPLAYED"
                                    ? "badge-success"
                                    : "badge-warning"
                              }`}
                            >
                              {event.status}
                            </span>
                          </td>
                          <td
                            style={{
                              fontSize: "0.8rem",
                              color: "var(--text-muted)",
                            }}
                          >
                            {new Date(event.createdAt).toLocaleString()}
                          </td>
                          <td>
                            <div style={{ display: "flex", gap: "0.25rem" }}>
                              <button
                                className="btn btn-secondary"
                                style={{
                                  padding: "0.3rem 0.6rem",
                                  fontSize: "0.75rem",
                                }}
                                onClick={() =>
                                  setExpandedDlqId(
                                    expandedDlqId === event.id
                                      ? null
                                      : event.id,
                                  )
                                }
                              >
                                inspect
                              </button>
                              {event.status === "PENDING" && (
                                <>
                                  <button
                                    className="btn btn-success"
                                    style={{
                                      padding: "0.3rem 0.6rem",
                                      fontSize: "0.75rem",
                                    }}
                                    onClick={() => {
                                      void handleReplayDlq(event.id);
                                    }}
                                  >
                                    Replay
                                  </button>
                                  <button
                                    className="btn btn-secondary"
                                    style={{
                                      padding: "0.3rem 0.6rem",
                                      fontSize: "0.75rem",
                                      color: "var(--text-muted)",
                                    }}
                                    onClick={() => {
                                      void handleDismissDlq(event.id);
                                    }}
                                  >
                                    dismiss
                                  </button>
                                </>
                              )}
                            </div>
                          </td>
                        </tr>
                        {/* Expanded Payload Inspector Row */}
                        {expandedDlqId === event.id && (
                          <tr>
                            <td
                              colSpan={8}
                              style={{
                                backgroundColor: "rgba(0,0,0,0.4)",
                                padding: "1.5rem",
                              }}
                            >
                              <div
                                style={{
                                  display: "grid",
                                  gridTemplateColumns: "1fr 1fr",
                                  gap: "2rem",
                                }}
                              >
                                <div>
                                  <h4
                                    style={{
                                      fontSize: "0.85rem",
                                      color: "var(--text-secondary)",
                                      marginBottom: "0.5rem",
                                    }}
                                  >
                                    Message Payload JSON
                                  </h4>
                                  <pre
                                    style={{
                                      fontFamily: "var(--font-mono)",
                                      fontSize: "0.8rem",
                                      backgroundColor: "rgba(0,0,0,0.3)",
                                      padding: "1rem",
                                      borderRadius: "8px",
                                      overflowX: "auto",
                                      whiteSpace: "pre-wrap",
                                      wordBreak: "break-all",
                                    }}
                                  >
                                    {JSON.stringify(event.payload, null, 2)}
                                  </pre>
                                </div>
                                <div>
                                  <h4
                                    style={{
                                      fontSize: "0.85rem",
                                      color: "var(--text-secondary)",
                                      marginBottom: "0.5rem",
                                    }}
                                  >
                                    Metadata & Original Headers
                                  </h4>
                                  <pre
                                    style={{
                                      fontFamily: "var(--font-mono)",
                                      fontSize: "0.8rem",
                                      backgroundColor: "rgba(0,0,0,0.3)",
                                      padding: "1rem",
                                      borderRadius: "8px",
                                      overflowX: "auto",
                                      whiteSpace: "pre-wrap",
                                      wordBreak: "break-all",
                                    }}
                                  >
                                    {JSON.stringify(
                                      {
                                        stream: event.stream,
                                        originalSubject: event.subject,
                                        consumerGroup: event.consumerGroup,
                                        numDeliveredAttempts:
                                          event.numDelivered,
                                        headers: event.headers || {},
                                      },
                                      null,
                                      2,
                                    )}
                                  </pre>
                                  {event.status === "PENDING" && (
                                    <div
                                      style={{
                                        marginTop: "1rem",
                                        display: "flex",
                                        gap: "1rem",
                                        alignItems: "center",
                                      }}
                                    >
                                      <span
                                        style={{
                                          fontSize: "0.85rem",
                                          color: "var(--text-secondary)",
                                        }}
                                      >
                                        Replay to custom subject:
                                      </span>
                                      <input
                                        type="text"
                                        className="input"
                                        placeholder={event.subject}
                                        style={{
                                          width: "200px",
                                          padding: "0.4rem",
                                        }}
                                        onKeyDown={(e) => {
                                          if (e.key === "Enter") {
                                            void handleReplayDlq(
                                              event.id,
                                              e.currentTarget.value,
                                            );
                                          }
                                        }}
                                      />
                                    </div>
                                  )}
                                </div>
                              </div>
                            </td>
                          </tr>
                        )}
                      </React.Fragment>
                    ))
                  )}
                </tbody>
              </table>
            </div>
          </div>
        )}

        {/* 5. VIEW: SCHEMA REGISTRY */}
        {activeTab === "schemas" && (
          <div className="panel">
            <div className="panel-header">
              <div className="panel-title">Contract Validation Catalog</div>
              <button
                className="btn btn-primary"
                onClick={() => setShowCreateSchema(true)}
              >
                + Register Schema
              </button>
            </div>

            <div className="table-container">
              <table className="table">
                <thead>
                  <tr>
                    <th>Subject Match Pattern</th>
                    <th>Version</th>
                    <th>Description</th>
                    <th>JSON Schema definition</th>
                    <th>Registered At</th>
                    <th>Actions</th>
                  </tr>
                </thead>
                <tbody>
                  {schemas.length === 0 ? (
                    <tr>
                      <td
                        colSpan={6}
                        style={{
                          textAlign: "center",
                          color: "var(--text-muted)",
                        }}
                      >
                        No schemas registered. Add one to restrict payloads on
                        specific NATS subject match patterns.
                      </td>
                    </tr>
                  ) : (
                    schemas.map((s) => (
                      <tr key={s.id}>
                        <td>
                          <span
                            className="badge badge-info"
                            style={{ fontSize: "0.85rem" }}
                          >
                            {s.subjectPattern}
                          </span>
                        </td>
                        <td>v{s.version}</td>
                        <td style={{ color: "var(--text-secondary)" }}>
                          {s.description || "—"}
                        </td>
                        <td>
                          <pre
                            style={{
                              fontFamily: "var(--font-mono)",
                              fontSize: "0.75rem",
                              backgroundColor: "rgba(0,0,0,0.3)",
                              padding: "0.5rem",
                              borderRadius: "4px",
                              maxHeight: "150px",
                              overflowY: "auto",
                              whiteSpace: "pre-wrap",
                              wordBreak: "break-all",
                            }}
                          >
                            {JSON.stringify(s.schema, null, 2)}
                          </pre>
                        </td>
                        <td
                          style={{
                            fontSize: "0.8rem",
                            color: "var(--text-muted)",
                          }}
                        >
                          {new Date(s.createdAt).toLocaleDateString()}
                        </td>
                        <td>
                          <button
                            className="btn btn-secondary"
                            style={{
                              color: "var(--color-danger)",
                              padding: "0.3rem 0.6rem",
                              fontSize: "0.75rem",
                            }}
                            onClick={() => {
                              void handleDeleteSchema(s.id);
                            }}
                          >
                            Delete
                          </button>
                        </td>
                      </tr>
                    ))
                  )}
                </tbody>
              </table>
            </div>
          </div>
        )}
      </main>

      {/* MODAL 1: CREATE STREAM */}
      {showCreateStream && (
        <div
          className="modal-overlay"
          onClick={() => setShowCreateStream(false)}
        >
          <div className="modal-content" onClick={(e) => e.stopPropagation()}>
            <h3
              style={{
                fontFamily: "var(--font-display)",
                fontSize: "1.25rem",
                marginBottom: "1.25rem",
              }}
            >
              Create NATS JetStream
            </h3>
            <form
              onSubmit={(e) => {
                void handleCreateStream(e);
              }}
            >
              <div className="form-group">
                <label className="form-label">Stream Name</label>
                <input
                  className="input"
                  type="text"
                  placeholder="USER_STREAM"
                  value={newStreamName}
                  onChange={(e) => setNewStreamName(e.target.value)}
                  required
                />
              </div>
              <div className="form-group">
                <label className="form-label">Subjects (comma separated)</label>
                <input
                  className="input"
                  type="text"
                  placeholder="events.user.*, events.profile.>"
                  value={newStreamSubjects}
                  onChange={(e) => setNewStreamSubjects(e.target.value)}
                  required
                />
              </div>
              <div
                style={{
                  display: "flex",
                  gap: "1rem",
                  justifyContent: "flex-end",
                  marginTop: "2rem",
                }}
              >
                <button
                  className="btn btn-secondary"
                  type="button"
                  onClick={() => setShowCreateStream(false)}
                >
                  Cancel
                </button>
                <button className="btn btn-primary" type="submit">
                  Save Stream
                </button>
              </div>
            </form>
          </div>
        </div>
      )}

      {/* MODAL 2: CREATE SCHEMA */}
      {showCreateSchema && (
        <div
          className="modal-overlay"
          onClick={() => setShowCreateSchema(false)}
        >
          <div
            className="modal-content"
            style={{ maxWidth: "700px" }}
            onClick={(e) => e.stopPropagation()}
          >
            <h3
              style={{
                fontFamily: "var(--font-display)",
                fontSize: "1.25rem",
                marginBottom: "1.25rem",
              }}
            >
              Register Subject Schema Contract
            </h3>
            <form
              onSubmit={(e) => {
                void handleCreateSchema(e);
              }}
            >
              <div className="form-group">
                <label className="form-label">
                  Subject Match Pattern (e.g. <code>events.user.*</code>)
                </label>
                <input
                  className="input"
                  type="text"
                  value={newSchemaSubject}
                  onChange={(e) => setNewSchemaSubject(e.target.value)}
                  required
                />
              </div>
              <div className="form-group">
                <label className="form-label">Description</label>
                <input
                  className="input"
                  type="text"
                  value={newSchemaDesc}
                  onChange={(e) => setNewSchemaDesc(e.target.value)}
                />
              </div>
              <div className="form-group">
                <label className="form-label">JSON Schema Definition</label>
                <textarea
                  className="textarea"
                  style={{
                    fontFamily: "var(--font-mono)",
                    fontSize: "0.8rem",
                    minHeight: "220px",
                  }}
                  value={newSchemaDef}
                  onChange={(e) => setNewSchemaDef(e.target.value)}
                  required
                />
              </div>
              <div
                style={{
                  display: "flex",
                  gap: "1rem",
                  justifyContent: "flex-end",
                  marginTop: "2rem",
                }}
              >
                <button
                  className="btn btn-secondary"
                  type="button"
                  onClick={() => setShowCreateSchema(false)}
                >
                  Cancel
                </button>
                <button className="btn btn-primary" type="submit">
                  Register Contract
                </button>
              </div>
            </form>
          </div>
        </div>
      )}

      {/* MODAL 3: CONFIGURE SIMULATOR */}
      {showCreateSimulator && (
        <div
          className="modal-overlay"
          onClick={() => setShowCreateSimulator(false)}
        >
          <div className="modal-content" onClick={(e) => e.stopPropagation()}>
            <h3
              style={{
                fontFamily: "var(--font-display)",
                fontSize: "1.25rem",
                marginBottom: "1.25rem",
              }}
            >
              Configure Simulated Consumer
            </h3>
            <form
              onSubmit={(e) => {
                void handleCreateSimulator(e);
              }}
            >
              <div className="form-group">
                <label className="form-label">Target NATS Stream</label>
                <select
                  className="select"
                  value={newSimStream}
                  onChange={(e) => setNewSimStream(e.target.value)}
                >
                  {streams.map((s) => (
                    <option key={s.name} value={s.name}>
                      {s.name}
                    </option>
                  ))}
                </select>
              </div>
              <div className="form-group">
                <label className="form-label">Filter Subject</label>
                <input
                  className="input"
                  type="text"
                  value={newSimSubject}
                  onChange={(e) => setNewSimSubject(e.target.value)}
                  required
                />
              </div>
              <div className="form-group">
                <label className="form-label">
                  Durable Consumer Group Name
                </label>
                <input
                  className="input"
                  type="text"
                  value={newSimDurableName}
                  onChange={(e) => setNewSimDurableName(e.target.value)}
                  required
                />
              </div>
              <div
                style={{
                  display: "grid",
                  gridTemplateColumns: "1fr 1fr 1fr",
                  gap: "1rem",
                }}
              >
                <div className="form-group">
                  <label className="form-label">Success Rate (0 to 1)</label>
                  <input
                    className="input"
                    type="number"
                    min="0"
                    max="1"
                    step="0.1"
                    value={newSimSuccessRate}
                    onChange={(e) => setNewSimSuccessRate(e.target.value)}
                    required
                  />
                </div>
                <div className="form-group">
                  <label className="form-label">Delay (ms)</label>
                  <input
                    className="input"
                    type="number"
                    min="0"
                    value={newSimDelay}
                    onChange={(e) => setNewSimDelay(e.target.value)}
                    required
                  />
                </div>
                <div className="form-group">
                  <label className="form-label">Max Retries</label>
                  <input
                    className="input"
                    type="number"
                    min="1"
                    value={newSimMaxDeliver}
                    onChange={(e) => setNewSimMaxDeliver(e.target.value)}
                    required
                  />
                </div>
              </div>
              <div
                style={{
                  display: "flex",
                  gap: "1rem",
                  justifyContent: "flex-end",
                  marginTop: "2rem",
                }}
              >
                <button
                  className="btn btn-secondary"
                  type="button"
                  onClick={() => setShowCreateSimulator(false)}
                >
                  Cancel
                </button>
                <button className="btn btn-primary" type="submit">
                  Deploy Simulator
                </button>
              </div>
            </form>
          </div>
        </div>
      )}
    </div>
  );
}
