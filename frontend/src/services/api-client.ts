const API_BASE = import.meta.env.VITE_API_URL || "";

function getToken(): string | null {
  return localStorage.getItem("nats-ui-token");
}

export function setToken(token: string) {
  localStorage.setItem("nats-ui-token", token);
}

export function clearToken() {
  localStorage.removeItem("nats-ui-token");
}

export function hasToken(): boolean {
  return !!getToken();
}

async function request<T>(path: string, options: RequestInit = {}): Promise<T> {
  const token = getToken();
  const headers: Record<string, string> = {
    "Content-Type": "application/json",
    ...((options.headers as Record<string, string>) || {}),
  };
  if (token) {
    headers["Authorization"] = `Bearer ${token}`;
  }

  const response = await fetch(`${API_BASE}/api${path}`, {
    ...options,
    headers,
  });

  if (response.status === 401) {
    clearToken();
    window.location.href = "/login";
    throw new Error("Unauthorized");
  }

  if (!response.ok) {
    const body = await response
      .json()
      .catch(() => ({ error: response.statusText }));
    throw new Error(body.error || `HTTP ${response.status}`);
  }

  return response.json();
}

// Servers
export async function listServers(): Promise<{ servers: string[] }> {
  return request("/servers");
}

// Auth
export async function login(
  username: string,
  password: string,
): Promise<{ token: string; username: string }> {
  const res = await request<{ token: string; username: string }>(
    "/auth/login",
    {
      method: "POST",
      body: JSON.stringify({ username, password }),
    },
  );
  setToken(res.token);
  return res;
}

export async function getMe(): Promise<{ username: string }> {
  return request("/auth/me");
}

export async function getOAuth2Providers(): Promise<
  { name: string; clientId: string }[]
> {
  return request("/auth/oauth2/providers");
}

export function getOAuth2AuthorizeURL(provider: string): string {
  return `${API_BASE}/api/auth/oauth2/${provider}/authorize`;
}

// Server (with server parameter)
export async function fetchServerInfo(
  server: string,
): Promise<Record<string, unknown>> {
  return request(`/servers/${server}/info`);
}

export async function fetchConnections(
  server: string,
  subs?: boolean,
): Promise<Record<string, unknown>> {
  const q = subs ? "?subs=1" : "";
  return request(`/servers/${server}/connections${q}`);
}

export async function fetchJetStreamInfo(
  server: string,
  params?: string,
): Promise<Record<string, unknown>> {
  const q = params ? `?${params}` : "";
  return request(`/servers/${server}/jetstream${q}`);
}

export async function fetchSubscriptions(
  server: string,
): Promise<Record<string, unknown>> {
  return request(`/servers/${server}/subscriptions`);
}

export async function fetchRoutes(
  server: string,
): Promise<Record<string, unknown>> {
  return request(`/servers/${server}/routes`);
}

export async function fetchGateways(
  server: string,
): Promise<Record<string, unknown>> {
  return request(`/servers/${server}/gateways`);
}

export async function fetchLeafnodes(
  server: string,
): Promise<Record<string, unknown>> {
  return request(`/servers/${server}/leafnodes`);
}

export async function fetchAccounts(
  server: string,
): Promise<Record<string, unknown>> {
  return request(`/servers/${server}/accounts`);
}

export async function fetchAccountDetail(
  server: string,
  account: string,
): Promise<Record<string, unknown>> {
  return request(`/servers/${server}/accounts/${account}`);
}

export async function fetchVarz(
  server: string,
): Promise<Record<string, unknown>> {
  return request(`/servers/${server}/varz`);
}

export async function checkHealth(
  server: string,
): Promise<{ status: string; connected: boolean }> {
  return request(`/servers/${server}/healthz`);
}

// Streams (with server parameter)
export interface StreamInfo {
  config: Record<string, unknown>;
  state: Record<string, unknown>;
}

export async function listStreams(server: string): Promise<StreamInfo[]> {
  return request(`/servers/${server}/streams`);
}

export async function getStream(
  server: string,
  name: string,
): Promise<StreamInfo> {
  return request(`/servers/${server}/streams/${name}`);
}

export async function createStream(
  server: string,
  config: {
    name: string;
    subjects: string[];
    description?: string;
    retention: string;
    storage: string;
    maxMsgs: number;
    maxBytes: number;
    maxAge: number;
    replicas: number;
  },
): Promise<StreamInfo> {
  return request(`/servers/${server}/streams`, {
    method: "POST",
    body: JSON.stringify(config),
  });
}

export async function updateStream(
  server: string,
  name: string,
  config: {
    subjects: string[];
    description?: string;
    retention: string;
    storage: string;
    maxMsgs: number;
    maxBytes: number;
    maxAge: number;
    replicas: number;
  },
): Promise<StreamInfo> {
  return request(`/servers/${server}/streams/${name}`, {
    method: "PUT",
    body: JSON.stringify(config),
  });
}

export async function deleteStream(
  server: string,
  name: string,
): Promise<void> {
  await request(`/servers/${server}/streams/${name}`, { method: "DELETE" });
}

export async function purgeStream(
  server: string,
  name: string,
  subject?: string,
): Promise<void> {
  const body = subject ? JSON.stringify({ subject }) : undefined;
  await request(`/servers/${server}/streams/${name}/purge`, {
    method: "POST",
    body,
  });
}

export async function sealStream(server: string, name: string): Promise<void> {
  await request(`/servers/${server}/streams/${name}/seal`, { method: "POST" });
}

export interface StreamMessage {
  sequence: number;
  subject: string;
  data: unknown;
  headers: Record<string, string>;
  timestamp: string;
}

export async function getStreamMessages(
  server: string,
  name: string,
  last?: number,
): Promise<StreamMessage[]> {
  const q = last ? `?last=${last}` : "";
  return request(`/servers/${server}/streams/${name}/messages${q}`);
}

// Consumers (with server parameter)
export interface ConsumerInfo {
  config: Record<string, unknown>;
  stream_name: string;
  name: string;
  delivered: Record<string, unknown>;
  ack_floor: Record<string, unknown>;
  num_pending: number;
  num_waiting: number;
  num_ack_pending: number;
  created: string;
}

export async function listConsumers(
  server: string,
  streamName: string,
): Promise<ConsumerInfo[]> {
  return request(`/servers/${server}/streams/${streamName}/consumers`);
}

export async function getConsumer(
  server: string,
  streamName: string,
  consumerName: string,
): Promise<ConsumerInfo> {
  return request(
    `/servers/${server}/streams/${streamName}/consumers/${consumerName}`,
  );
}

export async function createConsumer(
  server: string,
  streamName: string,
  config: {
    name: string;
    filterSubject?: string;
    deliverPolicy?: string;
    ackPolicy?: string;
    maxDeliver?: number;
    maxAckPending?: number;
    description?: string;
    durable?: boolean;
  },
): Promise<ConsumerInfo> {
  return request(`/servers/${server}/streams/${streamName}/consumers`, {
    method: "POST",
    body: JSON.stringify(config),
  });
}

export async function deleteConsumer(
  server: string,
  streamName: string,
  consumerName: string,
): Promise<void> {
  await request(
    `/servers/${server}/streams/${streamName}/consumers/${consumerName}`,
    { method: "DELETE" },
  );
}

export async function pauseConsumer(
  server: string,
  streamName: string,
  consumerName: string,
  pauseUntil?: string,
): Promise<{ paused: boolean; pause_until?: string }> {
  return request(
    `/servers/${server}/streams/${streamName}/consumers/${consumerName}/pause`,
    {
      method: "POST",
      body: JSON.stringify(pauseUntil ? { pause_until: pauseUntil } : {}),
    },
  );
}

export async function resumeConsumer(
  server: string,
  streamName: string,
  consumerName: string,
): Promise<{ paused: boolean }> {
  return request(
    `/servers/${server}/streams/${streamName}/consumers/${consumerName}/resume`,
    { method: "POST" },
  );
}

// KV Store (with server parameter)
export interface KVBucket {
  name: string;
  values?: number;
  bytes?: number;
  history?: number;
  ttl?: number;
}

export async function listKVBuckets(server: string): Promise<KVBucket[]> {
  return request(`/servers/${server}/kv`);
}

export async function createKVBucket(
  server: string,
  name: string,
  ttl?: number,
  history?: number,
): Promise<KVBucket> {
  return request(`/servers/${server}/kv`, {
    method: "POST",
    body: JSON.stringify({ name, ttl, history }),
  });
}

export async function deleteKVBucket(
  server: string,
  name: string,
): Promise<void> {
  await request(`/servers/${server}/kv/${name}`, { method: "DELETE" });
}

export async function listKVKeys(
  server: string,
  bucket: string,
): Promise<string[]> {
  return request(`/servers/${server}/kv/${bucket}/keys`);
}

export async function getKVValue(
  server: string,
  bucket: string,
  key: string,
): Promise<{ key: string; value: string; revision: number; created: string }> {
  return request(`/servers/${server}/kv/${bucket}/keys/${key}`);
}

export async function putKVValue(
  server: string,
  bucket: string,
  key: string,
  value: string,
): Promise<{ key: string; revision: number }> {
  return request(`/servers/${server}/kv/${bucket}/keys/${key}`, {
    method: "PUT",
    body: JSON.stringify({ value }),
  });
}

export async function deleteKVKey(
  server: string,
  bucket: string,
  key: string,
): Promise<void> {
  await request(`/servers/${server}/kv/${bucket}/keys/${key}`, {
    method: "DELETE",
  });
}

// Messages (with server parameter)
export async function publishMessage(
  server: string,
  subject: string,
  data: unknown,
  headers?: Record<string, string>,
): Promise<void> {
  await request(`/servers/${server}/messages/publish`, {
    method: "POST",
    body: JSON.stringify({ subject, data, headers }),
  });
}

export async function fetchActiveSubjects(server: string): Promise<string[]> {
  return request(`/servers/${server}/messages/subjects`);
}

export interface RequestReplyResponse {
  subject: string;
  data: unknown;
  headers: Record<string, string>;
  timestamp: number;
}

export async function requestReply(
  server: string,
  subject: string,
  data: unknown,
  headers?: Record<string, string>,
  timeout?: number,
): Promise<RequestReplyResponse> {
  return request(`/servers/${server}/messages/request`, {
    method: "POST",
    body: JSON.stringify({ subject, data, headers, timeout }),
  });
}

// SSE Subscribe with automatic reconnection and exponential backoff
export function subscribeSSE(
  server: string,
  subject: string,
  onMessage: (msg: {
    subject: string;
    data: unknown;
    headers?: Record<string, string>;
    timestamp: number;
    reply?: string;
  }) => void,
  onError?: (err: Event) => void,
): () => void {
  let es: EventSource | null = null;
  let closed = false;
  let retryDelay = 1000;
  const maxRetryDelay = 30000;
  let retryTimeout: ReturnType<typeof setTimeout> | null = null;

  function connect() {
    if (closed) return;
    const token = getToken();
    const url = `${API_BASE}/api/servers/${server}/messages/subscribe?subject=${encodeURIComponent(subject)}&token=${encodeURIComponent(token || "")}`;
    es = new EventSource(url);

    es.onopen = () => {
      retryDelay = 1000; // reset backoff on successful connection
    };

    es.onmessage = (event) => {
      try {
        const msg = JSON.parse(event.data);
        onMessage(msg);
      } catch (e) {
        console.error("Failed to parse SSE message:", e);
      }
    };

    es.onerror = (err) => {
      onError?.(err);
      es?.close();
      if (!closed) {
        retryTimeout = setTimeout(() => {
          retryDelay = Math.min(retryDelay * 2, maxRetryDelay);
          connect();
        }, retryDelay);
      }
    };
  }

  connect();

  return () => {
    closed = true;
    if (retryTimeout) clearTimeout(retryTimeout);
    es?.close();
  };
}

// Object Store (with server parameter)
export interface ObjectStoreBucket {
  name: string;
  description: string;
  sealed: boolean;
  size: number;
  chunks: number;
  ttl?: number;
  storage?: string;
  replicas?: number;
}

export interface ObjectInfo {
  name: string;
  description: string;
  size: number;
  chunks: number;
  digest: string;
  modified: string;
}

export async function listObjectStoreBuckets(
  server: string,
): Promise<ObjectStoreBucket[]> {
  return request(`/servers/${server}/objectstore`);
}

export async function getObjectStoreBucket(
  server: string,
  name: string,
): Promise<ObjectStoreBucket> {
  return request(`/servers/${server}/objectstore/${name}`);
}

export async function createObjectStoreBucket(
  server: string,
  config: {
    name: string;
    description?: string;
    max_bytes?: number;
    max_chunk_size?: number;
    ttl?: number;
  },
): Promise<ObjectStoreBucket> {
  return request(`/servers/${server}/objectstore`, {
    method: "POST",
    body: JSON.stringify(config),
  });
}

export async function deleteObjectStoreBucket(
  server: string,
  name: string,
): Promise<void> {
  await request(`/servers/${server}/objectstore/${name}`, { method: "DELETE" });
}

export async function listObjects(
  server: string,
  bucket: string,
): Promise<ObjectInfo[]> {
  return request(`/servers/${server}/objectstore/${bucket}/objects`);
}

export async function getObjectInfo(
  server: string,
  bucket: string,
  name: string,
): Promise<ObjectInfo> {
  return request(
    `/servers/${server}/objectstore/${bucket}/objects/${name}/info`,
  );
}

export async function uploadObject(
  server: string,
  bucket: string,
  name: string,
  data: Blob | ArrayBuffer,
): Promise<void> {
  const token = localStorage.getItem("nats-ui-token");
  const headers: Record<string, string> = {};
  if (token) headers["Authorization"] = `Bearer ${token}`;

  const response = await fetch(
    `${API_BASE}/api/servers/${server}/objectstore/${bucket}/objects/${name}`,
    {
      method: "PUT",
      headers,
      body: data,
    },
  );
  if (!response.ok) {
    const body = await response
      .json()
      .catch(() => ({ error: response.statusText }));
    throw new Error(body.error || `HTTP ${response.status}`);
  }
}

export async function downloadObject(
  server: string,
  bucket: string,
  name: string,
): Promise<Blob> {
  const token = localStorage.getItem("nats-ui-token");
  const headers: Record<string, string> = {};
  if (token) headers["Authorization"] = `Bearer ${token}`;

  const response = await fetch(
    `${API_BASE}/api/servers/${server}/objectstore/${bucket}/objects/${name}`,
    {
      method: "GET",
      headers,
    },
  );
  if (!response.ok) throw new Error(`HTTP ${response.status}`);
  return response.blob();
}

export async function deleteObject(
  server: string,
  bucket: string,
  name: string,
): Promise<void> {
  await request(`/servers/${server}/objectstore/${bucket}/objects/${name}`, {
    method: "DELETE",
  });
}

// Stream message replay
export async function replayStreamMessages(
  server: string,
  streamName: string,
  params: {
    seq?: number;
    last?: number;
    subject?: string;
    start_time?: string;
    limit?: number;
  },
): Promise<StreamMessage[]> {
  const q = new URLSearchParams();
  if (params.seq) q.set("seq", String(params.seq));
  if (params.last) q.set("last", String(params.last));
  if (params.subject) q.set("subject", params.subject);
  if (params.start_time) q.set("start_time", params.start_time);
  if (params.limit) q.set("limit", String(params.limit));
  return request(
    `/servers/${server}/streams/${streamName}/messages?${q.toString()}`,
  );
}
