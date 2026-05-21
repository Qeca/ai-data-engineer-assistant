import type {
  AgentMessage,
  AgentResponse,
  AgentSession,
  AgentStreamEvent,
  AirflowTaskInstance,
  AirflowRun,
  AirflowTaskLog,
  ArtifactVersion,
  CatalogTable,
  DatabaseConnection,
  DatabaseConnectionPayload,
  DatabaseConnectionTest,
  Pipeline,
  SparkJob,
  SqlResult,
  TokenPair,
  User,
} from "@/types";

const API_URL = process.env.NEXT_PUBLIC_API_URL ?? "/api/backend";

type ApiOptions = RequestInit & {
  token?: string | null;
};

async function request<T>(path: string, options: ApiOptions = {}): Promise<T> {
  const headers = new Headers(options.headers);
  headers.set("Content-Type", "application/json");
  if (options.token) {
    headers.set("Authorization", `Bearer ${options.token}`);
  }

  const response = await fetch(`${API_URL}${path}`, {
    ...options,
    headers,
  });

  if (!response.ok) {
    let detail = response.statusText;
    try {
      const body = await response.json();
      detail = body.detail ?? detail;
    } catch {
      // ignore non-JSON errors
    }
    throw new Error(Array.isArray(detail) ? detail.map((item) => item.msg).join(", ") : detail);
  }

  if (response.status === 204) {
    return undefined as T;
  }

  return response.json() as Promise<T>;
}

async function requestStream<T>(
  path: string,
  options: ApiOptions,
  onEvent: (event: AgentStreamEvent) => void,
): Promise<T> {
  const headers = new Headers(options.headers);
  headers.set("Content-Type", "application/json");
  if (options.token) {
    headers.set("Authorization", `Bearer ${options.token}`);
  }

  const response = await fetch(`${API_URL}${path}`, {
    ...options,
    headers,
  });

  if (!response.ok || !response.body) {
    throw new Error(response.statusText || "Stream request failed");
  }

  const reader = response.body.getReader();
  const decoder = new TextDecoder();
  let buffer = "";
  let finalValue: T | null = null;

  while (true) {
    const { value, done } = await reader.read();
    if (done) break;
    buffer += decoder.decode(value, { stream: true });
    const lines = buffer.split("\n");
    buffer = lines.pop() ?? "";

    for (const line of lines) {
      if (!line.trim()) continue;
      const event = JSON.parse(line) as AgentStreamEvent;
      onEvent(event);
      if (event.type === "final") {
        finalValue = event.response as T;
      }
      if (event.type === "error") {
        throw new Error(event.error);
      }
    }
  }

  if (buffer.trim()) {
    const event = JSON.parse(buffer) as AgentStreamEvent;
    onEvent(event);
    if (event.type === "final") {
      finalValue = event.response as T;
    }
    if (event.type === "error") {
      throw new Error(event.error);
    }
  }

  if (!finalValue) {
    throw new Error("Agent stream finished without final response");
  }
  return finalValue;
}

export const api = {
  login: (email: string, password: string) =>
    request<TokenPair>("/auth/login", {
      method: "POST",
      body: JSON.stringify({ email, password }),
    }),
  me: (token: string) => request<User>("/me", { token }),
  users: (token: string) => request<User[]>("/users", { token }),
  createUser: (token: string, payload: { email: string; full_name: string; role: string; password: string }) =>
    request<User>("/users", {
      method: "POST",
      token,
      body: JSON.stringify(payload),
    }),
  agentQuery: (
    token: string,
    query: string,
    sessionId?: string | null,
    appState?: Record<string, unknown>,
  ) =>
    request<AgentResponse>("/agent/query", {
      method: "POST",
      token,
      body: JSON.stringify({ query, session_id: sessionId, app_state: appState ?? {} }),
    }),
  agentQueryStream: (
    token: string,
    query: string,
    sessionId: string | null | undefined,
    appState: Record<string, unknown> | undefined,
    onEvent: (event: AgentStreamEvent) => void,
  ) =>
    requestStream<AgentResponse>(
      "/agent/query/stream",
      {
        method: "POST",
        token,
        body: JSON.stringify({ query, session_id: sessionId, app_state: appState ?? {} }),
      },
      onEvent,
    ),
  sessions: (token: string) => request<AgentSession[]>("/sessions", { token }),
  sessionMessages: (token: string, sessionId: string) =>
    request<AgentMessage[]>(`/sessions/${encodeURIComponent(sessionId)}/messages`, { token }),
  deleteSession: (token: string, sessionId: string) =>
    request<void>(`/sessions/${encodeURIComponent(sessionId)}`, {
      method: "DELETE",
      token,
    }),
  executeSql: (token: string, query: string, limit = 100) =>
    request<SqlResult>("/sql/execute", {
      method: "POST",
      token,
      body: JSON.stringify({ query, limit }),
    }),
  connections: (token: string) => request<DatabaseConnection[]>("/connections", { token }),
  createConnection: (token: string, payload: DatabaseConnectionPayload) =>
    request<DatabaseConnection>("/connections", {
      method: "POST",
      token,
      body: JSON.stringify(payload),
    }),
  updateConnection: (token: string, id: string, payload: Partial<DatabaseConnectionPayload>) =>
    request<DatabaseConnection>(`/connections/${encodeURIComponent(id)}`, {
      method: "PATCH",
      token,
      body: JSON.stringify(payload),
    }),
  testConnection: (token: string, id: string) =>
    request<DatabaseConnectionTest>(`/connections/${encodeURIComponent(id)}/test`, {
      method: "POST",
      token,
      body: JSON.stringify({}),
    }),
  catalogTables: (token: string) => request<CatalogTable[]>("/catalog/tables", { token }),
  pipelines: (token: string) => request<Pipeline[]>("/pipelines", { token }),
  triggerDag: (token: string, dagId: string) =>
    request<AirflowRun>(`/airflow/dags/${encodeURIComponent(dagId)}/runs`, {
      method: "POST",
      token,
      body: JSON.stringify({ conf: { source: "frontend" } }),
    }),
  getDagRun: (token: string, dagId: string, runId: string) =>
    request<AirflowRun>(
      `/airflow/dags/${encodeURIComponent(dagId)}/runs/${encodeURIComponent(runId)}`,
      { token },
    ),
  dagRuns: (token: string, dagId: string) =>
    request<AirflowRun[]>(`/airflow/dags/${encodeURIComponent(dagId)}/runs`, { token }),
  dagTaskInstances: (token: string, dagId: string, runId: string) =>
    request<AirflowTaskInstance[]>(
      `/airflow/dags/${encodeURIComponent(dagId)}/runs/${encodeURIComponent(runId)}/tasks`,
      { token },
    ),
  dagTaskLog: (token: string, dagId: string, runId: string, taskId: string, tryNumber = 1) =>
    request<AirflowTaskLog>(
      `/airflow/dags/${encodeURIComponent(dagId)}/runs/${encodeURIComponent(runId)}/tasks/${encodeURIComponent(taskId)}/logs/${tryNumber}`,
      { token },
    ),
  submitSpark: (token: string, payload: { name: string; app_resource: string; params: Record<string, unknown> }) =>
    request<SparkJob>("/spark/jobs", {
      method: "POST",
      token,
      body: JSON.stringify(payload),
    }),
  sparkJobs: (token: string) => request<SparkJob[]>("/spark/jobs", { token }),
  getSpark: (token: string, jobId: string) =>
    request<SparkJob>(`/spark/jobs/${encodeURIComponent(jobId)}`, { token }),
  artifactVersions: (token: string, artifactType: string, artifactName: string) =>
    request<{ scope: string; versions: ArtifactVersion[] }>(
      `/artifacts/versions?artifact_type=${encodeURIComponent(artifactType)}&artifact_name=${encodeURIComponent(artifactName)}`,
      { token },
    ),
};
