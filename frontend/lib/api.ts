import type {
  AgentResponse,
  AirflowRun,
  CatalogTable,
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

  return response.json() as Promise<T>;
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
  executeSql: (token: string, query: string, limit = 100) =>
    request<SqlResult>("/sql/execute", {
      method: "POST",
      token,
      body: JSON.stringify({ query, limit }),
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
  submitSpark: (token: string, payload: { name: string; app_resource: string; params: Record<string, unknown> }) =>
    request<SparkJob>("/spark/jobs", {
      method: "POST",
      token,
      body: JSON.stringify(payload),
    }),
  getSpark: (token: string, jobId: string) =>
    request<SparkJob>(`/spark/jobs/${encodeURIComponent(jobId)}`, { token }),
};
