export type User = {
  id: string;
  email: string;
  full_name: string;
  role: "admin" | "engineer" | "analyst";
  status: "active" | "invited" | "disabled";
  created_at: string;
};

export type TokenPair = {
  access_token: string;
  refresh_token: string;
  token_type: string;
};

export type ToolCall = {
  tool_name: string;
  status: string;
  input: Record<string, unknown>;
  output: Record<string, unknown>;
  latency_ms: number;
};

export type AgentResponse = {
  session_id: string;
  message_id: string;
  intent: string;
  answer: string;
  tool_calls: ToolCall[];
  ui_actions: UiAction[];
};

export type UiAction =
  | { type: "navigate"; screen: string }
  | { type: "toast"; message: string }
  | { type: string; [key: string]: unknown };

export type SqlResult = {
  columns: string[];
  rows: Record<string, unknown>[];
  row_count: number;
  latency_ms: number;
};

export type CatalogTable = {
  schema_name: string;
  name: string;
  type: string;
  columns: { name: string; type: string; nullable: boolean }[];
};

export type Pipeline = {
  dag_id: string;
  name: string;
  schedule: string;
  status: string;
  owner: string;
  last_run?: string | null;
  next_run?: string | null;
};

export type AirflowRun = {
  dag_id: string;
  run_id: string;
  status: string;
  external_url?: string | null;
  created_at?: string | null;
};

export type SparkJob = {
  job_id: string;
  name: string;
  status: string;
  app_resource: string;
  params: Record<string, unknown>;
  result_sample?: Record<string, unknown>[] | null;
  driver_log?: string | null;
  created_at?: string | null;
};
