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

export type AgentSession = {
  id: string;
  title: string;
  created_at: string;
  updated_at: string;
};

export type AgentMessage = {
  id: string;
  role: "user" | "assistant" | string;
  content: string;
  metadata_json?: Record<string, unknown> | null;
  created_at: string;
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
  description?: string | null;
  last_run?: string | null;
  next_run?: string | null;
  tasks?: { task_id?: string; operator_name?: string; downstream_task_ids?: string[] }[];
};

export type AirflowRun = {
  dag_id: string;
  run_id: string;
  status: string;
  external_url?: string | null;
  created_at?: string | null;
};

export type AirflowTaskInstance = {
  dag_id: string;
  run_id: string;
  task_id: string;
  state: string;
  try_number: number;
  operator?: string | null;
  start_date?: string | null;
  end_date?: string | null;
  duration?: number | null;
};

export type AirflowTaskLog = {
  dag_id: string;
  run_id: string;
  task_id: string;
  try_number: number;
  content: string;
  source?: string;
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
