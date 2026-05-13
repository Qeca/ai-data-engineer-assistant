import { Badge, Card } from "@/components/ui";

const connections = [
  { name: "PostgreSQL warehouse", status: "active", latency: "local", type: "SQL metadata + app DB" },
  { name: "Airflow REST API", status: "active", latency: "/api/v1", type: "DAG run control" },
  { name: "Spark standalone", status: "active", latency: "spark://spark-master:7077", type: "job metadata adapter" },
  { name: "MagnitGPT", status: "active", latency: "llmlite /v1", type: "ReAct chat tool calling" },
  { name: "OpenAI Responses API", status: "invited", latency: "env OPENAI_API_KEY", type: "function calling" },
  { name: "OpenRouter", status: "invited", latency: "env OPENROUTER_API_KEY", type: "chat tool calling" },
  { name: "Langfuse", status: "invited", latency: "env LANGFUSE_*", type: "trace export" },
];

export function ConnectionsScreen() {
  return (
    <div className="content">
      <div className="grid-3">
        {connections.map((connection) => (
          <Card key={connection.name} title={connection.name} sub={connection.type}>
            <div style={{ display: "flex", alignItems: "center", gap: 10 }}>
              <Badge status={connection.status}>{connection.status === "active" ? "configured" : "env required"}</Badge>
              <span className="tag">{connection.latency}</span>
            </div>
          </Card>
        ))}
      </div>
    </div>
  );
}
