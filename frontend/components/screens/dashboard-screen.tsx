"use client";

import { useQuery } from "@tanstack/react-query";
import { Bot, GitBranch, Play, Table2 } from "lucide-react";
import { api } from "@/lib/api";
import { useAppStore } from "@/lib/store";
import { Badge, Card, StatCard } from "@/components/ui";

export function DashboardScreen() {
  const token = useAppStore((state) => state.accessToken);
  const setScreen = useAppStore((state) => state.setScreen);
  const pipelines = useQuery({
    queryKey: ["pipelines", token],
    queryFn: () => api.pipelines(token ?? ""),
    enabled: Boolean(token),
  });

  const rows = pipelines.data ?? [];
  const failed = rows.filter((row) => row.status === "failed").length;

  return (
    <div className="content">
      <div className="grid-4" style={{ marginBottom: 16 }}>
        <StatCard label="Active Pipelines" value={rows.length || 4} note="Airflow registry" tone="var(--blue-300)" />
        <StatCard label="Failed Jobs" value={failed} note="require attention" tone="var(--rose-400)" />
        <StatCard label="Data Processed" value="2.4 TB" note="demo warehouse sample" />
        <StatCard label="SLA Compliance" value="98.7%" note="last 24h" tone="var(--emerald-400)" />
      </div>

      <div style={{ display: "grid", gridTemplateColumns: "minmax(0, 1fr) 330px", gap: 14 }}>
        <Card
          title="Pipeline Health"
          sub="Live through backend adapter"
          action={<button className="btn btn-ghost" onClick={() => setScreen("pipelines")}>View all</button>}
        >
          <div className="table-wrap">
            <table className="data-table">
              <thead>
                <tr>
                  <th>Pipeline</th>
                  <th>Status</th>
                  <th>Owner</th>
                  <th>Schedule</th>
                  <th>Next run</th>
                </tr>
              </thead>
              <tbody>
                {rows.map((pipeline) => (
                  <tr key={pipeline.dag_id}>
                    <td>
                      <div className="mono">{pipeline.dag_id}</div>
                      <div className="card-sub">{pipeline.name}</div>
                    </td>
                    <td><Badge status={pipeline.status} /></td>
                    <td>{pipeline.owner}</td>
                    <td><span className="tag">{pipeline.schedule}</span></td>
                    <td>{pipeline.next_run ?? "queued"}</td>
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
        </Card>

        <div style={{ display: "flex", flexDirection: "column", gap: 14 }}>
          <Card title="Quick Actions">
            <div style={{ display: "grid", gridTemplateColumns: "1fr 1fr", gap: 8 }}>
              <button className="btn btn-secondary" onClick={() => setScreen("ai-agent")}><Bot size={14} /> Ask AI</button>
              <button className="btn btn-secondary" onClick={() => setScreen("sql")}><Table2 size={14} /> SQL</button>
              <button className="btn btn-secondary" onClick={() => setScreen("airflow")}><Play size={14} /> DAG Run</button>
              <button className="btn btn-secondary" onClick={() => setScreen("spark")}><GitBranch size={14} /> Spark</button>
            </div>
          </Card>

          <Card title="AI Insight">
            <div style={{ display: "flex", gap: 10, alignItems: "flex-start" }}>
              <div className="avatar ai">AI</div>
              <div>
                <p style={{ margin: 0, color: "var(--text-secondary)" }}>
                  `clickstream_aggregation` выглядит как главный риск: есть OOM в Spark-задаче. Для демо можно
                  запустить agent query или Spark job и получить сохранённый результат.
                </p>
                <button className="btn btn-ai" style={{ marginTop: 12 }} onClick={() => setScreen("ai-agent")}>
                  Open agent
                </button>
              </div>
            </div>
          </Card>
        </div>
      </div>
    </div>
  );
}
