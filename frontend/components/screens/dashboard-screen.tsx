"use client";

import { useQuery } from "@tanstack/react-query";
import { Bot, GitBranch, Play, Table2 } from "lucide-react";
import { api } from "@/lib/api";
import { useAppStore } from "@/lib/store";
import { Badge, Card, StatCard } from "@/components/ui";
import type { DatabaseConnection, Pipeline, SparkJob } from "@/types";

export function DashboardScreen() {
  const token = useAppStore((state) => state.accessToken);
  const setScreen = useAppStore((state) => state.setScreen);
  const pipelines = useQuery({
    queryKey: ["pipelines", token],
    queryFn: () => api.pipelines(token ?? ""),
    enabled: Boolean(token),
    refetchInterval: 30_000,
  });

  const sparkJobs = useQuery({
    queryKey: ["spark-jobs", token],
    queryFn: () => api.sparkJobs(token ?? ""),
    enabled: Boolean(token),
    refetchInterval: 30_000,
  });

  const connections = useQuery({
    queryKey: ["database-connections", token],
    queryFn: () => api.connections(token ?? ""),
    enabled: Boolean(token),
    refetchInterval: 30_000,
  });

  const rows = pipelines.data ?? [];
  const visibleRows = sortPipelinesForHealth(rows).slice(0, 10);
  const active = rows.filter((row) => row.status === "active").length;
  const failed = countFailedPipelines(rows) + countFailedSparkJobs(sparkJobs.data ?? []);
  const sla = calculateSla(rows);
  const insight = buildInsight({
    pipelines: rows,
    sparkJobs: sparkJobs.data ?? [],
    connections: connections.data ?? [],
    isLoading: pipelines.isLoading || sparkJobs.isLoading || connections.isLoading,
    hasError: Boolean(pipelines.error || sparkJobs.error || connections.error),
  });

  return (
    <div className="content">
      <div className="grid-4" style={{ marginBottom: 16 }}>
        <StatCard label="Active Pipelines" value={pipelines.isLoading ? "..." : active} note={`${rows.length} total in Airflow`} tone="var(--blue-300)" />
        <StatCard label="Failed Jobs" value={failed} note="require attention" tone="var(--rose-400)" />
        <StatCard label="Data Processed" value="2.4 TB" note="demo warehouse sample" />
        <StatCard label="SLA Compliance" value={sla} note="latest DAG runs" tone="var(--emerald-400)" />
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
                {visibleRows.map((pipeline) => (
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
            {pipelines.isLoading && <div className="empty-state">Загружаю состояние Airflow DAG...</div>}
            {pipelines.error && <div className="empty-state">Не удалось получить Pipeline Health через backend adapter.</div>}
            {!pipelines.isLoading && !pipelines.error && visibleRows.length === 0 && (
              <div className="empty-state">Airflow DAG пока не найдены.</div>
            )}
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
                  {insight.text}
                </p>
                <div className="card-sub" style={{ marginTop: 10 }}>
                  {insight.metrics}
                </div>
                <button className="btn btn-ai" style={{ marginTop: 12 }} onClick={() => setScreen(insight.screen)}>
                  {insight.action}
                </button>
              </div>
            </div>
          </Card>
        </div>
      </div>
    </div>
  );
}

type InsightInput = {
  pipelines: Pipeline[];
  sparkJobs: SparkJob[];
  connections: DatabaseConnection[];
  isLoading: boolean;
  hasError: boolean;
};

type Insight = {
  text: string;
  metrics: string;
  action: string;
  screen: "ai-agent" | "airflow" | "spark" | "connections";
};

function sortPipelinesForHealth(pipelines: Pipeline[]) {
  return [...pipelines].sort((left, right) => pipelinePriority(right) - pipelinePriority(left));
}

function pipelinePriority(pipeline: Pipeline) {
  if (isFailedPipeline(pipeline)) return 4;
  if (pipeline.status === "import_error") return 3;
  if (pipeline.status === "running") return 2;
  if (pipeline.status === "paused") return 1;
  return 0;
}

function countFailedPipelines(pipelines: Pipeline[]) {
  return pipelines.filter(isFailedPipeline).length;
}

function countFailedSparkJobs(jobs: SparkJob[]) {
  return jobs.filter((job) => isBadStatus(job.status)).length;
}

function calculateSla(pipelines: Pipeline[]) {
  const finished = pipelines.filter((pipeline) => {
    const lastRun = pipeline.last_run?.toLowerCase() ?? "";
    return lastRun.startsWith("success") || lastRun.startsWith("failed");
  });
  if (!finished.length) return "n/a";

  const success = finished.filter((pipeline) => pipeline.last_run?.toLowerCase().startsWith("success")).length;
  return `${((success / finished.length) * 100).toFixed(1)}%`;
}

function buildInsight(input: InsightInput): Insight {
  const metrics = [
    `Airflow: ${input.pipelines.length} DAG`,
    `Spark: ${input.sparkJobs.length} jobs`,
    `DB: ${input.connections.filter((connection) => connection.status === "online").length}/${input.connections.length} online`,
  ].join(" · ");

  if (input.isLoading) {
    return {
      text: "Собираю актуальное состояние Airflow, Spark jobs и подключений к базам. Insight появится после ответа backend adapter.",
      metrics,
      action: "Open agent",
      screen: "ai-agent",
    };
  }

  if (input.hasError) {
    return {
      text: "Часть live-метрик не загрузилась. Нужно проверить авторизацию frontend и доступность backend endpoints для pipelines, Spark jobs или connections.",
      metrics,
      action: "Open agent",
      screen: "ai-agent",
    };
  }

  const failedPipeline = input.pipelines.find(isFailedPipeline);
  if (failedPipeline) {
    return {
      text: `Главный риск сейчас: DAG ${failedPipeline.dag_id} имеет проблемный последний запуск (${failedPipeline.last_run ?? failedPipeline.status}). Расписание: ${failedPipeline.schedule}, владелец: ${failedPipeline.owner}.`,
      metrics,
      action: "Open Airflow",
      screen: "airflow",
    };
  }

  const offlineConnection = input.connections.find((connection) => connection.status === "offline");
  if (offlineConnection) {
    return {
      text: `Есть проблема с подключением ${offlineConnection.name}: статус offline${offlineConnection.last_error ? `, ошибка: ${offlineConnection.last_error}` : ""}. Это может ломать SQL, DAG и Spark сценарии.`,
      metrics,
      action: "Open connections",
      screen: "connections",
    };
  }

  const failedSpark = input.sparkJobs.find((job) => isBadStatus(job.status));
  if (failedSpark) {
    return {
      text: `Spark job ${failedSpark.name} завершился со статусом ${failedSpark.status}. Проверь driver log и result sample перед повторным запуском.`,
      metrics,
      action: "Open Spark",
      screen: "spark",
    };
  }

  const runningPipeline = input.pipelines.find((pipeline) => pipeline.last_run?.toLowerCase().startsWith("running"));
  if (runningPipeline) {
    return {
      text: `Сейчас выполняется DAG ${runningPipeline.dag_id}. Можно открыть Airflow, посмотреть task instances и логи выполнения.`,
      metrics,
      action: "Open Airflow",
      screen: "airflow",
    };
  }

  return {
    text: "Критичных проблем в последних live-метриках не видно: Airflow отвечает, Spark jobs доступны, подключения к БД проверены. Для следующего шага можно запустить agent query или открыть нужный раздел.",
    metrics,
    action: "Open agent",
    screen: "ai-agent",
  };
}

function isFailedPipeline(pipeline: Pipeline) {
  const status = pipeline.status.toLowerCase();
  const lastRun = pipeline.last_run?.toLowerCase() ?? "";
  return isBadStatus(status) || status === "import_error" || lastRun.startsWith("failed");
}

function isBadStatus(status: string) {
  return ["failed", "error", "offline"].includes(status.toLowerCase());
}
