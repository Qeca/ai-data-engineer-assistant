"use client";

import { useMutation, useQuery, useQueryClient } from "@tanstack/react-query";
import { FileText, Play, RefreshCw } from "lucide-react";
import { useEffect, useState } from "react";
import { api } from "@/lib/api";
import { useAppStore } from "@/lib/store";
import type { AirflowRun, AirflowTaskInstance, AirflowTaskLog } from "@/types";
import { Badge, Card } from "@/components/ui";

export function AirflowScreen() {
  const token = useAppStore((state) => state.accessToken);
  const queryClient = useQueryClient();
  const [selectedDagId, setSelectedDagId] = useState<string | null>(null);
  const [selectedRunId, setSelectedRunId] = useState<string | null>(null);
  const [selectedTask, setSelectedTask] = useState<AirflowTaskInstance | null>(null);
  const pipelines = useQuery({
    queryKey: ["pipelines", token],
    queryFn: () => api.pipelines(token ?? ""),
    enabled: Boolean(token),
  });
  const runs = useQuery({
    queryKey: ["airflow-runs", token, selectedDagId],
    queryFn: () => api.dagRuns(token ?? "", selectedDagId ?? ""),
    enabled: Boolean(token && selectedDagId),
  });
  const tasks = useQuery({
    queryKey: ["airflow-tasks", token, selectedDagId, selectedRunId],
    queryFn: () => api.dagTaskInstances(token ?? "", selectedDagId ?? "", selectedRunId ?? ""),
    enabled: Boolean(token && selectedDagId && selectedRunId),
  });
  const taskLog = useQuery({
    queryKey: ["airflow-task-log", token, selectedDagId, selectedRunId, selectedTask?.task_id, selectedTask?.try_number],
    queryFn: () =>
      api.dagTaskLog(
        token ?? "",
        selectedDagId ?? "",
        selectedRunId ?? "",
        selectedTask?.task_id ?? "",
        selectedTask?.try_number || 1,
      ),
    enabled: Boolean(token && selectedDagId && selectedRunId && selectedTask),
  });

  useEffect(() => {
    if (selectedDagId || !pipelines.data?.length) return;
    setSelectedDagId(pipelines.data[0].dag_id);
  }, [pipelines.data, selectedDagId]);

  useEffect(() => {
    setSelectedRunId(null);
    setSelectedTask(null);
  }, [selectedDagId]);

  useEffect(() => {
    if (!runs.data?.length) return;
    setSelectedRunId((current) => current ?? runs.data[0].run_id);
  }, [runs.data]);

  useEffect(() => {
    setSelectedTask(null);
  }, [selectedRunId]);

  const trigger = useMutation({
    mutationFn: (dagId: string) => api.triggerDag(token ?? "", dagId),
    onSuccess: (run) => {
      setSelectedDagId(run.dag_id);
      setSelectedRunId(run.run_id);
      queryClient.invalidateQueries({ queryKey: ["pipelines"] });
      queryClient.invalidateQueries({ queryKey: ["airflow-runs"] });
    },
  });

  const latestRun = trigger.data as AirflowRun | undefined;
  const log = taskLog.data as AirflowTaskLog | undefined;

  function refreshSelectedDag() {
    if (!selectedDagId) return;
    void pipelines.refetch();
    void runs.refetch();
    if (selectedRunId) void tasks.refetch();
    if (selectedTask) void taskLog.refetch();
  }

  return (
    <div className="content">
      <div style={{ marginBottom: 14 }}>
        <div className="card-title">Airflow DAGs</div>
        <div className="card-sub">Runs, task instances and logs through Airflow stable REST API</div>
      </div>
      <div style={{ display: "flex", flexDirection: "column", gap: 10, marginBottom: 14 }}>
        {(pipelines.data ?? []).map((pipeline) => (
          <div
            key={pipeline.dag_id}
            className="card"
            style={{
              padding: 14,
              borderColor: selectedDagId === pipeline.dag_id ? "rgba(45, 126, 248, 0.45)" : undefined,
            }}
          >
            <div style={{ display: "flex", alignItems: "center", gap: 12 }}>
              <div style={{ flex: 1 }}>
                <div className="mono" style={{ fontWeight: 700 }}>{pipeline.dag_id}</div>
                <div className="card-sub">{pipeline.owner} · {pipeline.next_run}</div>
              </div>
              <Badge status={pipeline.status} />
              <span className="tag">{pipeline.schedule}</span>
              <button className="btn btn-secondary" onClick={() => setSelectedDagId(pipeline.dag_id)}>
                <FileText size={14} />
                Runs
              </button>
              <button className="btn btn-primary" onClick={() => trigger.mutate(pipeline.dag_id)} disabled={trigger.isPending}>
                <Play size={14} />
                Run
              </button>
            </div>
          </div>
        ))}
      </div>

      {latestRun && (
        <Card title="Latest DAG Run">
          <div style={{ display: "flex", alignItems: "center", gap: 10, flexWrap: "wrap" }}>
            <Badge status={latestRun.status} />
            <span className="mono">{latestRun.dag_id}</span>
            <span className="tag">{latestRun.run_id}</span>
            {latestRun.external_url && <a className="btn btn-secondary" href={latestRun.external_url}>Airflow UI</a>}
          </div>
        </Card>
      )}

      {selectedDagId && (
        <div className="grid-2">
          <Card
            title={`Runs: ${selectedDagId}`}
            sub="Click a run to inspect task instances"
            action={
              <button
                className="btn btn-secondary icon-btn"
                type="button"
                onClick={refreshSelectedDag}
                disabled={runs.isFetching}
                title="Refresh runs"
                aria-label="Refresh runs"
              >
                <RefreshCw size={14} className={runs.isFetching ? "spin" : undefined} />
              </button>
            }
          >
            <div className="table-wrap">
              <table className="data-table">
                <thead>
                  <tr>
                    <th>Run</th>
                    <th>Status</th>
                    <th>Created</th>
                  </tr>
                </thead>
                <tbody>
                  {(runs.data ?? []).map((run) => (
                    <tr key={run.run_id} onClick={() => setSelectedRunId(run.run_id)} style={{ cursor: "pointer" }}>
                      <td className="mono">{run.run_id}</td>
                      <td><Badge status={run.status} /></td>
                      <td>{run.created_at ?? "remote"}</td>
                    </tr>
                  ))}
                </tbody>
              </table>
            </div>
          </Card>

          <Card
            title={selectedRunId ? `Tasks: ${selectedRunId}` : "Tasks"}
            sub="Click a task to load its Airflow log"
          >
            <div className="table-wrap">
              <table className="data-table">
                <thead>
                  <tr>
                    <th>Task</th>
                    <th>State</th>
                    <th>Try</th>
                    <th>Duration</th>
                  </tr>
                </thead>
                <tbody>
                  {(tasks.data ?? []).map((task) => (
                    <tr key={task.task_id} onClick={() => setSelectedTask(task)} style={{ cursor: "pointer" }}>
                      <td>
                        <div className="mono">{task.task_id}</div>
                        <div className="card-sub">{task.operator}</div>
                      </td>
                      <td><Badge status={task.state} /></td>
                      <td>{task.try_number}</td>
                      <td>{task.duration ?? "-"}</td>
                    </tr>
                  ))}
                </tbody>
              </table>
            </div>
          </Card>
        </div>
      )}

      {selectedTask && (
        <Card title={`Log: ${selectedTask.task_id}`} sub={`${selectedDagId} / ${selectedRunId}`}>
          <pre className="code-box">{log?.content ?? "Loading log..."}</pre>
        </Card>
      )}
    </div>
  );
}
