"use client";

import { useMutation, useQuery, useQueryClient } from "@tanstack/react-query";
import { Play, RefreshCw } from "lucide-react";
import { useEffect, useMemo, useState } from "react";
import { api } from "@/lib/api";
import { useAppStore } from "@/lib/store";
import type { SparkJob } from "@/types";
import { Badge, Card, StatCard } from "@/components/ui";

export function SparkScreen() {
  const token = useAppStore((state) => state.accessToken);
  const queryClient = useQueryClient();
  const [selectedJobId, setSelectedJobId] = useState<string | null>(null);

  const jobs = useQuery({
    queryKey: ["spark-jobs", token],
    queryFn: () => api.sparkJobs(token ?? ""),
    enabled: Boolean(token),
    refetchInterval: 10000,
  });

  useEffect(() => {
    if (selectedJobId || !jobs.data?.length) return;
    setSelectedJobId(jobs.data[0].job_id);
  }, [jobs.data, selectedJobId]);

  const selectedFromList = jobs.data?.find((job) => job.job_id === selectedJobId) ?? null;
  const selectedJob = useQuery({
    queryKey: ["spark-job", token, selectedJobId],
    queryFn: () => api.getSpark(token ?? "", selectedJobId ?? ""),
    enabled: Boolean(token && selectedJobId),
  });

  const submit = useMutation({
    mutationFn: () =>
      api.submitSpark(token ?? "", {
        name: "clickstream_aggregation_debug",
        app_resource: "local:///opt/spark/jobs/sample_job.py",
        params: { executor_memory: "6g", partitions: 96 },
      }),
    onSuccess: (job) => {
      setSelectedJobId(job.job_id);
      queryClient.invalidateQueries({ queryKey: ["spark-jobs"] });
    },
  });

  const refreshSelected = () => {
    queryClient.invalidateQueries({ queryKey: ["spark-jobs"] });
    queryClient.invalidateQueries({ queryKey: ["spark-job", token, selectedJobId] });
  };

  const allJobs = jobs.data ?? [];
  const activeCount = allJobs.filter((job) => ["running", "queued", "submitted"].includes(job.status)).length;
  const failedCount = allJobs.filter((job) => ["failed", "error"].includes(job.status)).length;
  const successCount = allJobs.filter((job) => job.status === "success").length;
  const detail = selectedJob.data ?? selectedFromList;
  const resultRows = detail?.result_sample ?? [];

  const params = useMemo(() => Object.entries(detail?.params ?? {}), [detail?.params]);

  return (
    <div className="content">
      <div className="grid-4" style={{ marginBottom: 16 }}>
        <StatCard label="Jobs" value={allJobs.length} note="stored Spark job metadata" tone="var(--blue-300)" />
        <StatCard label="Active" value={activeCount} note="queued / running / submitted" tone="var(--emerald-400)" />
        <StatCard label="Success" value={successCount} note="completed jobs" tone="var(--emerald-400)" />
        <StatCard label="Failed" value={failedCount} note="failed or errored jobs" tone="var(--color-error)" />
      </div>

      <div className="grid-2">
        <Card
          title="Spark Jobs"
          sub="Job history, launch payload and current status"
          action={
            <div style={{ display: "flex", gap: 8 }}>
              <button className="btn btn-secondary" onClick={refreshSelected} disabled={jobs.isFetching}>
                <RefreshCw size={14} />
                Refresh
              </button>
              <button className="btn btn-primary" onClick={() => submit.mutate()} disabled={submit.isPending}>
                <Play size={14} />
                Submit demo
              </button>
            </div>
          }
        >
          {allJobs.length > 0 ? (
            <div className="table-wrap">
              <table className="data-table">
                <thead>
                  <tr>
                    <th>Job</th>
                    <th>Status</th>
                    <th>Created</th>
                  </tr>
                </thead>
                <tbody>
                  {allJobs.map((job) => (
                    <tr
                      key={job.job_id}
                      onClick={() => setSelectedJobId(job.job_id)}
                      style={{
                        cursor: "pointer",
                        background: selectedJobId === job.job_id ? "rgba(45, 126, 248, 0.12)" : undefined,
                      }}
                    >
                      <td>
                        <div className="mono" style={{ fontWeight: 700 }}>{job.name}</div>
                        <div className="card-sub">{job.job_id}</div>
                      </td>
                      <td><Badge status={job.status} /></td>
                      <td>{formatDate(job.created_at)}</td>
                    </tr>
                  ))}
                </tbody>
              </table>
            </div>
          ) : (
            <div className="empty-state">
              Spark jobs пока не запускались. Нажмите Submit demo или попросите агента создать Spark job.
            </div>
          )}
        </Card>

        <Card
          title={detail ? detail.name : "Job Details"}
          sub={detail ? detail.job_id : "Select a Spark job to inspect"}
          action={
            detail && (
              <button className="btn btn-secondary" onClick={refreshSelected} disabled={selectedJob.isFetching}>
                <RefreshCw size={14} />
                Refresh status
              </button>
            )
          }
        >
          {detail ? (
            <div className="spark-detail">
              <div className="spark-detail-header">
                <Badge status={detail.status} />
                <span className="tag">{formatDate(detail.created_at)}</span>
                {detail.updated_at && <span className="tag">updated {formatDate(detail.updated_at)}</span>}
              </div>

              <section className="detail-section">
                <div className="stat-label">Что делает job</div>
                <div className="detail-text">{describeSparkJob(detail)}</div>
              </section>

              <section className="detail-section">
                <div className="stat-label">Application Resource</div>
                <div className="code-inline">{detail.app_resource}</div>
              </section>

              <section className="detail-section">
                <div className="stat-label">Run Parameters</div>
                {params.length > 0 ? (
                  <div className="kv-grid">
                    {params.map(([key, value]) => (
                      <div className="kv-row" key={key}>
                        <span>{key}</span>
                        <code>{formatValue(value)}</code>
                      </div>
                    ))}
                  </div>
                ) : (
                  <div className="card-sub">Параметры не передавались.</div>
                )}
              </section>

              <section className="detail-section">
                <div className="stat-label">Result Sample</div>
                {resultRows.length > 0 ? (
                  <div className="table-wrap">
                    <table className="data-table">
                      <thead>
                        <tr>
                          {Object.keys(resultRows[0]).map((key) => <th key={key}>{key}</th>)}
                        </tr>
                      </thead>
                      <tbody>
                        {resultRows.map((row, index) => (
                          <tr key={index}>
                            {Object.keys(resultRows[0]).map((key) => (
                              <td className="mono" key={key}>{formatValue(row[key])}</td>
                            ))}
                          </tr>
                        ))}
                      </tbody>
                    </table>
                  </div>
                ) : (
                  <div className="card-sub">Result sample пока пустой.</div>
                )}
              </section>

              <section className="detail-section">
                <div className="stat-label">Driver Logs</div>
                <pre className="code-box">{detail.driver_log || "Логи пока не записаны."}</pre>
              </section>
            </div>
          ) : (
            <div className="empty-state">Выберите Spark job из списка.</div>
          )}
        </Card>
      </div>
    </div>
  );
}

function describeSparkJob(job: SparkJob): string {
  const partitions = job.params.partitions ? `, partitions=${formatValue(job.params.partitions)}` : "";
  const memory = job.params.executor_memory ? `, executor_memory=${formatValue(job.params.executor_memory)}` : "";
  if (job.name.includes("clickstream")) {
    return `Debug aggregation for clickstream data${partitions}${memory}.`;
  }
  return `Запускает Spark application ${job.app_resource}${partitions}${memory}.`;
}

function formatDate(value?: string | null): string {
  if (!value) return "-";
  return new Date(value).toLocaleString("ru-RU");
}

function formatValue(value: unknown): string {
  if (value === null || value === undefined) return "-";
  if (typeof value === "object") return JSON.stringify(value);
  return String(value);
}
