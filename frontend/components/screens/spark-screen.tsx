"use client";

import { useMutation } from "@tanstack/react-query";
import { Play } from "lucide-react";
import { useState } from "react";
import { api } from "@/lib/api";
import { useAppStore } from "@/lib/store";
import type { SparkJob } from "@/types";
import { Badge, Card, StatCard } from "@/components/ui";

export function SparkScreen() {
  const token = useAppStore((state) => state.accessToken);
  const [job, setJob] = useState<SparkJob | null>(null);

  const submit = useMutation({
    mutationFn: () =>
      api.submitSpark(token ?? "", {
        name: "clickstream_aggregation_debug",
        app_resource: "local:///opt/spark/jobs/sample_job.py",
        params: { executor_memory: "6g", partitions: 96 },
      }),
    onSuccess: setJob,
  });

  const refresh = useMutation({
    mutationFn: () => api.getSpark(token ?? "", job?.job_id ?? ""),
    onSuccess: setJob,
  });

  return (
    <div className="content">
      <div className="grid-4" style={{ marginBottom: 16 }}>
        <StatCard label="Active Executors" value="2" note="local standalone workers" tone="var(--blue-300)" />
        <StatCard label="Memory Used" value="8 GB" note="demo allocation" />
        <StatCard label="Tasks / min" value="1,840" note="sample telemetry" tone="var(--emerald-400)" />
        <StatCard label="Shuffle Read" value="840 MB" note="latest sample" tone="var(--amber-400)" />
      </div>

      <Card
        title="Spark Submit"
        sub="Metadata is persisted; compose can attach this to Spark standalone"
        action={
          <button className="btn btn-primary" onClick={() => submit.mutate()} disabled={submit.isPending}>
            <Play size={14} />
            Submit Job
          </button>
        }
      >
        {job ? (
          <div style={{ display: "flex", flexDirection: "column", gap: 12 }}>
            <div style={{ display: "flex", gap: 10, alignItems: "center", flexWrap: "wrap" }}>
              <Badge status={job.status} />
              <span className="mono">{job.job_id}</span>
              <span className="tag">{job.name}</span>
              <button className="btn btn-secondary" onClick={() => refresh.mutate()} disabled={refresh.isPending}>
                Refresh status
              </button>
            </div>
            <div className="table-wrap">
              <table className="data-table">
                <thead>
                  <tr><th>Metric</th><th>Value</th></tr>
                </thead>
                <tbody>
                  {(job.result_sample ?? []).map((row, index) => (
                    <tr key={index}>
                      <td className="mono">{String(row.metric)}</td>
                      <td className="mono">{String(row.value)}</td>
                    </tr>
                  ))}
                </tbody>
              </table>
            </div>
            {job.driver_log && <pre className="code-box">{job.driver_log}</pre>}
          </div>
        ) : (
          <p style={{ margin: 0, color: "var(--text-secondary)" }}>
            Нажмите Submit Job, чтобы создать Spark job через API и получить sample результата.
          </p>
        )}
      </Card>
    </div>
  );
}
