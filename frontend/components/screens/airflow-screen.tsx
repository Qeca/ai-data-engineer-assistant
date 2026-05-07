"use client";

import { useMutation, useQuery, useQueryClient } from "@tanstack/react-query";
import { Play } from "lucide-react";
import { api } from "@/lib/api";
import { useAppStore } from "@/lib/store";
import type { AirflowRun } from "@/types";
import { Badge, Card } from "@/components/ui";

export function AirflowScreen() {
  const token = useAppStore((state) => state.accessToken);
  const queryClient = useQueryClient();
  const pipelines = useQuery({
    queryKey: ["pipelines", token],
    queryFn: () => api.pipelines(token ?? ""),
    enabled: Boolean(token),
  });

  const trigger = useMutation({
    mutationFn: (dagId: string) => api.triggerDag(token ?? "", dagId),
    onSuccess: () => queryClient.invalidateQueries({ queryKey: ["pipelines"] }),
  });

  const latestRun = trigger.data as AirflowRun | undefined;

  return (
    <div className="content">
      <div style={{ marginBottom: 14 }}>
        <div className="card-title">Airflow DAGs</div>
        <div className="card-sub">Stable REST API /api/v1; local fallback persists runs</div>
      </div>
      <div style={{ display: "flex", flexDirection: "column", gap: 10, marginBottom: 14 }}>
        {(pipelines.data ?? []).map((pipeline) => (
          <div key={pipeline.dag_id} className="card" style={{ padding: 14 }}>
            <div style={{ display: "flex", alignItems: "center", gap: 12 }}>
              <div style={{ flex: 1 }}>
                <div className="mono" style={{ fontWeight: 700 }}>{pipeline.dag_id}</div>
                <div className="card-sub">{pipeline.owner} · {pipeline.next_run}</div>
              </div>
              <Badge status={pipeline.status} />
              <span className="tag">{pipeline.schedule}</span>
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

      <Card title="Generated DAG Preview" sub="Agent can produce and deploy this through a controlled workflow">
        <pre className="code-box">{`from airflow import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from datetime import datetime, timedelta

with DAG(
    dag_id="clickstream_aggregation",
    schedule_interval="*/15 * * * *",
    start_date=datetime(2026, 1, 1),
    catchup=False,
    default_args={"retries": 3, "retry_delay": timedelta(minutes=5)},
) as dag:
    aggregate = SparkSubmitOperator(
        task_id="aggregate_clickstream",
        application="jobs/clickstream_agg.py",
        conf={
            "spark.executor.memory": "6g",
            "spark.dynamicAllocation.enabled": "true",
        },
    )`}</pre>
      </Card>
    </div>
  );
}
