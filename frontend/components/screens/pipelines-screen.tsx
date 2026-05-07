"use client";

import { useQuery } from "@tanstack/react-query";
import { api } from "@/lib/api";
import { useAppStore } from "@/lib/store";
import { Badge, Card } from "@/components/ui";

export function PipelinesScreen() {
  const token = useAppStore((state) => state.accessToken);
  const setScreen = useAppStore((state) => state.setScreen);
  const pipelines = useQuery({
    queryKey: ["pipelines", token],
    queryFn: () => api.pipelines(token ?? ""),
    enabled: Boolean(token),
  });

  return (
    <div className="content">
      <Card title="Pipeline Registry" sub="Airflow-backed list with local fallback">
        <div className="table-wrap">
          <table className="data-table">
            <thead>
              <tr>
                <th>DAG</th>
                <th>Status</th>
                <th>Schedule</th>
                <th>Owner</th>
                <th>Last run</th>
                <th />
              </tr>
            </thead>
            <tbody>
              {(pipelines.data ?? []).map((pipeline) => (
                <tr key={pipeline.dag_id}>
                  <td>
                    <div className="mono">{pipeline.dag_id}</div>
                    <div className="card-sub">{pipeline.name}</div>
                  </td>
                  <td><Badge status={pipeline.status} /></td>
                  <td><span className="tag">{pipeline.schedule}</span></td>
                  <td>{pipeline.owner}</td>
                  <td>{pipeline.last_run ?? "none"}</td>
                  <td>
                    <button className="btn btn-secondary" onClick={() => setScreen("airflow")}>Open</button>
                  </td>
                </tr>
              ))}
            </tbody>
          </table>
        </div>
      </Card>
    </div>
  );
}
