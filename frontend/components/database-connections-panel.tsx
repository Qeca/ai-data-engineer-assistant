"use client";

import { useMutation, useQuery, useQueryClient } from "@tanstack/react-query";
import { Database, PlugZap } from "lucide-react";
import { FormEvent, useState } from "react";
import { api } from "@/lib/api";
import { useAppStore } from "@/lib/store";
import type { DatabaseConnectionPayload, DatabaseEngine } from "@/types";
import { Badge, Card } from "@/components/ui";

const engines: DatabaseEngine[] = ["postgresql", "mysql", "clickhouse", "mongodb", "redis"];

export function DatabaseConnectionsPanel() {
  const token = useAppStore((state) => state.accessToken);
  const user = useAppStore((state) => state.user);
  const queryClient = useQueryClient();
  const canManage = user?.role === "admin" || user?.role === "engineer";
  const [form, setForm] = useState<DatabaseConnectionPayload>({
    name: "custom-postgres",
    engine: "postgresql",
    host: "demo-postgres",
    port: 5432,
    database: "analytics",
    username: "demo",
    password: "",
    visibility: "private",
    options: {},
  });

  const connections = useQuery({
    queryKey: ["database-connections", token],
    queryFn: () => api.connections(token ?? ""),
    enabled: Boolean(token),
  });

  const createConnection = useMutation({
    mutationFn: () => api.createConnection(token ?? "", form),
    onSuccess: () => queryClient.invalidateQueries({ queryKey: ["database-connections"] }),
  });

  const testConnection = useMutation({
    mutationFn: (id: string) => api.testConnection(token ?? "", id),
    onSuccess: () => queryClient.invalidateQueries({ queryKey: ["database-connections"] }),
  });

  function submit(event: FormEvent) {
    event.preventDefault();
    if (!canManage) return;
    createConnection.mutate();
  }

  return (
    <Card
      title="Database Connections"
      sub="Shared demo DBs and user-managed connections visible to the agent"
      action={<Badge status={connections.isFetching ? "running" : "success"}>{connections.data?.length ?? 0} visible</Badge>}
    >
      {canManage && (
        <form className="connection-form" onSubmit={submit}>
          <input className="input" value={form.name} onChange={(event) => setForm({ ...form, name: event.target.value })} />
          <select
            className="select"
            value={form.engine}
            onChange={(event) => setForm({ ...form, engine: event.target.value as DatabaseEngine })}
          >
            {engines.map((engine) => (
              <option key={engine} value={engine}>{engine}</option>
            ))}
          </select>
          <input className="input" value={form.host} onChange={(event) => setForm({ ...form, host: event.target.value })} />
          <input
            className="input"
            type="number"
            value={form.port}
            onChange={(event) => setForm({ ...form, port: Number(event.target.value) })}
          />
          <input
            className="input"
            value={form.database ?? ""}
            onChange={(event) => setForm({ ...form, database: event.target.value })}
          />
          <input
            className="input"
            value={form.username ?? ""}
            onChange={(event) => setForm({ ...form, username: event.target.value })}
          />
          <input
            className="input"
            type="password"
            value={form.password ?? ""}
            onChange={(event) => setForm({ ...form, password: event.target.value })}
          />
          <select
            className="select"
            value={form.visibility}
            onChange={(event) => setForm({ ...form, visibility: event.target.value as "private" | "shared" })}
          >
            <option value="private">private</option>
            <option value="shared">shared</option>
          </select>
          <button className="btn btn-primary" disabled={createConnection.isPending}>
            <PlugZap size={14} />
            Save
          </button>
        </form>
      )}

      {createConnection.error && <div className="error-text">{createConnection.error.message}</div>}
      {connections.error && <div className="error-text">{connections.error.message}</div>}

      <div className="connection-list">
        {(connections.data ?? []).map((connection) => {
          const publicHost = connection.options_json?.public_host;
          const publicPort = connection.options_json?.public_port;
          return (
            <div className="connection-row" key={connection.id}>
              <Database size={17} />
              <div className="connection-main">
                <div className="connection-title">
                  <span>{connection.name}</span>
                  <Badge status={connection.status}>{connection.status}</Badge>
                  <span className="tag">{connection.engine}</span>
                  <span className="tag">{connection.visibility}</span>
                </div>
                <div className="connection-meta">
                  {connection.host}:{connection.port}
                  {connection.database ? ` / ${connection.database}` : ""}
                  {connection.username ? ` / ${connection.username}` : ""}
                  {publicHost && publicPort ? ` / host ${String(publicHost)}:${String(publicPort)}` : ""}
                </div>
                {connection.last_error && <div className="error-text">{connection.last_error}</div>}
              </div>
              <button
                className="btn btn-secondary"
                disabled={testConnection.isPending}
                onClick={() => testConnection.mutate(connection.id)}
              >
                Test
              </button>
            </div>
          );
        })}
      </div>
    </Card>
  );
}
