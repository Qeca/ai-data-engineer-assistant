"use client";

import { useMutation, useQuery, useQueryClient } from "@tanstack/react-query";
import { FormEvent, useState } from "react";
import { api } from "@/lib/api";
import { useAppStore } from "@/lib/store";
import { DatabaseConnectionsPanel } from "@/components/database-connections-panel";
import { Badge, Card } from "@/components/ui";

export function SettingsScreen() {
  const token = useAppStore((state) => state.accessToken);
  const user = useAppStore((state) => state.user);
  const queryClient = useQueryClient();
  const [email, setEmail] = useState("analyst@local.dev");
  const [fullName, setFullName] = useState("Demo Analyst");
  const [role, setRole] = useState("analyst");

  const users = useQuery({
    queryKey: ["users", token],
    queryFn: () => api.users(token ?? ""),
    enabled: Boolean(token && user?.role === "admin"),
  });

  const createUser = useMutation({
    mutationFn: () =>
      api.createUser(token ?? "", {
        email,
        full_name: fullName,
        role,
        password: "demo",
      }),
    onSuccess: () => queryClient.invalidateQueries({ queryKey: ["users"] }),
  });

  function submit(event: FormEvent) {
    event.preventDefault();
    createUser.mutate();
  }

  return (
    <div className="content">
      <div className="grid-2">
        <Card title="Access Management" sub="JWT roles: admin, engineer, analyst">
          {user?.role !== "admin" ? (
            <p style={{ margin: 0, color: "var(--text-secondary)" }}>Only admins can manage users.</p>
          ) : (
            <>
              <form onSubmit={submit} style={{ display: "grid", gridTemplateColumns: "1fr 1fr 130px auto", gap: 8, marginBottom: 16 }}>
                <input className="input" value={email} onChange={(event) => setEmail(event.target.value)} />
                <input className="input" value={fullName} onChange={(event) => setFullName(event.target.value)} />
                <select className="select" value={role} onChange={(event) => setRole(event.target.value)}>
                  <option value="analyst">analyst</option>
                  <option value="engineer">engineer</option>
                  <option value="admin">admin</option>
                </select>
                <button className="btn btn-primary" disabled={createUser.isPending}>Invite</button>
              </form>
              {createUser.error && <div className="error-text">{createUser.error.message}</div>}
              <div className="table-wrap">
                <table className="data-table">
                  <thead>
                    <tr><th>User</th><th>Role</th><th>Status</th></tr>
                  </thead>
                  <tbody>
                    {(users.data ?? []).map((row) => (
                      <tr key={row.id}>
                        <td>
                          <div>{row.full_name}</div>
                          <div className="card-sub">{row.email}</div>
                        </td>
                        <td><span className="tag">{row.role}</span></td>
                        <td><Badge status={row.status} /></td>
                      </tr>
                    ))}
                  </tbody>
                </table>
              </div>
            </>
          )}
        </Card>

        <Card title="Model & Tracing" sub="External keys are read only from backend env">
          <div style={{ display: "flex", flexDirection: "column", gap: 10 }}>
            <div style={{ display: "flex", justifyContent: "space-between", gap: 10 }}>
              <span>OpenAI model</span>
              <span className="tag">gpt-5.5</span>
            </div>
            <div style={{ display: "flex", justifyContent: "space-between", gap: 10 }}>
              <span>OpenRouter model</span>
              <span className="tag">openai/gpt-5.5</span>
            </div>
            <div style={{ display: "flex", justifyContent: "space-between", gap: 10 }}>
              <span>Langfuse traces</span>
              <Badge status="ai">enabled when env exists</Badge>
            </div>
            <div style={{ display: "flex", justifyContent: "space-between", gap: 10 }}>
              <span>RAG / ChromaDB</span>
              <Badge status="neutral">not in first release</Badge>
            </div>
          </div>
        </Card>
      </div>
      <div style={{ marginTop: 14 }}>
        <DatabaseConnectionsPanel />
      </div>
    </div>
  );
}
