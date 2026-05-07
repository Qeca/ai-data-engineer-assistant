"use client";

import Editor from "@monaco-editor/react";
import { useMutation, useQuery } from "@tanstack/react-query";
import { Play } from "lucide-react";
import { useState } from "react";
import { api } from "@/lib/api";
import { useAppStore } from "@/lib/store";
import type { SqlResult } from "@/types";
import { Badge } from "@/components/ui";

const defaultSql = `SELECT
  strftime('%Y-%m-%d %H:00:00', created_at) AS hour,
  COUNT(*) AS order_count,
  ROUND(AVG(total_amount), 2) AS avg_amount
FROM orders
WHERE created_at >= datetime('now', '-30 days')
GROUP BY 1
ORDER BY order_count DESC
LIMIT 10`;

export function SqlScreen() {
  const token = useAppStore((state) => state.accessToken);
  const [query, setQuery] = useState(defaultSql);
  const [result, setResult] = useState<SqlResult | null>(null);

  const catalog = useQuery({
    queryKey: ["catalog", token],
    queryFn: () => api.catalogTables(token ?? ""),
    enabled: Boolean(token),
  });

  const execute = useMutation({
    mutationFn: () => api.executeSql(token ?? "", query, 100),
    onSuccess: setResult,
  });

  return (
    <div className="split">
      <aside className="left-panel">
        <div style={{ padding: 12, borderBottom: "1px solid var(--border-subtle)" }}>
          <input className="input" placeholder="Search tables..." />
        </div>
        <div style={{ padding: 12 }}>
          {(catalog.data ?? []).map((table) => (
            <div key={table.name} style={{ marginBottom: 14 }}>
              <div style={{ display: "flex", alignItems: "center", gap: 8 }}>
                <Badge status="info">{table.name}</Badge>
              </div>
              <div style={{ marginTop: 6, paddingLeft: 8 }}>
                {table.columns.slice(0, 6).map((column) => (
                  <div key={column.name} style={{ display: "flex", gap: 8, color: "var(--text-secondary)" }}>
                    <span className="mono">{column.name}</span>
                    <span className="card-sub">{column.type}</span>
                  </div>
                ))}
              </div>
            </div>
          ))}
        </div>
      </aside>

      <section className="work-area">
        <div style={{ display: "flex", alignItems: "center", gap: 10, padding: "8px 12px", background: "var(--bg-surface)", borderBottom: "1px solid var(--border-subtle)" }}>
          <span className="tag">anomaly_check.sql</span>
          <div className="spacer" />
          <button className="btn btn-primary" onClick={() => execute.mutate()} disabled={execute.isPending}>
            <Play size={14} />
            {execute.isPending ? "Running" : "Run"}
          </button>
        </div>

        <div style={{ height: 260, borderBottom: "1px solid var(--border-subtle)" }}>
          <Editor
            height="260px"
            defaultLanguage="sql"
            value={query}
            onChange={(value) => setQuery(value ?? "")}
            theme="vs-dark"
            options={{
              minimap: { enabled: false },
              fontFamily: "JetBrains Mono",
              fontSize: 12,
              lineHeight: 20,
              padding: { top: 12, bottom: 12 },
              scrollBeyondLastLine: false,
              automaticLayout: true,
            }}
          />
        </div>

        <div style={{ padding: "8px 12px", background: "rgba(139, 92, 246, 0.08)", borderBottom: "1px solid rgba(139, 92, 246, 0.18)" }}>
          <span style={{ color: "var(--text-secondary)" }}>
            AI hint: для production Postgres агент автоматически заменит SQLite-функции на `date_trunc` и `interval`.
          </span>
        </div>

        <div style={{ flex: 1, overflow: "hidden", display: "flex", flexDirection: "column" }}>
          <div style={{ display: "flex", gap: 8, alignItems: "center", padding: "8px 12px", borderBottom: "1px solid var(--border-subtle)", background: "var(--bg-surface)" }}>
            {result ? <Badge status="success">{result.row_count} rows</Badge> : <Badge status="neutral">not run</Badge>}
            {result && <span className="card-sub mono">{result.latency_ms}ms</span>}
            {execute.error && <span className="error-text">{execute.error.message}</span>}
          </div>

          <div className="table-wrap" style={{ flex: 1 }}>
            <table className="data-table">
              <thead>
                <tr>
                  {(result?.columns ?? ["hour", "order_count", "avg_amount"]).map((column) => (
                    <th key={column}>{column}</th>
                  ))}
                </tr>
              </thead>
              <tbody>
                {(result?.rows ?? []).map((row, index) => (
                  <tr key={index}>
                    {result?.columns.map((column) => <td key={column} className="mono">{String(row[column] ?? "")}</td>)}
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
        </div>
      </section>
    </div>
  );
}
