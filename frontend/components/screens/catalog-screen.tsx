"use client";

import { useQuery } from "@tanstack/react-query";
import { api } from "@/lib/api";
import { useAppStore } from "@/lib/store";
import { Badge } from "@/components/ui";

export function CatalogScreen() {
  const token = useAppStore((state) => state.accessToken);
  const catalog = useQuery({
    queryKey: ["catalog", token],
    queryFn: () => api.catalogTables(token ?? ""),
    enabled: Boolean(token),
  });

  return (
    <div className="content">
      <div style={{ marginBottom: 14 }}>
        <div className="card-title">Data Catalog</div>
        <div className="card-sub">Read from database metadata, not RAG</div>
      </div>
      <div className="grid-3">
        {(catalog.data ?? []).map((table) => (
          <section className="card" key={table.name}>
            <div className="card-header">
              <Badge status="info">{table.name}</Badge>
              <span className="card-sub">{table.schema_name}</span>
            </div>
            <div className="card-body" style={{ paddingTop: 10 }}>
              {table.columns.map((column) => (
                <div key={column.name} style={{ display: "flex", gap: 10, padding: "4px 0" }}>
                  <span className="mono" style={{ flex: 1 }}>{column.name}</span>
                  <span className="tag">{column.type}</span>
                </div>
              ))}
            </div>
          </section>
        ))}
      </div>
    </div>
  );
}
