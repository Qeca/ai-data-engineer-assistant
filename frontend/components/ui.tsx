import type { ReactNode } from "react";

export function Badge({ status, children }: { status: string; children?: ReactNode }) {
  const tone =
    status === "success" || status === "active"
      ? "badge-success"
      : status === "running" || status === "queued"
        ? "badge-running"
        : status === "failed" || status === "error" || status === "disabled"
          ? "badge-error"
          : status === "delayed" || status === "warning" || status === "invited"
            ? "badge-warning"
            : status === "ai"
              ? "badge-ai"
              : "badge-neutral";

  return <span className={`badge ${tone}`}>{children ?? status}</span>;
}

export function Card({
  title,
  sub,
  action,
  children,
}: {
  title?: string;
  sub?: string;
  action?: ReactNode;
  children: ReactNode;
}) {
  return (
    <section className="card">
      {(title || action) && (
        <div className="card-header">
          <div>
            {title && <div className="card-title">{title}</div>}
            {sub && <div className="card-sub">{sub}</div>}
          </div>
          <div className="spacer" />
          {action}
        </div>
      )}
      <div className="card-body">{children}</div>
    </section>
  );
}

export function StatCard({
  label,
  value,
  note,
  tone,
}: {
  label: string;
  value: ReactNode;
  note: string;
  tone?: string;
}) {
  return (
    <div className="stat-card">
      <div className="stat-label">{label}</div>
      <div className="stat-value" style={tone ? { color: tone } : undefined}>
        {value}
      </div>
      <div className="stat-note">{note}</div>
    </div>
  );
}
