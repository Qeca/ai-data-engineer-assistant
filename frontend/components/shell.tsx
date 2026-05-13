"use client";

import { useQuery, useQueryClient, type QueryClient } from "@tanstack/react-query";
import {
  Bot,
  Boxes,
  ChevronRight,
  Database,
  GitBranch,
  LayoutDashboard,
  LogOut,
  Moon,
  Network,
  Settings,
  Sparkles,
  Sun,
  Table2,
  UserCircle,
} from "lucide-react";
import { PointerEvent, useEffect, useMemo, useRef, useState, type MutableRefObject } from "react";
import { api } from "@/lib/api";
import { useAppStore, type Screen } from "@/lib/store";
import type { DatabaseConnection, Pipeline, SparkJob } from "@/types";

type NavStatusKey = "pipelines" | "airflow" | "spark" | "connections" | "catalog";

type NavStatusCounts = {
  working: number;
  broken: number;
};

const sections: { label: string; items: { screen: Screen; label: string; icon: React.ElementType; badge?: string; statusKey?: NavStatusKey }[] }[] =
  [
    {
      label: "Workspace",
      items: [
        { screen: "dashboard", label: "Dashboard", icon: LayoutDashboard },
        { screen: "ai-agent", label: "AI Agent", icon: Bot, badge: "NEW" },
        { screen: "sql", label: "SQL Workspace", icon: Table2 },
      ],
    },
    {
      label: "Pipelines",
      items: [
        { screen: "pipelines", label: "Pipelines", icon: GitBranch, statusKey: "pipelines" },
        { screen: "airflow", label: "Airflow DAGs", icon: Network, statusKey: "airflow" },
        { screen: "spark", label: "Spark Jobs", icon: Sparkles, statusKey: "spark" },
      ],
    },
    {
      label: "Infrastructure",
      items: [
        { screen: "connections", label: "Connections", icon: Boxes, statusKey: "connections" },
        { screen: "catalog", label: "Data Catalog", icon: Database, statusKey: "catalog" },
      ],
    },
  ];

export function Shell({ children }: { children: React.ReactNode }) {
  const token = useAppStore((state) => state.accessToken);
  const screen = useAppStore((state) => state.screen);
  const setScreen = useAppStore((state) => state.setScreen);
  const theme = useAppStore((state) => state.theme);
  const toggleTheme = useAppStore((state) => state.toggleTheme);
  const logout = useAppStore((state) => state.logout);
  const user = useAppStore((state) => state.user);
  const queryClient = useQueryClient();
  const connectionsRef = useRef<DatabaseConnection[]>([]);
  const checkingConnections = useRef(false);
  const lastHealthCheckAt = useRef(0);
  const [sidebarWidth, setSidebarWidth] = useState(250);

  const pipelines = useQuery({
    queryKey: ["pipelines", token],
    queryFn: () => api.pipelines(token ?? ""),
    enabled: Boolean(token),
    refetchInterval: 30_000,
  });

  const sparkJobs = useQuery({
    queryKey: ["spark-jobs", token],
    queryFn: () => api.sparkJobs(token ?? ""),
    enabled: Boolean(token),
    refetchInterval: 30_000,
  });

  const connections = useQuery({
    queryKey: ["database-connections", token],
    queryFn: () => api.connections(token ?? ""),
    enabled: Boolean(token),
    refetchInterval: 30_000,
  });

  const catalog = useQuery({
    queryKey: ["catalog", token],
    queryFn: () => api.catalogTables(token ?? ""),
    enabled: Boolean(token),
    refetchInterval: 60_000,
  });

  const navStatuses = useMemo(
    () => ({
      pipelines: pipelines.error ? { working: 0, broken: 1 } : countStatuses(pipelines.data ?? []),
      airflow: pipelines.error ? { working: 0, broken: 1 } : countStatuses(pipelines.data ?? []),
      spark: sparkJobs.error ? { working: 0, broken: 1 } : countStatuses(sparkJobs.data ?? []),
      connections: connections.error ? { working: 0, broken: 1 } : countStatuses(connections.data ?? []),
      catalog: catalog.error ? { working: 0, broken: 1 } : { working: catalog.data?.length ?? 0, broken: 0 },
    }),
    [catalog.data, catalog.error, connections.data, connections.error, pipelines.data, pipelines.error, sparkJobs.data, sparkJobs.error],
  );

  useEffect(() => {
    const saved = window.localStorage.getItem("main-sidebar-width");
    if (saved) setSidebarWidth(clamp(Number(saved), 210, 420));
  }, []);

  useEffect(() => {
    connectionsRef.current = connections.data ?? [];
  }, [connections.data]);

  useEffect(() => {
    if (!token || !connections.data?.length) return;
    const now = Date.now();
    if (now - lastHealthCheckAt.current < 55_000) return;
    lastHealthCheckAt.current = now;
    void testVisibleConnections(token, connections.data, queryClient, checkingConnections);
  }, [connections.data, queryClient, token]);

  useEffect(() => {
    if (!token) return;
    const interval = window.setInterval(() => {
      lastHealthCheckAt.current = Date.now();
      void testVisibleConnections(token, connectionsRef.current, queryClient, checkingConnections);
    }, 60_000);
    return () => window.clearInterval(interval);
  }, [queryClient, token]);

  function resizeSidebar(event: PointerEvent<HTMLDivElement>) {
    const startX = event.clientX;
    const startWidth = sidebarWidth;
    let nextWidth = startWidth;
    event.currentTarget.setPointerCapture(event.pointerId);

    function move(moveEvent: globalThis.PointerEvent) {
      nextWidth = clamp(startWidth + moveEvent.clientX - startX, 210, 420);
      setSidebarWidth(nextWidth);
    }

    function up() {
      window.localStorage.setItem("main-sidebar-width", String(nextWidth));
      window.removeEventListener("pointermove", move);
      window.removeEventListener("pointerup", up);
    }

    window.addEventListener("pointermove", move);
    window.addEventListener("pointerup", up);
  }

  return (
    <div className={`app-shell ${theme === "light" ? "theme-light" : ""}`}>
      <aside className="sidebar resizable-panel" style={{ width: sidebarWidth }}>
        <div className="sidebar-logo">
          <div className="logo-mark">DE</div>
          <div>
            <div className="logo-title">DataFlow AI</div>
            <div className="logo-sub">v0.1 · local</div>
          </div>
        </div>
        <nav className="nav">
          {sections.map((section) => (
            <div key={section.label}>
              <div className="nav-section">{section.label}</div>
              {section.items.map((item) => {
                const Icon = item.icon;
                return (
                  <button
                    key={item.screen}
                    className={`nav-item ${screen === item.screen ? "active" : ""}`}
                    onClick={() => setScreen(item.screen)}
                  >
                    <Icon />
                    <span className="nav-label">{item.label}</span>
                    <NavStatus badge={item.badge} counts={item.statusKey ? navStatuses[item.statusKey] : undefined} />
                  </button>
                );
              })}
            </div>
          ))}
        </nav>
        <div style={{ borderTop: "1px solid var(--border-subtle)", padding: "8px 0" }}>
          <button className={`nav-item ${screen === "settings" ? "active" : ""}`} onClick={() => setScreen("settings")}>
            <Settings />
            Settings
          </button>
          <button className={`nav-item ${screen === "profile" ? "active" : ""}`} onClick={() => setScreen("profile")}>
            <UserCircle />
            {user?.full_name ?? "Profile"}
          </button>
        </div>
        <div className="resize-handle" onPointerDown={resizeSidebar} aria-label="Resize navigation" role="separator" />
      </aside>
      <main className="main">
        <div className="topbar">
          <div className="breadcrumb">
            Workspace <ChevronRight size={13} style={{ verticalAlign: "-2px" }} />{" "}
            <strong>{screenLabel(screen)}</strong>
          </div>
          <div className="spacer" />
          <span className="badge badge-success">local</span>
          <button className="btn btn-secondary icon-btn" onClick={toggleTheme} title="Toggle theme">
            {theme === "light" ? <Moon size={15} /> : <Sun size={15} />}
          </button>
          <button className="btn btn-ghost icon-btn" onClick={logout} title="Logout">
            <LogOut size={15} />
          </button>
        </div>
        {children}
      </main>
    </div>
  );
}

function NavStatus({ badge, counts }: { badge?: string; counts?: NavStatusCounts }) {
  if (!badge && !counts) return null;

  return (
    <span className="nav-status">
      {badge && <span className="tag">{badge}</span>}
      {counts && (
        <>
          <span className="nav-count nav-count-good" title="Рабочие">
            {counts.working}
          </span>
          <span className="nav-count nav-count-bad" title="Нерабочие">
            {counts.broken}
          </span>
        </>
      )}
    </span>
  );
}

async function testVisibleConnections(
  token: string,
  rows: DatabaseConnection[],
  queryClient: QueryClient,
  checkingConnections: MutableRefObject<boolean>,
) {
  if (checkingConnections.current || rows.length === 0) return;
  checkingConnections.current = true;
  await Promise.allSettled(rows.map((connection) => api.testConnection(token, connection.id)));
  checkingConnections.current = false;
  await queryClient.invalidateQueries({ queryKey: ["database-connections", token] });
}

function countStatuses(rows: Array<Pipeline | SparkJob | DatabaseConnection>): NavStatusCounts {
  return rows.reduce(
    (counts, row) => {
      if (isWorkingStatus(row.status)) {
        counts.working += 1;
      } else {
        counts.broken += 1;
      }
      return counts;
    },
    { working: 0, broken: 0 },
  );
}

function isWorkingStatus(status: string) {
  return ["active", "online", "queued", "running", "submitted", "success"].includes(status.toLowerCase());
}

function clamp(value: number, min: number, max: number) {
  return Math.min(Math.max(value, min), max);
}

function screenLabel(screen: Screen) {
  const labels: Record<Screen, string> = {
    dashboard: "Dashboard",
    "ai-agent": "AI Agent",
    sql: "SQL Workspace",
    pipelines: "Pipelines",
    airflow: "Airflow DAGs",
    spark: "Spark Jobs",
    connections: "Connections",
    catalog: "Data Catalog",
    settings: "Settings",
    profile: "Profile",
  };
  return labels[screen];
}
