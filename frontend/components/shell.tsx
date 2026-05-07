"use client";

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
import { useAppStore, type Screen } from "@/lib/store";

const sections: { label: string; items: { screen: Screen; label: string; icon: React.ElementType; badge?: string }[] }[] =
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
        { screen: "pipelines", label: "Pipelines", icon: GitBranch },
        { screen: "airflow", label: "Airflow DAGs", icon: Network, badge: "2" },
        { screen: "spark", label: "Spark Jobs", icon: Sparkles },
      ],
    },
    {
      label: "Infrastructure",
      items: [
        { screen: "connections", label: "Connections", icon: Boxes },
        { screen: "catalog", label: "Data Catalog", icon: Database },
      ],
    },
  ];

export function Shell({ children }: { children: React.ReactNode }) {
  const screen = useAppStore((state) => state.screen);
  const setScreen = useAppStore((state) => state.setScreen);
  const theme = useAppStore((state) => state.theme);
  const toggleTheme = useAppStore((state) => state.toggleTheme);
  const logout = useAppStore((state) => state.logout);
  const user = useAppStore((state) => state.user);

  return (
    <div className={`app-shell ${theme === "light" ? "theme-light" : ""}`}>
      <aside className="sidebar">
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
                    <span>{item.label}</span>
                    {item.badge && <span className="tag" style={{ marginLeft: "auto" }}>{item.badge}</span>}
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
