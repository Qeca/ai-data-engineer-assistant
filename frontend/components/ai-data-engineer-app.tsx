"use client";

import { useQuery } from "@tanstack/react-query";
import { useEffect, useState } from "react";
import { api } from "@/lib/api";
import { useAppStore } from "@/lib/store";
import { LoginPanel } from "@/components/login-panel";
import { Shell } from "@/components/shell";
import { AgentScreen } from "@/components/screens/agent-screen";
import { AirflowScreen } from "@/components/screens/airflow-screen";
import { CatalogScreen } from "@/components/screens/catalog-screen";
import { ConnectionsScreen } from "@/components/screens/connections-screen";
import { DashboardScreen } from "@/components/screens/dashboard-screen";
import { PipelinesScreen } from "@/components/screens/pipelines-screen";
import { ProfileScreen } from "@/components/screens/profile-screen";
import { SettingsScreen } from "@/components/screens/settings-screen";
import { SparkScreen } from "@/components/screens/spark-screen";
import { SqlScreen } from "@/components/screens/sql-screen";

export function AIDataEngineerApp() {
  const [hydrated, setHydrated] = useState(false);
  const token = useAppStore((state) => state.accessToken);
  const screen = useAppStore((state) => state.screen);
  const setUser = useAppStore((state) => state.setUser);
  const logout = useAppStore((state) => state.logout);

  const me = useQuery({
    queryKey: ["me", token],
    queryFn: () => api.me(token ?? ""),
    enabled: Boolean(token),
    retry: false,
  });

  useEffect(() => {
    if (me.data) setUser(me.data);
    if (me.error) logout();
  }, [me.data, me.error, logout, setUser]);

  useEffect(() => {
    const unsubscribe = useAppStore.persist.onFinishHydration(() => setHydrated(true));
    setHydrated(useAppStore.persist.hasHydrated());
    return unsubscribe;
  }, []);

  if (!hydrated) {
    return <main className="app-loading" />;
  }

  if (!token) {
    return <LoginPanel />;
  }

  return (
    <Shell>
      {screen === "dashboard" && <DashboardScreen />}
      {screen === "ai-agent" && <AgentScreen />}
      {screen === "sql" && <SqlScreen />}
      {screen === "pipelines" && <PipelinesScreen />}
      {screen === "airflow" && <AirflowScreen />}
      {screen === "spark" && <SparkScreen />}
      {screen === "connections" && <ConnectionsScreen />}
      {screen === "catalog" && <CatalogScreen />}
      {screen === "settings" && <SettingsScreen />}
      {screen === "profile" && <ProfileScreen />}
    </Shell>
  );
}
