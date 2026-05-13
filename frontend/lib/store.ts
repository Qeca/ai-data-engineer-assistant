"use client";

import { create } from "zustand";
import { persist } from "zustand/middleware";
import type { User } from "@/types";

export type Screen =
  | "dashboard"
  | "ai-agent"
  | "sql"
  | "pipelines"
  | "airflow"
  | "spark"
  | "connections"
  | "catalog"
  | "settings"
  | "profile";

type AppState = {
  accessToken: string | null;
  refreshToken: string | null;
  user: User | null;
  screen: Screen;
  agentSessionId: string | null;
  theme: "dark" | "light";
  setAuth: (accessToken: string, refreshToken: string) => void;
  setUser: (user: User | null) => void;
  setScreen: (screen: Screen) => void;
  setAgentSessionId: (sessionId: string | null) => void;
  toggleTheme: () => void;
  logout: () => void;
};

export const useAppStore = create<AppState>()(
  persist(
    (set, get) => ({
      accessToken: null,
      refreshToken: null,
      user: null,
      screen: "dashboard",
      agentSessionId: null,
      theme: "dark",
      setAuth: (accessToken, refreshToken) => set({ accessToken, refreshToken }),
      setUser: (user) => set({ user }),
      setScreen: (screen) => set({ screen }),
      setAgentSessionId: (agentSessionId) => set({ agentSessionId }),
      toggleTheme: () => set({ theme: get().theme === "dark" ? "light" : "dark" }),
      logout: () => set({ accessToken: null, refreshToken: null, user: null, screen: "dashboard", agentSessionId: null }),
    }),
    { name: "ai-de-assistant" },
  ),
);
