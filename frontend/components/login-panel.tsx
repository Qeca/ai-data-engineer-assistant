"use client";

import { useMutation } from "@tanstack/react-query";
import { DatabaseZap } from "lucide-react";
import { FormEvent, useState } from "react";
import { api } from "@/lib/api";
import { useAppStore } from "@/lib/store";

export function LoginPanel() {
  const setAuth = useAppStore((state) => state.setAuth);
  const setUser = useAppStore((state) => state.setUser);
  const [email, setEmail] = useState("admin@local.dev");
  const [password, setPassword] = useState("admin");

  const login = useMutation({
    mutationFn: () => api.login(email, password),
    onSuccess: async (tokens) => {
      setAuth(tokens.access_token, tokens.refresh_token);
      const user = await api.me(tokens.access_token);
      setUser(user);
    },
  });

  function submit(event: FormEvent) {
    event.preventDefault();
    login.mutate();
  }

  return (
    <main className="login-page">
      <section className="login-panel">
        <div className="card-header">
          <div className="logo-mark">DE</div>
          <div>
            <div className="card-title">AI Data Engineer Assistant</div>
            <div className="card-sub">Локальный демо-вход: admin@local.dev / admin</div>
          </div>
        </div>
        <form onSubmit={submit}>
          <label>
            <div className="stat-label">Email</div>
            <input className="input" value={email} onChange={(event) => setEmail(event.target.value)} />
          </label>
          <label>
            <div className="stat-label">Password</div>
            <input
              className="input"
              type="password"
              value={password}
              onChange={(event) => setPassword(event.target.value)}
            />
          </label>
          {login.error && <div className="error-text">{login.error.message}</div>}
          <button className="btn btn-primary" disabled={login.isPending}>
            <DatabaseZap size={15} />
            {login.isPending ? "Signing in..." : "Sign in"}
          </button>
        </form>
      </section>
    </main>
  );
}
