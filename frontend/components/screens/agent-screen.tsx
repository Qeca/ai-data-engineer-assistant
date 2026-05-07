"use client";

import { useMutation } from "@tanstack/react-query";
import { SendHorizonal } from "lucide-react";
import { FormEvent, useState } from "react";
import { api } from "@/lib/api";
import { useAppStore, type Screen } from "@/lib/store";
import type { ToolCall, UiAction } from "@/types";
import { Badge } from "@/components/ui";

type ChatMessage = {
  role: "user" | "assistant";
  content: string;
  intent?: string;
  tools?: ToolCall[];
  uiActions?: UiAction[];
};

const starter: ChatMessage[] = [
  {
    role: "assistant",
    content:
      "Привет. Я могу выполнить SQL-анализ, запустить Airflow DAG, отправить Spark job или показать каталог таблиц.",
  },
];

export function AgentScreen() {
  const token = useAppStore((state) => state.accessToken);
  const screen = useAppStore((state) => state.screen);
  const setScreen = useAppStore((state) => state.setScreen);
  const user = useAppStore((state) => state.user);
  const [sessionId, setSessionId] = useState<string | null>(null);
  const [input, setInput] = useState("Проанализируй таблицу orders за последние 30 дней и найди аномалии по часам");
  const [messages, setMessages] = useState<ChatMessage[]>(starter);

  const ask = useMutation({
    mutationFn: (query: string) =>
      api.agentQuery(token ?? "", query, sessionId, {
        screen,
        user,
        visible_panels: ["navigation", "chat", "tools"],
      }),
    onSuccess: (result) => {
      setSessionId(result.session_id);
      applyUiActions(result.ui_actions);
      setMessages((current) => [
        ...current,
        {
          role: "assistant",
          content: result.answer,
          intent: result.intent,
          tools: result.tool_calls,
          uiActions: result.ui_actions,
        },
      ]);
    },
  });

  function submit(event: FormEvent) {
    event.preventDefault();
    const query = input.trim();
    if (!query) return;
    setMessages((current) => [...current, { role: "user", content: query }]);
    setInput("");
    ask.mutate(query);
  }

  function applyUiActions(actions: UiAction[]) {
    for (const action of actions) {
      if (action.type === "navigate" && typeof action.screen === "string") {
        setScreen(action.screen as Screen);
      }
    }
  }

  return (
    <div className="split">
      <aside className="left-panel">
        <div className="card-body">
          <div className="stat-label" style={{ marginBottom: 8 }}>Tools</div>
          {["SiteStatusTool", "SiteControlTool", "SQLTool", "CatalogTool", "AirflowTool", "SparkTool"].map((tool) => (
            <div key={tool} style={{ display: "flex", alignItems: "center", gap: 8, marginBottom: 7 }}>
              <Badge status="success">ready</Badge>
              <span className="mono">{tool}</span>
            </div>
          ))}
        </div>
        <div className="card-body" style={{ borderTop: "1px solid var(--border-subtle)" }}>
          <div className="stat-label" style={{ marginBottom: 8 }}>Examples</div>
          {[
            "Запусти DAG orders_sync",
            "Отправь Spark job для clickstream",
            "Покажи каталог таблиц",
          ].map((text) => (
            <button key={text} className="btn btn-ghost" style={{ width: "100%", justifyContent: "flex-start" }} onClick={() => setInput(text)}>
              {text}
            </button>
          ))}
        </div>
      </aside>

      <section className="work-area">
        <div className="chat-list">
          {messages.map((message, index) => (
            <div className={`chat-msg ${message.role === "user" ? "user" : ""}`} key={`${message.role}-${index}`}>
              <div className={`avatar ${message.role === "assistant" ? "ai" : ""}`}>
                {message.role === "assistant" ? "AI" : "YOU"}
              </div>
              <div className="bubble">
                <div>{message.content}</div>
                {message.intent && (
                  <div style={{ marginTop: 10, display: "flex", gap: 8, flexWrap: "wrap" }}>
                    <Badge status="ai">{message.intent}</Badge>
                    {message.tools?.map((tool) => (
                      <Badge key={`${tool.tool_name}-${tool.latency_ms}`} status={tool.status}>
                        {tool.tool_name} · {tool.latency_ms}ms
                      </Badge>
                    ))}
                    {message.uiActions?.map((action, actionIndex) => (
                      <span className="tag" key={`${action.type}-${actionIndex}`}>
                        ui:{action.type}
                      </span>
                    ))}
                  </div>
                )}
              </div>
            </div>
          ))}
          {ask.isPending && (
            <div className="chat-msg">
              <div className="avatar ai">AI</div>
              <div className="bubble">Выполняю tool-call...</div>
            </div>
          )}
          {ask.error && <div className="error-text">{ask.error.message}</div>}
        </div>

        <form className="chat-input" onSubmit={submit}>
          <textarea className="textarea" value={input} onChange={(event) => setInput(event.target.value)} />
          <button className="btn btn-primary" disabled={ask.isPending}>
            <SendHorizonal size={14} />
            Send
          </button>
        </form>
      </section>
    </div>
  );

}
