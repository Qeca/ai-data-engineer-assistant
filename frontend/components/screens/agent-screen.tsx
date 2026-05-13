"use client";

import { useMutation, useQuery, useQueryClient } from "@tanstack/react-query";
import { MessageSquare, Plus, SendHorizonal } from "lucide-react";
import { FormEvent, useEffect, useState } from "react";
import ReactMarkdown from "react-markdown";
import remarkGfm from "remark-gfm";
import { api } from "@/lib/api";
import { useAppStore, type Screen } from "@/lib/store";
import type { AgentMessage, AgentStreamEvent, ToolCall, UiAction } from "@/types";
import { Badge } from "@/components/ui";

type ChatMessage = {
  role: "user" | "assistant" | "tool";
  content: string;
  intent?: string;
  tools?: ToolCall[];
  uiActions?: UiAction[];
  toolOutput?: Record<string, unknown>;
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
  const queryClient = useQueryClient();
  const [sessionId, setSessionId] = useState<string | null>(null);
  const [input, setInput] = useState("");
  const [messages, setMessages] = useState<ChatMessage[]>(starter);

  const sessions = useQuery({
    queryKey: ["agent-sessions", token],
    queryFn: () => api.sessions(token ?? ""),
    enabled: Boolean(token),
  });

  const sessionMessages = useQuery({
    queryKey: ["agent-session-messages", token, sessionId],
    queryFn: () => api.sessionMessages(token ?? "", sessionId ?? ""),
    enabled: Boolean(token && sessionId),
  });

  useEffect(() => {
    if (!sessionMessages.data) return;
    setMessages(sessionMessages.data.map(toChatMessage));
  }, [sessionMessages.data]);

  const ask = useMutation({
    mutationFn: (query: string) =>
      api.agentQueryStream(
        token ?? "",
        query,
        sessionId,
        {
          screen,
          user,
          visible_panels: ["navigation", "chat", "tools"],
        },
        handleStreamEvent,
      ),
    onSuccess: () => {
      queryClient.invalidateQueries({ queryKey: ["agent-sessions"] });
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

  function openNewChat() {
    setSessionId(null);
    setMessages(starter);
  }

  function openChat(id: string) {
    setSessionId(id);
  }

  function applyUiActions(actions: UiAction[]) {
    for (const action of actions) {
      if (action.type === "navigate" && typeof action.screen === "string") {
        setScreen(action.screen as Screen);
      }
      if (action.type === "refresh_connections") {
        queryClient.invalidateQueries({ queryKey: ["database-connections"] });
      }
    }
  }

  function handleStreamEvent(event: AgentStreamEvent) {
    if (event.type === "session") {
      setSessionId(event.session_id);
      return;
    }

    if (event.type === "tool_call_start") {
      setMessages((current) => [
        ...current,
        {
          role: "tool",
          content: `Запускаю \`${event.tool_name}\``,
          intent: "tool-start",
          toolOutput: event.arguments,
        },
      ]);
      return;
    }

    if (event.type === "tool_call_result") {
      const toolCall = event.tool_call;
      applyUiActions(toolCall.ui_actions ?? []);
      setMessages((current) => [
        ...current,
        {
          role: "tool",
          content: summarizeToolCall(toolCall),
          intent: "tool-result",
          tools: [toolCall],
          toolOutput: toolCall.output,
        },
      ]);
      return;
    }

    if (event.type === "final") {
      const result = event.response;
      setSessionId(result.session_id);
      applyUiActions(result.ui_actions);
      queryClient.invalidateQueries({ queryKey: ["agent-sessions"] });
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
      return;
    }

    if (event.type === "error") {
      setMessages((current) => [...current, { role: "assistant", content: event.error, intent: "agent-error" }]);
    }
  }

  return (
    <div className="split">
      <aside className="left-panel">
        <div className="card-body">
          <div style={{ display: "flex", alignItems: "center", gap: 8, marginBottom: 10 }}>
            <div className="stat-label" style={{ flex: 1 }}>Chats</div>
            <button className="icon-btn" onClick={openNewChat} title="New chat">
              <Plus size={14} />
            </button>
          </div>
          <div className="chat-session-list">
            <button className={`chat-session ${sessionId === null ? "active" : ""}`} onClick={openNewChat}>
              <MessageSquare size={14} />
              <span>Новый чат</span>
            </button>
            {(sessions.data ?? []).map((session) => (
              <button
                className={`chat-session ${session.id === sessionId ? "active" : ""}`}
                key={session.id}
                onClick={() => openChat(session.id)}
              >
                <MessageSquare size={14} />
                <span>{session.title}</span>
              </button>
            ))}
          </div>
        </div>
        <div className="card-body" style={{ borderTop: "1px solid var(--border-subtle)" }}>
          <div className="stat-label" style={{ marginBottom: 8 }}>Tools</div>
          {["SiteStatusTool", "SiteControlTool", "SQLTool", "CatalogTool", "DatabaseConnectionTool", "AirflowTool", "SparkTool"].map((tool) => (
            <div key={tool} style={{ display: "flex", alignItems: "center", gap: 8, marginBottom: 7 }}>
              <Badge status="success">ready</Badge>
              <span className="mono">{tool}</span>
            </div>
          ))}
        </div>
      </aside>

      <section className="work-area">
        <div className="chat-list">
          {messages.map((message, index) => (
            <div className={`chat-msg ${message.role === "user" ? "user" : ""} ${message.role === "tool" ? "tool" : ""}`} key={`${message.role}-${index}`}>
              <div className={`avatar ${message.role === "assistant" ? "ai" : ""}`}>
                {message.role === "assistant" ? "AI" : message.role === "tool" ? "TOOL" : "YOU"}
              </div>
              <div className="bubble">
                {message.role === "assistant" ? (
                  <ReactMarkdown remarkPlugins={[remarkGfm]}>{message.content}</ReactMarkdown>
                ) : (
                  <div>{message.content}</div>
                )}
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
                {message.toolOutput && (
                  <details className="tool-output" open>
                    <summary>result</summary>
                    <pre>{formatToolOutput(message.toolOutput)}</pre>
                  </details>
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
          {sessionMessages.isFetching && sessionId && (
            <div className="chat-msg">
              <div className="avatar ai">AI</div>
              <div className="bubble">Загружаю чат...</div>
            </div>
          )}
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

function toChatMessage(message: AgentMessage): ChatMessage {
  const role = message.role === "user" || message.role === "tool" ? message.role : "assistant";
  const metadata = message.metadata_json ?? {};
  const intent = typeof metadata.intent === "string" ? metadata.intent : undefined;
  const toolCall = isToolCall(metadata.tool_call) ? metadata.tool_call : undefined;
  const toolCalls = Array.isArray(metadata.tool_calls) ? metadata.tool_calls.filter(isToolCall) : undefined;
  const uiActions = Array.isArray(metadata.ui_actions) ? (metadata.ui_actions as UiAction[]) : undefined;

  return {
    role,
    content: message.content,
    intent,
    tools: toolCall ? [toolCall] : toolCalls,
    uiActions,
    toolOutput: role === "tool" ? toolCall?.output : undefined,
  };
}

function isToolCall(value: unknown): value is ToolCall {
  if (!value || typeof value !== "object") return false;
  const candidate = value as Partial<ToolCall>;
  return (
    typeof candidate.tool_name === "string" &&
    typeof candidate.status === "string" &&
    typeof candidate.latency_ms === "number" &&
    typeof candidate.input === "object" &&
    typeof candidate.output === "object"
  );
}

function summarizeToolCall(toolCall: ToolCall): string {
  const state = toolCall.status === "success" ? "готов" : "ошибка";
  return `\`${toolCall.tool_name}\` ${state} за ${toolCall.latency_ms}ms`;
}

function formatToolOutput(output: Record<string, unknown>): string {
  const text = JSON.stringify(output, null, 2);
  if (text.length <= 3000) return text;
  return `${text.slice(0, 3000)}\n... truncated`;
}
