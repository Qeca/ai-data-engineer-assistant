"use client";

import { useMutation, useQuery, useQueryClient } from "@tanstack/react-query";
import { CheckCircle2, ChevronRight, Circle, Loader2, MessageSquare, Plus, SendHorizonal, Trash2 } from "lucide-react";
import { FormEvent, PointerEvent, useEffect, useState } from "react";
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

type RenderItem =
  | { type: "message"; message: ChatMessage; key: string }
  | { type: "activity"; messages: ChatMessage[]; key: string }
  | { type: "assistant_with_activity"; message: ChatMessage; activityMessages: ChatMessage[]; key: string };

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
  const [panelWidth, setPanelWidth] = useState(270);

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

  useEffect(() => {
    const saved = window.localStorage.getItem("agent-sidebar-width");
    if (saved) setPanelWidth(clamp(Number(saved), 230, 520));
  }, []);

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

  const deleteChat = useMutation({
    mutationFn: (id: string) => api.deleteSession(token ?? "", id),
    onSuccess: (_, deletedId) => {
      if (sessionId === deletedId) {
        setSessionId(null);
        setMessages(starter);
      }
      queryClient.invalidateQueries({ queryKey: ["agent-sessions"] });
      queryClient.removeQueries({ queryKey: ["agent-session-messages", token, deletedId] });
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

  function removeChat(id: string) {
    deleteChat.mutate(id);
  }

  function resizePanel(event: PointerEvent<HTMLDivElement>) {
    const startX = event.clientX;
    const startWidth = panelWidth;
    let nextWidth = startWidth;
    event.currentTarget.setPointerCapture(event.pointerId);

    function move(moveEvent: globalThis.PointerEvent) {
      nextWidth = clamp(startWidth + moveEvent.clientX - startX, 230, 520);
      setPanelWidth(nextWidth);
    }

    function up() {
      window.localStorage.setItem("agent-sidebar-width", String(nextWidth));
      window.removeEventListener("pointermove", move);
      window.removeEventListener("pointerup", up);
    }

    window.addEventListener("pointermove", move);
    window.addEventListener("pointerup", up);
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
      <aside className="left-panel resizable-panel" style={{ width: panelWidth }}>
        <div className="card-body">
          <div className="chat-panel-heading">
            <div className="stat-label">Chats</div>
            <button className="btn btn-ghost icon-btn" type="button" onClick={openNewChat} title="Новый чат" aria-label="Новый чат">
              <Plus size={14} />
            </button>
          </div>
          <div className="chat-session-list">
            <button className={`chat-session ${sessionId === null ? "active" : ""}`} onClick={openNewChat}>
              <MessageSquare size={14} />
              <span>Новый чат</span>
            </button>
            {(sessions.data ?? []).map((session) => (
              <div className={`chat-session-row ${session.id === sessionId ? "active" : ""}`} key={session.id}>
                <button className="chat-session" type="button" onClick={() => openChat(session.id)}>
                  <MessageSquare size={14} />
                  <span>{session.title}</span>
                </button>
                <button
                  className="btn btn-ghost icon-btn chat-delete"
                  type="button"
                  onClick={() => removeChat(session.id)}
                  disabled={deleteChat.isPending}
                  title="Удалить чат"
                  aria-label={`Удалить чат ${session.title}`}
                >
                  <Trash2 size={13} />
                </button>
              </div>
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
        <div className="resize-handle" onPointerDown={resizePanel} aria-label="Resize chats" role="separator" />
      </aside>

      <section className="work-area">
        <div className="chat-list">
          {groupMessages(messages).map((item) =>
            item.type === "activity" ? (
              <ActivityMessage key={item.key} messages={item.messages} />
            ) : item.type === "assistant_with_activity" ? (
              <AssistantMessage key={item.key} message={item.message} activityMessages={item.activityMessages} />
            ) : (
              <div className={`chat-msg ${item.message.role === "user" ? "user" : ""}`} key={item.key}>
                <div className={`avatar ${item.message.role === "assistant" ? "ai" : ""}`}>
                  {item.message.role === "assistant" ? "AI" : "YOU"}
                </div>
                <div className="bubble">
                  {item.message.role === "assistant" ? (
                    <ReactMarkdown remarkPlugins={[remarkGfm]}>{item.message.content}</ReactMarkdown>
                  ) : (
                    <div>{item.message.content}</div>
                  )}
                  {item.message.intent && (
                    <div style={{ marginTop: 10, display: "flex", gap: 8, flexWrap: "wrap" }}>
                      <Badge status="ai">{item.message.intent}</Badge>
                      {item.message.tools?.map((tool) => (
                        <Badge key={`${tool.tool_name}-${tool.latency_ms}`} status={tool.status}>
                          {tool.tool_name} · {tool.latency_ms}ms
                        </Badge>
                      ))}
                      {item.message.uiActions?.map((action, actionIndex) => (
                        <span className="tag" key={`${action.type}-${actionIndex}`}>
                          ui:{action.type}
                        </span>
                      ))}
                    </div>
                  )}
                </div>
              </div>
            ),
          )}
          {ask.isPending && messages.at(-1)?.role !== "tool" && (
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

function AssistantMessage({
  message,
  activityMessages = [],
}: {
  message: ChatMessage;
  activityMessages?: ChatMessage[];
}) {
  return (
    <div className="chat-msg">
      <div className="avatar ai">AI</div>
      <div className="assistant-composite">
        {activityMessages.length > 0 && <ActivityPanel messages={activityMessages} />}
        <div className="bubble assistant-answer">
          <ReactMarkdown remarkPlugins={[remarkGfm]}>{message.content}</ReactMarkdown>
          {message.intent && activityMessages.length === 0 && (
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
    </div>
  );
}

function ActivityMessage({ messages }: { messages: ChatMessage[] }) {
  return (
    <div className="chat-msg activity">
      <div className="avatar ai">AI</div>
      <ActivityPanel messages={messages} />
    </div>
  );
}

function ActivityPanel({ messages }: { messages: ChatMessage[] }) {
  const toolCalls = messages.flatMap((message) => message.tools ?? []);
  const totalLatency = toolCalls.reduce((sum, tool) => sum + tool.latency_ms, 0);
  const hasPending = messages.some((message) => message.intent === "tool-start");

  return (
    <details className="activity-card">
      <summary className="activity-header">
        <ChevronRight size={14} className="activity-chevron" />
        {hasPending ? <Loader2 size={15} className="spin" /> : <CheckCircle2 size={15} />}
        <span>Активность</span>
        {totalLatency > 0 && <span className="activity-time">· {formatLatency(totalLatency)}</span>}
      </summary>
      <div className="activity-timeline">
        {messages.map((message, index) => (
          <ActivityStep key={`${message.content}-${index}`} message={message} />
        ))}
      </div>
    </details>
  );
}

function ActivityStep({ message }: { message: ChatMessage }) {
  const tool = message.tools?.[0];
  const isPending = message.intent === "tool-start";

  return (
    <div className="activity-step">
      <div className={`activity-step-icon ${tool?.status === "error" ? "error" : ""}`}>
        {isPending ? <Loader2 size={13} className="spin" /> : tool?.status === "success" ? <CheckCircle2 size={13} /> : <Circle size={13} />}
      </div>
      <div className="activity-step-body">
        <div className="activity-step-title">{activityTitle(message, tool)}</div>
        <div className="activity-step-sub">{activitySummary(message, tool)}</div>
        <div className="activity-step-meta">
          {tool && <span>{tool.tool_name}</span>}
          {tool && <span>{formatLatency(tool.latency_ms)}</span>}
          {tool?.status && <span>{tool.status}</span>}
        </div>
      </div>
    </div>
  );
}

function groupMessages(items: ChatMessage[]): RenderItem[] {
  const grouped: RenderItem[] = [];
  let index = 0;

  while (index < items.length) {
    const message = items[index];
    if (message.role !== "tool") {
      grouped.push({ type: "message", message, key: `${message.role}-${index}` });
      index += 1;
      continue;
    }

    const activityMessages: ChatMessage[] = [];
    const startIndex = index;
    while (items[index]?.role === "tool") {
      activityMessages.push(items[index]);
      index += 1;
    }
    if (items[index]?.role === "assistant") {
      grouped.push({
        type: "assistant_with_activity",
        message: items[index],
        activityMessages,
        key: `assistant-activity-${startIndex}`,
      });
      index += 1;
      continue;
    }
    grouped.push({ type: "activity", messages: activityMessages, key: `activity-${startIndex}` });
  }

  return grouped;
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

function activityTitle(message: ChatMessage, tool?: ToolCall): string {
  if (message.intent === "tool-start") return stripMarkdown(message.content);
  if (!tool) return stripMarkdown(message.content);
  if (tool.status === "error") return `${tool.tool_name} завершился с ошибкой`;
  return `${tool.tool_name} выполнен`;
}

function activitySummary(message: ChatMessage, tool?: ToolCall): string {
  if (message.intent === "tool-start") return "Агент вызвал function tool и ждёт результат.";
  if (!tool) return "Шаг выполнен.";

  const output = tool.output;
  if (Array.isArray(output.pipelines)) {
    const names = output.pipelines
      .map((item) => (isRecord(item) ? item.dag_id : undefined))
      .filter(Boolean)
      .slice(0, 4)
      .join(", ");
    return `Проверены Airflow DAG: ${output.pipelines.length}${names ? ` (${names})` : ""}.`;
  }
  if (Array.isArray(output.rows)) {
    return `SQL вернул строк: ${output.rows.length}.`;
  }
  if (Array.isArray(output.tables)) {
    return `Каталог БД: ${output.tables.length} таблиц.`;
  }
  if (typeof output.run_id === "string") {
    return `DAG run: ${output.run_id}, статус ${String(output.status ?? tool.status)}.`;
  }
  if (typeof output.job_id === "string") {
    return `Spark job: ${output.job_id}, статус ${String(output.status ?? tool.status)}.`;
  }
  if (typeof output.status === "string") {
    return `Статус: ${output.status}.`;
  }
  return "Результат получен и передан модели.";
}

function isRecord(value: unknown): value is Record<string, unknown> {
  return Boolean(value && typeof value === "object" && !Array.isArray(value));
}

function stripMarkdown(value: string): string {
  return value.replaceAll("`", "");
}

function formatLatency(ms: number): string {
  if (ms < 1000) return `${ms}ms`;
  return `${(ms / 1000).toFixed(ms >= 10000 ? 0 : 1)}s`;
}

function clamp(value: number, min: number, max: number) {
  return Math.min(Math.max(value, min), max);
}

function formatToolOutput(output: Record<string, unknown>): string {
  const text = JSON.stringify(output, null, 2);
  if (text.length <= 3000) return text;
  return `${text.slice(0, 3000)}\n... truncated`;
}
