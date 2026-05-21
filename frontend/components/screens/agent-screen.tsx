"use client";

import { useMutation, useQuery, useQueryClient } from "@tanstack/react-query";
import dynamic from "next/dynamic";
import { CheckCircle2, ChevronRight, Circle, Code2, Loader2, MessageSquare, Plus, RotateCcw, SendHorizonal, Trash2, X } from "lucide-react";
import { FormEvent, PointerEvent, useEffect, useRef, useState } from "react";
import ReactMarkdown from "react-markdown";
import remarkGfm from "remark-gfm";
import { api } from "@/lib/api";
import { useAppStore, type Screen } from "@/lib/store";
import type { AgentMessage, AgentStreamEvent, ArtifactVersion, ToolCall, UiAction } from "@/types";
import { Badge } from "@/components/ui";

type ChatMessage = {
  role: "user" | "assistant" | "tool";
  content: string;
  intent?: string;
  tools?: ToolCall[];
  uiActions?: UiAction[];
  toolOutput?: Record<string, unknown>;
  elapsedMs?: number;
  startedAt?: number;
};

type RenderItem =
  | { type: "message"; message: ChatMessage; key: string }
  | { type: "activity"; messages: ChatMessage[]; key: string }
  | { type: "assistant_with_activity"; message: ChatMessage; activityMessages: ChatMessage[]; key: string };

type CodeArtifact = {
  artifactType: string;
  artifactName: string;
  code: string;
  originalCode: string;
  path?: string;
  runtimePath?: string;
  toolName?: string;
  status?: string;
  validationStatus?: string;
  version?: number;
  gitCommit?: string;
  deployedToRuntime?: boolean;
};

const MonacoEditor = dynamic(() => import("@monaco-editor/react"), { ssr: false });

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
  const sessionId = useAppStore((state) => state.agentSessionId);
  const setSessionId = useAppStore((state) => state.setAgentSessionId);
  const queryClient = useQueryClient();
  const [input, setInput] = useState("");
  const [messages, setMessages] = useState<ChatMessage[]>(starter);
  const [panelWidth, setPanelWidth] = useState(270);
  const [codePanelWidth, setCodePanelWidth] = useState(560);
  const [activeArtifact, setActiveArtifact] = useState<CodeArtifact | null>(null);
  const [latestArtifact, setLatestArtifact] = useState<CodeArtifact | null>(null);
  const [codePanelDismissed, setCodePanelDismissed] = useState(
    () => typeof window !== "undefined" && window.localStorage.getItem("agent-code-panel-dismissed") === "true",
  );
  const [streamingSessionId, setStreamingSessionId] = useState<string | null>(null);
  const abortControllerRef = useRef<AbortController | null>(null);

  const sessions = useQuery({
    queryKey: ["agent-sessions", token],
    queryFn: () => api.sessions(token ?? ""),
    enabled: Boolean(token),
  });

  const sessionMessages = useQuery({
    queryKey: ["agent-session-messages", token, sessionId],
    queryFn: () => api.sessionMessages(token ?? "", sessionId ?? ""),
    enabled: Boolean(token && sessionId && sessionId !== streamingSessionId),
  });

  useEffect(() => {
    const saved = window.localStorage.getItem("agent-sidebar-width");
    if (saved) setPanelWidth(clamp(Number(saved), 230, 520));
    const savedCodePanel = window.localStorage.getItem("agent-code-panel-width");
    if (savedCodePanel) setCodePanelWidth(clamp(Number(savedCodePanel), 380, 820));
  }, []);

  const ask = useMutation({
    mutationFn: (query: string) => {
      const controller = new AbortController();
      abortControllerRef.current = controller;
      return api.agentQueryStream(
        token ?? "",
        query,
        sessionId,
        {
          screen,
          user,
          visible_panels: activeArtifact ? ["navigation", "chat", "tools", "code"] : ["navigation", "chat", "tools"],
        },
        handleStreamEvent,
        controller.signal,
      );
    },
    onSuccess: () => {
      queryClient.invalidateQueries({ queryKey: ["agent-sessions"] });
    },
    onError: (error) => {
      setStreamingSessionId(null);
      if (isAbortError(error)) {
        setMessages((current) => [
          ...markPendingActivityStopped(current),
          { role: "assistant", content: "Генерация остановлена пользователем.", intent: "agent-stopped" },
        ]);
      }
    },
    onSettled: () => {
      abortControllerRef.current = null;
    },
  });

  useEffect(() => {
    if (!sessionMessages.data || sessionId === streamingSessionId) return;
    const nextMessages = sessionMessages.data.map(toChatMessage);
    const artifact = findLatestArtifact(nextMessages);
    setMessages(nextMessages);
    setLatestArtifact(artifact);
    setActiveArtifact(codePanelDismissed ? null : artifact);
  }, [codePanelDismissed, sessionId, sessionMessages.data, streamingSessionId]);

  useEffect(() => {
    if (!sessionId || ask.isPending || !sessionMessages.error) return;
    const message = sessionMessages.error instanceof Error ? sessionMessages.error.message : "";
    if (message !== "Session not found") return;
    setSessionId(null);
    setMessages(starter);
    setActiveArtifact(null);
    queryClient.removeQueries({ queryKey: ["agent-session-messages", token, sessionId] });
  }, [ask.isPending, queryClient, sessionId, sessionMessages.error, setSessionId, token]);

  const deleteChat = useMutation({
    mutationFn: (id: string) => api.deleteSession(token ?? "", id),
    onSuccess: (_, deletedId) => {
      if (sessionId === deletedId) {
        setSessionId(null);
        setMessages(starter);
        setActiveArtifact(null);
      }
      queryClient.invalidateQueries({ queryKey: ["agent-sessions"] });
      queryClient.removeQueries({ queryKey: ["agent-session-messages", token, deletedId] });
    },
  });

  function submit(event: FormEvent) {
    event.preventDefault();
    if (ask.isPending) return;
    const query = input.trim();
    if (!query) return;
    setStreamingSessionId(sessionId);
    setMessages((current) => [...current, { role: "user", content: query }]);
    setInput("");
    ask.mutate(query);
  }

  function stopGeneration() {
    abortControllerRef.current?.abort();
  }

  function openNewChat() {
    setStreamingSessionId(null);
    setSessionId(null);
    setMessages(starter);
    setActiveArtifact(null);
  }

  function openChat(id: string) {
    setStreamingSessionId(null);
    setSessionId(id);
    setActiveArtifact(null);
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

  function resizeCodePanel(event: PointerEvent<HTMLDivElement>) {
    const startX = event.clientX;
    const startWidth = codePanelWidth;
    let nextWidth = startWidth;
    event.currentTarget.setPointerCapture(event.pointerId);

    function move(moveEvent: globalThis.PointerEvent) {
      nextWidth = clamp(startWidth - moveEvent.clientX + startX, 380, 820);
      setCodePanelWidth(nextWidth);
    }

    function up() {
      window.localStorage.setItem("agent-code-panel-width", String(nextWidth));
      window.removeEventListener("pointermove", move);
      window.removeEventListener("pointerup", up);
    }

    window.addEventListener("pointermove", move);
    window.addEventListener("pointerup", up);
  }

  function updateArtifactCode(code: string) {
    setActiveArtifact((current) => (current ? { ...current, code } : current));
    setLatestArtifact((current) => (current && current.artifactName === activeArtifact?.artifactName ? { ...current, code } : current));
  }

  function resetArtifactCode() {
    setActiveArtifact((current) => (current ? { ...current, code: current.originalCode } : current));
    setLatestArtifact((current) => (current ? { ...current, code: current.originalCode } : current));
  }

  function prepareArtifactSave() {
    if (!activeArtifact) return;
    setInput(buildArtifactSavePrompt(activeArtifact));
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
      setStreamingSessionId(event.session_id);
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
          startedAt: Date.now(),
        },
      ]);
      return;
    }

    if (event.type === "tool_call_result") {
      const toolCall = event.tool_call;
      applyUiActions(toolCall.ui_actions ?? []);
      const artifact = artifactFromToolCall(toolCall);
      if (artifact) {
        setLatestArtifact(artifact);
        if (!codePanelDismissed) setActiveArtifact(artifact);
      }
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
      const artifact = findLatestArtifactFromToolCalls(result.tool_calls);
      if (artifact) {
        setLatestArtifact(artifact);
        if (!codePanelDismissed) setActiveArtifact(artifact);
      }
      queryClient.removeQueries({ queryKey: ["agent-session-messages", token, result.session_id] });
      setStreamingSessionId(null);
      queryClient.invalidateQueries({ queryKey: ["agent-sessions"] });
      queryClient.invalidateQueries({ queryKey: ["agent-session-messages", token, result.session_id] });
      setMessages((current) => [
        ...current,
        {
          role: "assistant",
          content: result.answer,
          intent: result.intent,
          tools: result.tool_calls,
          uiActions: result.ui_actions,
          elapsedMs: result.elapsed_ms,
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
          {["SiteStatusTool", "SiteControlTool", "SQLTool", "CatalogTool", "DatabaseConnectionTool", "AirflowTool", "SparkTool", "GitTool", "BashSandboxTool"].map((tool) => (
            <div key={tool} style={{ display: "flex", alignItems: "center", gap: 8, marginBottom: 7 }}>
              <Badge status="success">ready</Badge>
              <span className="mono">{tool}</span>
            </div>
          ))}
        </div>
        <div className="resize-handle" onPointerDown={resizePanel} aria-label="Resize chats" role="separator" />
      </aside>

      <section className="work-area agent-work-area">
        <div className="agent-conversation">
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
            {ask.error && !isAbortError(ask.error) && <div className="error-text">{ask.error.message}</div>}
            {sessionMessages.isFetching && sessionId && (
              <div className="chat-msg">
                <div className="avatar ai">AI</div>
                <div className="bubble">Загружаю чат...</div>
              </div>
            )}
          </div>

          <form className="chat-input" onSubmit={submit}>
            <textarea className="textarea" value={input} onChange={(event) => setInput(event.target.value)} />
            {ask.isPending ? (
              <button className="btn btn-secondary" type="button" onClick={stopGeneration}>
                <X size={14} />
                Остановить
              </button>
            ) : (
              <button className="btn btn-primary" disabled={!input.trim()}>
                <SendHorizonal size={14} />
                Send
              </button>
            )}
          </form>
        </div>

        {!activeArtifact && latestArtifact && (
          <button
            className="code-drawer-tab"
            type="button"
            onClick={() => {
              setCodePanelDismissed(false);
              window.localStorage.setItem("agent-code-panel-dismissed", "false");
              setActiveArtifact(latestArtifact);
            }}
          >
            <Code2 size={14} />
            Код
          </button>
        )}

        {activeArtifact && (
          <CodeWorkspace
            artifact={activeArtifact}
            width={codePanelWidth}
            onChange={updateArtifactCode}
            onClose={() => {
              setCodePanelDismissed(true);
              window.localStorage.setItem("agent-code-panel-dismissed", "true");
              setActiveArtifact(null);
            }}
            onPrepareSave={prepareArtifactSave}
            onReset={resetArtifactCode}
            onResize={resizeCodePanel}
          />
        )}
      </section>
    </div>
  );

}

function CodeWorkspace({
  artifact,
  width,
  onChange,
  onClose,
  onPrepareSave,
  onReset,
  onResize,
}: {
  artifact: CodeArtifact;
  width: number;
  onChange: (code: string) => void;
  onClose: () => void;
  onPrepareSave: () => void;
  onReset: () => void;
  onResize: (event: PointerEvent<HTMLDivElement>) => void;
}) {
  const token = useAppStore((state) => state.accessToken);
  const dirty = artifact.code !== artifact.originalCode;
  const [selectedVersion, setSelectedVersion] = useState<number | null>(null);
  const versions = useQuery({
    queryKey: ["artifact-versions", token, artifact.artifactType, artifact.artifactName],
    queryFn: () => api.artifactVersions(token ?? "", artifact.artifactType, artifact.artifactName),
    enabled: Boolean(token),
  });
  const versionRows: ArtifactVersion[] = versions.data?.versions ?? [];
  const visibleVersion = versionRows.find((item) => item.version === selectedVersion) ?? versionRows[0] ?? null;

  useEffect(() => {
    setSelectedVersion(null);
  }, [artifact.artifactName, artifact.artifactType]);

  return (
    <aside className="code-workspace" style={{ width }}>
      <div className="code-resize-handle" onPointerDown={onResize} aria-label="Resize code workspace" role="separator" />
      <div className="code-workspace-header">
        <div className="code-title">
          <Code2 size={16} />
          <div>
            <div className="code-name">{artifact.artifactName}</div>
            <div className="code-path">{artifact.path ?? artifact.runtimePath ?? artifact.artifactType}</div>
          </div>
        </div>
        <button className="btn btn-ghost icon-btn" type="button" onClick={onClose} title="Закрыть код" aria-label="Закрыть код">
          <X size={15} />
        </button>
      </div>
      <div className="code-meta">
        <Badge status="ai">{artifactLabel(artifact.artifactType)}</Badge>
        {artifact.validationStatus && <Badge status={artifact.validationStatus === "valid" ? "success" : "error"}>{artifact.validationStatus}</Badge>}
        {artifact.deployedToRuntime && <Badge status="success">deployed</Badge>}
        {artifact.version && <span className="tag">v{artifact.version}</span>}
        {artifact.gitCommit && <span className="tag">{artifact.gitCommit}</span>}
      </div>
      <div className="artifact-versions">
        <div className="artifact-versions-head">
          <span>Git versions</span>
          {versions.isFetching && <span className="tag">loading</span>}
        </div>
        {versionRows.length > 0 ? (
          <>
            <div className="artifact-version-list">
              {versionRows.slice(0, 8).map((version) => (
                <button
                  className={`artifact-version ${visibleVersion?.version === version.version ? "active" : ""}`}
                  key={`${version.artifact_name}-${version.version}`}
                  type="button"
                  onClick={() => setSelectedVersion(version.version)}
                >
                  <span>v{version.version}</span>
                  <code>{version.git_commit_short_sha ?? version.git_status}</code>
                </button>
              ))}
            </div>
            {visibleVersion && (
              <details className="artifact-version-code">
                <summary>
                  v{visibleVersion.version} · {visibleVersion.git_commit_short_sha ?? visibleVersion.git_status} · {formatArtifactDate(visibleVersion.created_at)}
                </summary>
                <div className="artifact-version-message">{visibleVersion.message}</div>
                {visibleVersion.git_history.length > 0 && (
                  <div className="artifact-git-history">
                    {visibleVersion.git_history.slice(0, 5).map((commit) => (
                      <div className="artifact-git-commit" key={commit.commit_sha}>
                        <code>{commit.commit_short_sha}</code>
                        <span>{commit.message}</span>
                      </div>
                    ))}
                  </div>
                )}
                <pre>{visibleVersion.code || "Код версии недоступен."}</pre>
              </details>
            )}
          </>
        ) : (
          <div className="card-sub">Версии для этого артефакта пока не найдены.</div>
        )}
      </div>
      <div className="code-editor-shell">
        <MonacoEditor
          height="100%"
          language="python"
          loading={<div className="code-editor-loading">Загружаю редактор...</div>}
          options={{
            automaticLayout: true,
            fontFamily: "var(--font-mono)",
            fontSize: 12,
            lineNumbersMinChars: 3,
            minimap: { enabled: false },
            scrollBeyondLastLine: false,
            wordWrap: "on",
          }}
          theme="vs-dark"
          value={artifact.code}
          onChange={(value) => onChange(value ?? "")}
        />
      </div>
      <div className="code-workspace-footer">
        <button className="btn btn-ghost" type="button" onClick={onReset} disabled={!dirty}>
          <RotateCcw size={14} />
          Сбросить
        </button>
        <button className="btn btn-primary" type="button" onClick={onPrepareSave} disabled={!dirty}>
          <SendHorizonal size={14} />
          Вставить правку
        </button>
      </div>
    </aside>
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
        {activityMessages.length > 0 && <ActivityPanel messages={activityMessages} elapsedMs={message.elapsedMs} />}
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

function ActivityPanel({ messages, elapsedMs }: { messages: ChatMessage[]; elapsedMs?: number }) {
  const [now, setNow] = useState(Date.now());
  const toolCalls = messages.flatMap((message) => message.tools ?? []);
  const totalLatency = toolCalls.reduce((sum, tool) => sum + tool.latency_ms, 0);
  const startedCount = messages.filter((message) => message.intent === "tool-start").length;
  const finishedCount = messages.filter((message) => message.intent === "tool-result").length;
  const hasPending = startedCount > finishedCount;
  const firstStartedAt = messages
    .map((message) => message.startedAt)
    .filter((value): value is number => typeof value === "number")
    .sort((left, right) => left - right)[0];
  const displayLatency = hasPending && firstStartedAt ? now - firstStartedAt : elapsedMs ?? totalLatency;

  useEffect(() => {
    if (!hasPending) return;
    const timerId = window.setInterval(() => setNow(Date.now()), 500);
    return () => window.clearInterval(timerId);
  }, [hasPending]);

  return (
    <details className="activity-card">
      <summary className="activity-header">
        <ChevronRight size={14} className="activity-chevron" />
        {hasPending ? <Loader2 size={15} className="spin" /> : <CheckCircle2 size={15} />}
        <span>{hasPending ? "Думаю" : "Думал"}</span>
        {displayLatency > 0 && <span className="activity-time">на протяжении {formatLatency(displayLatency)}</span>}
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
    if (message.role === "assistant") {
      const activityMessages: ChatMessage[] = [];
      let nextIndex = index + 1;
      while (items[nextIndex]?.role === "tool") {
        activityMessages.push(items[nextIndex]);
        nextIndex += 1;
      }
      if (activityMessages.length > 0 || (message.tools?.length ?? 0) > 0) {
        grouped.push({
          type: "assistant_with_activity",
          message,
          activityMessages: activityMessages.length > 0 ? activityMessages : activityMessagesFromTools(message.tools ?? []),
          key: `assistant-activity-${index}`,
        });
        index = nextIndex;
        continue;
      }
    }

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

function isAbortError(error: unknown): boolean {
  return (
    (typeof DOMException !== "undefined" && error instanceof DOMException && error.name === "AbortError") ||
    (error instanceof Error && error.name === "AbortError")
  );
}

function markPendingActivityStopped(messages: ChatMessage[]): ChatMessage[] {
  const now = Date.now();
  return messages.map((message) => {
    if (message.role !== "tool" || message.intent !== "tool-start") return message;

    const toolName = toolNameFromStartMessage(message.content) ?? "AgentTool";
    const latencyMs = message.startedAt ? Math.max(0, now - message.startedAt) : 0;
    const input = isRecord(message.toolOutput) ? message.toolOutput : {};
    const stoppedTool: ToolCall = {
      tool_name: toolName,
      status: "stopped",
      input,
      output: { status: "stopped" },
      latency_ms: latencyMs,
    };

    return {
      ...message,
      content: `${toolName} остановлен пользователем`,
      intent: "tool-result",
      tools: [stoppedTool],
      toolOutput: stoppedTool.output,
    };
  });
}

function toolNameFromStartMessage(content: string): string | undefined {
  return content.match(/`([^`]+)`/)?.[1];
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
    elapsedMs: typeof metadata.elapsed_ms === "number" ? metadata.elapsed_ms : undefined,
  };
}

function activityMessagesFromTools(tools: ToolCall[]): ChatMessage[] {
  return tools.map((tool) => ({
    role: "tool",
    content: summarizeToolCall(tool),
    intent: "tool-result",
    tools: [tool],
    toolOutput: tool.output,
  }));
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
  if (typeof output.code === "string") {
    const artifactName = readString(output.artifact_name) ?? readString(output.path) ?? "артефакт";
    return `Открыт исходник ${artifactName}.`;
  }
  if (typeof output.artifact_name === "string" && typeof output.version === "number") {
    return `Артефакт ${output.artifact_name} сохранен, версия ${output.version}.`;
  }
  if (typeof output.run_id === "string") {
    return `DAG run: ${output.run_id}, статус ${String(output.status ?? tool.status)}.`;
  }
  if (typeof output.job_id === "string") {
    return `Spark job: ${output.job_id}, статус ${String(output.status ?? tool.status)}.`;
  }
  if (Array.isArray(output.runtime_command)) {
    return `Sandbox command: ${output.runtime_command.join(" ")}, статус ${String(output.runtime_status ?? tool.status)}.`;
  }
  if (typeof output.status === "string") {
    return `Статус: ${output.status}.`;
  }
  if (typeof output.command === "string" && typeof output.returncode === "number") {
    return `Git command: ${output.command}, код ${output.returncode}.`;
  }
  return "Результат получен и передан модели.";
}

function findLatestArtifact(messages: ChatMessage[]): CodeArtifact | null {
  for (let index = messages.length - 1; index >= 0; index -= 1) {
    const artifact = findLatestArtifactFromToolCalls(messages[index].tools ?? []);
    if (artifact) return artifact;
  }
  return null;
}

function findLatestArtifactFromToolCalls(toolCalls: ToolCall[]): CodeArtifact | null {
  for (let index = toolCalls.length - 1; index >= 0; index -= 1) {
    const artifact = artifactFromToolCall(toolCalls[index]);
    if (artifact) return artifact;
  }
  return null;
}

function artifactFromToolCall(toolCall: ToolCall): CodeArtifact | null {
  const output = toolCall.output;
  if (typeof output.code !== "string") return null;

  const path = readString(output.path);
  const runtimePath = readString(output.runtime_path);
  const artifactName = readString(output.artifact_name) ?? filenameFromPath(path ?? runtimePath) ?? "artifact.py";
  const artifactType = readString(output.artifact_type) ?? inferArtifactType(toolCall.tool_name, artifactName, path ?? runtimePath);

  return {
    artifactType,
    artifactName,
    code: output.code,
    originalCode: output.code,
    path,
    runtimePath,
    toolName: toolCall.tool_name,
    status: toolCall.status,
    validationStatus: readString(output.validation_status),
    version: typeof output.version === "number" ? output.version : undefined,
    gitCommit: readString(output.git_commit_short_sha),
    deployedToRuntime: output.deployed_to_runtime === true,
  };
}

function inferArtifactType(toolName: string, artifactName: string, path?: string): string {
  const source = `${toolName} ${artifactName} ${path ?? ""}`.toLowerCase();
  if (source.includes("spark")) return "spark_script";
  return "airflow_dag";
}

function artifactLabel(artifactType: string): string {
  if (artifactType === "spark_script") return "Spark script";
  if (artifactType === "airflow_dag") return "Airflow DAG";
  return artifactType;
}

function buildArtifactSavePrompt(artifact: CodeArtifact): string {
  const isSpark = artifact.artifactType === "spark_script";
  const toolName = isSpark ? "write_spark_script" : "write_airflow_dag";
  const artifactId = artifact.artifactName.replace(/\.py$/, "");
  const idField = isSpark ? "script_name" : "dag_id";
  const artifactKind = isSpark ? "Spark-скрипт" : "Airflow DAG";

  return [
    `Сохрани обновленный ${artifactKind} через function tool ${toolName}.`,
    `${idField}: ${artifactId}`,
    "После записи проверь код в Docker sandbox и кратко покажи результат.",
    "",
    "Код:",
    "```python",
    artifact.code,
    "```",
  ].join("\n");
}

function filenameFromPath(path?: string): string | undefined {
  if (!path) return undefined;
  return path.split("/").filter(Boolean).at(-1);
}

function readString(value: unknown): string | undefined {
  return typeof value === "string" && value.length > 0 ? value : undefined;
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

function formatArtifactDate(value?: string | null): string {
  if (!value) return "-";
  return new Date(value).toLocaleString("ru-RU");
}

function clamp(value: number, min: number, max: number) {
  return Math.min(Math.max(value, min), max);
}
