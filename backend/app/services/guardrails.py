import re
from dataclasses import dataclass
from typing import Any


SECRET_KEY_PATTERN = re.compile(
    r"\b("
    r"api[_-]?key|token|secret|password|passwd|jwt[_-]?secret|"
    r"openai[_-]?api[_-]?key|openrouter[_-]?api[_-]?key|magnitgpt[_-]?api[_-]?key|"
    r"private[_-]?key|\.env|id_rsa"
    r")\b",
    re.IGNORECASE,
)

SQL_WRITE_PATTERN = re.compile(
    r"\b(insert|update|delete|drop|alter|truncate|create|grant|revoke|merge|copy|call)\b",
    re.IGNORECASE,
)


@dataclass(frozen=True)
class GuardrailDecision:
    allowed: bool
    category: str = ""
    reason: str = ""

    @classmethod
    def allow(cls) -> "GuardrailDecision":
        return cls(True)

    @classmethod
    def deny(cls, category: str, reason: str) -> "GuardrailDecision":
        return cls(False, category, reason)

    def answer(self) -> str:
        return (
            "Запрос заблокирован guardrails: "
            f"{self.reason}. Я могу помочь безопасной альтернативой: показать статус, "
            "прочитать логи, выполнить read-only SQL или подготовить код без выполнения опасного действия."
        )


class AgentGuardrails:
    destructive_text_patterns = [
        re.compile(r"\b(drop|truncate|delete|wipe|destroy|purge)\b.*\b(database|schema|table|db|бд|баз)", re.IGNORECASE),
        re.compile(r"\b(удали|сотри|снеси|очисти|дропни)\b.*\b(таблиц|баз|бд|данн|volume|контейнер)", re.IGNORECASE),
        re.compile(r"\b(disable|отключи)\b.*\b(auth|authentication|jwt|guardrail|guardrails|авторизац)", re.IGNORECASE),
        re.compile(r"\brm\s+-[a-zA-Z]*r[f]?\s+(/|\*|\.|~)", re.IGNORECASE),
        re.compile(r"\bdocker\s+(rm|rmi|kill|stop|compose\s+down)\b", re.IGNORECASE),
        re.compile(r"\b(killall|shutdown|reboot|mkfs|dd\s+if=|chmod\s+-R\s+777)\b", re.IGNORECASE),
    ]

    command_patterns = [
        re.compile(r"\brm\s+-[a-zA-Z]*r[f]?\s+(/|\*|\.|~)", re.IGNORECASE),
        re.compile(r"\bdocker\s+(rm|rmi|kill|stop|compose\s+down|system\s+prune)\b", re.IGNORECASE),
        re.compile(r"\b(killall|shutdown|reboot|mkfs|dd\s+if=|chmod\s+-R\s+777|chown\s+-R)\b", re.IGNORECASE),
        re.compile(r":\s*\(\s*\)\s*\{\s*:\s*\|\s*:", re.IGNORECASE),
        re.compile(r"\bcurl\b.*\|\s*(sh|bash|python)", re.IGNORECASE),
        re.compile(r"\bwget\b.*\|\s*(sh|bash|python)", re.IGNORECASE),
    ]

    def validate_user_query(self, query: str) -> GuardrailDecision:
        text = self._normalize(query)
        if not text:
            return GuardrailDecision.allow()

        if self._looks_like_secret_request(text):
            return GuardrailDecision.deny(
                "secret_request",
                "нельзя раскрывать ключи, токены, пароли, приватные ключи или содержимое .env",
            )

        for pattern in self.destructive_text_patterns:
            if pattern.search(text):
                return GuardrailDecision.deny(
                    "destructive_request",
                    "запрос похож на разрушительное действие с данными, контейнерами или настройками безопасности",
                )
        return GuardrailDecision.allow()

    def validate_tool_call(self, tool_name: str, args: dict[str, Any]) -> GuardrailDecision:
        if tool_name == "execute_sql":
            return self._validate_sql(str(args.get("query") or ""))

        if tool_name == "call_mcp_tool":
            return self._validate_mcp_tool(args)

        if tool_name == "run_bash_sandbox":
            return self._validate_command(str(args.get("command") or ""))

        if tool_name == "run_git_command":
            return self._validate_git_command(str(args.get("command") or ""))

        if tool_name in {
            "write_airflow_dag",
            "write_spark_script",
            "check_airflow_dag_sandbox",
            "run_spark_script_sandbox",
            "run_python_script_sandbox",
        }:
            return self._validate_code(str(args.get("code") or ""))

        if tool_name == "upsert_database_connection":
            return self._validate_database_connection(args)

        return GuardrailDecision.allow()

    def _validate_sql(self, query: str) -> GuardrailDecision:
        text = self._normalize(query)
        if SQL_WRITE_PATTERN.search(text):
            return GuardrailDecision.deny(
                "unsafe_sql",
                "разрешены только read-only SQL запросы без DDL/DML операций",
            )
        if SECRET_KEY_PATTERN.search(text):
            return GuardrailDecision.deny(
                "secret_request",
                "SQL-запрос не должен читать секреты или служебные настройки",
            )
        return GuardrailDecision.allow()

    def _validate_mcp_tool(self, args: dict[str, Any]) -> GuardrailDecision:
        if args.get("product") != "database":
            return GuardrailDecision.allow()

        arguments = args.get("arguments")
        if not isinstance(arguments, dict):
            return GuardrailDecision.allow()

        for key in ("sql", "query", "statement"):
            value = arguments.get(key)
            if isinstance(value, str):
                decision = self._validate_sql(value)
                if not decision.allowed:
                    return decision
        return GuardrailDecision.allow()

    def _validate_command(self, command: str) -> GuardrailDecision:
        text = self._normalize(command)
        if self._looks_like_secret_request(text):
            return GuardrailDecision.deny(
                "secret_request",
                "команда не должна читать ключи, токены, пароли, приватные ключи или .env",
            )
        for pattern in self.command_patterns:
            if pattern.search(text):
                return GuardrailDecision.deny(
                    "unsafe_command",
                    "команда выглядит разрушительной или запускает непроверенный удаленный код",
                )
        return GuardrailDecision.allow()

    def _validate_git_command(self, command: str) -> GuardrailDecision:
        text = self._normalize(command)
        if re.search(r"\bgit\s+(reset|clean|checkout|switch|push|pull|fetch)\b", text, re.IGNORECASE):
            return GuardrailDecision.deny(
                "unsafe_git",
                "через agent tool разрешены только локальные безопасные Git-команды без reset/clean/push/pull",
            )
        return GuardrailDecision.allow()

    def _validate_code(self, code: str) -> GuardrailDecision:
        text = self._normalize(code)
        if not text:
            return GuardrailDecision.allow()
        if self._looks_like_secret_request(text):
            return GuardrailDecision.deny(
                "secret_request",
                "код не должен читать или выводить секреты окружения",
            )
        for pattern in self.command_patterns:
            if pattern.search(text):
                return GuardrailDecision.deny(
                    "unsafe_code",
                    "код содержит потенциально разрушительную shell-команду",
                )
        return GuardrailDecision.allow()

    def _validate_database_connection(self, args: dict[str, Any]) -> GuardrailDecision:
        host = self._normalize(str(args.get("host") or ""))
        database = self._normalize(str(args.get("database") or ""))
        engine = self._normalize(str(args.get("engine") or ""))
        if engine == "postgresql" and host in {"postgres", "localhost", "127.0.0.1", "host.docker.internal"} and database == "ai_de":
            return GuardrailDecision.deny(
                "product_database_access",
                "агенту запрещено подключаться к служебной БД продукта",
            )
        return GuardrailDecision.allow()

    @staticmethod
    def _normalize(value: str) -> str:
        return " ".join(value.strip().split()).lower()

    @staticmethod
    def _looks_like_secret_request(text: str) -> bool:
        if not SECRET_KEY_PATTERN.search(text):
            return False
        return bool(
            re.search(
                r"\b(show|print|cat|read|dump|echo|env|export|покажи|выведи|прочитай|напечатай|скинь|открой)\b",
                text,
                re.IGNORECASE,
            )
        )
