import re
import time
from typing import Any

from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession

from app.core.config import settings
from app.db.agent_data import agent_data_db
from app.tools.base import ToolExecution

WRITE_KEYWORDS = re.compile(
    r"\b(insert|update|delete|drop|alter|truncate|create|grant|revoke|merge|call|copy)\b",
    re.IGNORECASE,
)


class SQLTool:
    name = "SQLTool"

    async def execute(self, db: AsyncSession, query: str, limit: int = 100) -> ToolExecution:
        started = time.perf_counter()
        payload = {"query": query, "limit": limit}

        if not self._is_read_only(query):
            return ToolExecution(
                tool_name=self.name,
                status="error",
                input=payload,
                output={"error": "Only read-only SELECT/WITH/EXPLAIN queries are allowed"},
                latency_ms=self._elapsed_ms(started),
                error="read_only_violation",
            )

        try:
            async with agent_data_db.session(db) as data_db:
                result = await data_db.execute(text(query))
                rows = [dict(row._mapping) for row in result.fetchmany(limit)]
                columns = list(result.keys())
            return ToolExecution(
                tool_name=self.name,
                status="success",
                input=payload,
                output={"columns": columns, "rows": rows, "row_count": len(rows)},
                latency_ms=self._elapsed_ms(started),
            )
        except Exception as exc:  # noqa: BLE001
            return ToolExecution(
                tool_name=self.name,
                status="error",
                input=payload,
                output={"error": str(exc)},
                latency_ms=self._elapsed_ms(started),
                error=exc.__class__.__name__,
            )

    def anomaly_query(self) -> str:
        if agent_data_db.is_sqlite():
            return """
SELECT
  strftime('%Y-%m-%d %H:00:00', created_at) AS hour,
  COUNT(*) AS order_count,
  ROUND(AVG(total_amount), 2) AS avg_amount
FROM orders
WHERE created_at >= datetime('now', '-30 days')
GROUP BY 1
ORDER BY order_count DESC
LIMIT 10
""".strip()

        if settings.agent_database_schema == "sales":
            return """
SELECT
  date_trunc('hour', order_ts) AS hour,
  COUNT(*) AS order_count,
  ROUND(AVG(amount), 2) AS avg_amount
FROM orders
WHERE order_ts >= now() - interval '30 days'
GROUP BY 1
ORDER BY order_count DESC
LIMIT 10
""".strip()

        return """
SELECT
  date_trunc('hour', created_at) AS hour,
  COUNT(*) AS order_count,
  ROUND(AVG(total_amount), 2) AS avg_amount
FROM orders
WHERE created_at >= now() - interval '30 days'
GROUP BY 1
ORDER BY order_count DESC
LIMIT 10
""".strip()

    def revenue_query(self) -> str:
        if agent_data_db.is_sqlite():
            return """
SELECT
  date(created_at) AS day,
  ROUND(SUM(total_amount), 2) AS revenue,
  COUNT(*) AS orders
FROM orders
WHERE status = 'paid'
GROUP BY 1
ORDER BY day DESC
LIMIT 14
""".strip()
        if settings.agent_database_schema == "sales":
            return """
SELECT
  order_ts::date AS day,
  ROUND(SUM(amount), 2) AS revenue,
  COUNT(*) AS orders
FROM orders
WHERE status = 'paid'
GROUP BY 1
ORDER BY day DESC
LIMIT 14
""".strip()
        return """
SELECT
  created_at::date AS day,
  ROUND(SUM(total_amount), 2) AS revenue,
  COUNT(*) AS orders
FROM orders
WHERE status = 'paid'
GROUP BY 1
ORDER BY day DESC
LIMIT 14
""".strip()

    @staticmethod
    def _is_read_only(query: str) -> bool:
        stripped = re.sub(r"/\*.*?\*/|--.*?$", "", query, flags=re.MULTILINE | re.DOTALL).strip()
        if ";" in stripped.rstrip(";"):
            return False
        if WRITE_KEYWORDS.search(stripped):
            return False
        return stripped.lower().startswith(("select", "with", "explain"))

    @staticmethod
    def _elapsed_ms(started: float) -> int:
        return max(1, int((time.perf_counter() - started) * 1000))
