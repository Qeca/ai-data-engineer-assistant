from __future__ import annotations

import time
from typing import Any

from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession

from app.core.config import settings
from app.schemas import CatalogTable, TableColumn
from app.tools.base import ToolExecution


class CatalogTool:
    name = "CatalogTool"

    async def list_tables(self, db: AsyncSession) -> list[CatalogTable]:
        if settings.database_url.startswith("sqlite"):
            rows = await db.execute(
                text(
                    """
                    SELECT name
                    FROM sqlite_master
                    WHERE type = 'table'
                      AND name NOT LIKE 'sqlite_%'
                      AND name NOT LIKE 'alembic_%'
                    ORDER BY name
                    """
                )
            )
            tables: list[CatalogTable] = []
            for table_name in [row[0] for row in rows]:
                safe_name = table_name.replace('"', '""')
                column_rows = await db.execute(text(f'PRAGMA table_info("{safe_name}")'))
                columns = [
                    TableColumn(name=row[1], type=row[2] or "unknown", nullable=not bool(row[3]))
                    for row in column_rows
                ]
                tables.append(CatalogTable(name=table_name, columns=columns))
            return tables

        rows = await db.execute(
            text(
                """
                SELECT table_schema, table_name, column_name, data_type, is_nullable
                FROM information_schema.columns
                WHERE table_schema NOT IN ('pg_catalog', 'information_schema')
                ORDER BY table_schema, table_name, ordinal_position
                """
            )
        )
        grouped: dict[tuple[str, str], list[TableColumn]] = {}
        for row in rows:
            key = (row.table_schema, row.table_name)
            grouped.setdefault(key, []).append(
                TableColumn(
                    name=row.column_name,
                    type=row.data_type,
                    nullable=row.is_nullable == "YES",
                )
            )
        return [
            CatalogTable(schema_name=schema, name=name, columns=columns)
            for (schema, name), columns in grouped.items()
        ]

    async def execute(self, db: AsyncSession) -> ToolExecution:
        started = time.perf_counter()
        try:
            tables = await self.list_tables(db)
            return ToolExecution(
                tool_name=self.name,
                status="success",
                input={},
                output={"tables": [table.model_dump() for table in tables]},
                latency_ms=max(1, int((time.perf_counter() - started) * 1000)),
            )
        except Exception as exc:  # noqa: BLE001
            return ToolExecution(
                tool_name=self.name,
                status="error",
                input={},
                output={"error": str(exc)},
                latency_ms=max(1, int((time.perf_counter() - started) * 1000)),
                error=exc.__class__.__name__,
            )

    async def inspect_database(self, db: AsyncSession, sample_limit: int = 3) -> ToolExecution:
        started = time.perf_counter()
        try:
            tables = await self.list_tables(db)
            output_tables: list[dict[str, Any]] = []
            for table in tables:
                row_count = await self._row_count(db, table.schema_name, table.name)
                samples = await self._sample_rows(db, table.schema_name, table.name, sample_limit)
                output_tables.append(
                    {
                        "schema_name": table.schema_name,
                        "name": table.name,
                        "type": table.type,
                        "row_count": row_count,
                        "column_count": len(table.columns),
                        "columns": [column.model_dump() for column in table.columns],
                        "sample_rows": samples,
                    }
                )
            return ToolExecution(
                tool_name="DatabaseInspectorTool",
                status="success",
                input={"sample_limit": sample_limit},
                output={
                    "table_count": len(output_tables),
                    "tables": output_tables,
                },
                latency_ms=max(1, int((time.perf_counter() - started) * 1000)),
            )
        except Exception as exc:  # noqa: BLE001
            return ToolExecution(
                tool_name="DatabaseInspectorTool",
                status="error",
                input={"sample_limit": sample_limit},
                output={"error": str(exc)},
                latency_ms=max(1, int((time.perf_counter() - started) * 1000)),
                error=exc.__class__.__name__,
            )

    async def _row_count(self, db: AsyncSession, schema_name: str, table_name: str) -> int:
        result = await db.execute(text(f"SELECT COUNT(*) AS row_count FROM {self._table_ref(schema_name, table_name)}"))
        return int(result.scalar_one())

    async def _sample_rows(self, db: AsyncSession, schema_name: str, table_name: str, limit: int) -> list[dict[str, Any]]:
        if limit <= 0:
            return []
        result = await db.execute(text(f"SELECT * FROM {self._table_ref(schema_name, table_name)} LIMIT :limit"), {"limit": limit})
        return [dict(row._mapping) for row in result]

    def _table_ref(self, schema_name: str, table_name: str) -> str:
        quoted_table = self._quote_identifier(table_name)
        if settings.database_url.startswith("sqlite"):
            return quoted_table
        return f"{self._quote_identifier(schema_name)}.{quoted_table}"

    @staticmethod
    def _quote_identifier(value: str) -> str:
        return '"' + value.replace('"', '""') + '"'
