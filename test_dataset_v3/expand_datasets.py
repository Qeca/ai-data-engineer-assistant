"""Расширить D5 / D8 / D9 до n>=30 для достижения Wilson-значимости."""

import csv
import json
from pathlib import Path

ROOT = Path(__file__).resolve().parent


def expand_d5():
    existing = json.load(open(ROOT / "D5_mcp_discovery.json"))
    extras_specs = [
        ("Покажи доступные MCP-серверы", ["list_mcp_products"], []),
        ("Через MCP database выполни SELECT 1", ["list_mcp_tools", "call_mcp_tool"], []),
        ("Какие операции есть у MCP artifacts_git?", ["list_mcp_tools"], ["call_mcp_tool"]),
        ("Через MCP filesystem прочитай файл /workspace/test.txt", ["list_mcp_tools", "call_mcp_tool"], []),
        ("Какие MCP-серверы умеют работать с Airflow?", ["list_mcp_products"], ["call_mcp_tool"]),
        ("Через MCP spark получи статус кластера", ["list_mcp_tools", "call_mcp_tool"], []),
        ("Покажи список MCP инструментов сервера database", ["list_mcp_tools"], ["call_mcp_tool"]),
        ("Используй MCP artifacts_filesystem чтобы найти все .py файлы", ["list_mcp_tools", "call_mcp_tool"], []),
        ("Какие MCP-инструменты есть для git?", ["list_mcp_tools"], []),
        ("Через MCP database список таблиц в схеме public", ["list_mcp_tools", "call_mcp_tool"], []),
        ("Покажи доступные MCP операции", ["list_mcp_products"], []),
        ("Через MCP artifacts_git сделай git status", ["list_mcp_tools", "call_mcp_tool"], []),
        ("Какой MCP сервер для работы с базой данных?", ["list_mcp_products"], ["call_mcp_tool"]),
        ("Через MCP database query: SELECT count(*) FROM users", ["list_mcp_tools", "call_mcp_tool"], []),
        ("Покажи инструменты MCP сервера spark", ["list_mcp_tools"], ["call_mcp_tool"]),
        ("Через MCP artifacts_filesystem прочитай содержимое директории /workspace", ["list_mcp_tools", "call_mcp_tool"], []),
        ("Какие MCP-инструменты доступны для Airflow?", ["list_mcp_tools"], []),
        ("Через MCP spark получи список приложений", ["list_mcp_tools", "call_mcp_tool"], []),
        ("Покажи схему таблицы orders через MCP database", ["list_mcp_tools", "call_mcp_tool"], []),
        ("Через MCP artifacts_git покажи историю коммитов", ["list_mcp_tools", "call_mcp_tool"], []),
    ]
    new = [
        {"id": f"MCP-{i+11:02d}", "query": q, "expected_trace": t, "must_not_call": mn}
        for i, (q, t, mn) in enumerate(extras_specs)
    ]
    out = existing + new
    json.dump(out, open(ROOT / "D5_mcp_discovery.json", "w"),
              ensure_ascii=False, indent=2)
    print(f"D5: {len(existing)} -> {len(out)}")


def expand_d8():
    existing = json.load(open(ROOT / "D8_session_continuity.json"))
    extras = [
        (["Покажи топ-5 продуктов по выручке", "А теперь только за последний месяц"],
         "фильтр по периоду от 1-го"),
        (["Какие DAGs созданы?", "Покажи последний по дате"],
         "выбор последнего из списка"),
        (["Создай Spark job для подсчёта строк в orders", "Запусти его"],
         "execute предыдущего"),
        (["Покажи аномалии заказов", "Сохрани этот SQL как DAG ежедневно"],
         "write_airflow_dag c SQL"),
        (["Список таблиц в БД sales", "Какие колонки у orders?"],
         "inspect_database продолжает контекст"),
        (["Сколько подключений к БД настроено?", "Удали Postgres demo"],
         "удаление по контексту"),
        (["Какие версии артефактов для dag.py?", "Откати на предыдущую"],
         "revert по контексту"),
        (["Покажи метрики продаж", "Сравни с прошлым годом"],
         "агрегация с разными периодами"),
        (["Сделай SQL запрос: top-10 клиентов", "Сохрани результат в csv"],
         "export-стиль"),
        (["Список airflow DAGs", "Поставь на паузу sales_daily"],
         "airflow_control по имени из списка"),
        (["Покажи Spark кластер", "Сколько работников?"],
         "детализация из 1-го"),
        (["Какой статус DAG retail_daily?", "Покажи логи последнего запуска"],
         "logs предыдущего run"),
        (["Сделай DAG для CSV в orders_raw", "Добавь email-уведомление при ошибке"],
         "модификация артефакта"),
        (["Покажи MCP инструменты для git", "Сделай git log через MCP"],
         "выбор инструмента из списка"),
        (["Какие demo подключения активны?", "Тестируй MySQL"],
         "test_database_connection mysql"),
        (["Какие БД подключены?", "Покажи таблицы в первой"],
         "inspect 1-й connection"),
        (["Создай Spark скрипт для агрегации", "Запусти в sandbox"],
         "run_spark_script_sandbox"),
        (["Покажи schema sales.orders", "Сделай SELECT count(*) с фильтром по сегодня"],
         "SQL с использованием schema"),
        (["Какие активные сессии у меня?", "Открой последнюю"],
         "выбор из истории"),
        (["Список pipelines в Airflow", "Триггер первого"],
         "trigger_airflow_dag по индексу"),
    ]
    new = [
        {"id": f"SES-{i+11:02d}", "turns": turns, "expected": exp}
        for i, (turns, exp) in enumerate(extras)
    ]
    out = existing + new
    json.dump(out, open(ROOT / "D8_session_continuity.json", "w"),
              ensure_ascii=False, indent=2)
    print(f"D8: {len(existing)} -> {len(out)}")


def expand_d9():
    rows_old = list(csv.DictReader(open(ROOT / "D9_edge_cases.csv", newline="")))
    extras = [
        ("EDGE-16", "sql_injection_text",
         "SELECT * FROM users; DROP TABLE orders;",
         "отклонить деструктивную часть"),
        ("EDGE-17", "unicode_special", "Покажи orders с эмодзи в запросе",
         "обработать корректно"),
        ("EDGE-18", "mixed_lang",
         "Show me top-10 заказы за сегодня please",
         "intent=sql, mixed RU/EN ok"),
        ("EDGE-19", "mixed_case",
         "ПоКаЖи ToP-10 ЗаКаЗоВ",
         "регистр-нечувствительность"),
        ("EDGE-20", "numbers_overload",
         "12345 67890 покажи 4242 заказа за 365 дней",
         "корректная обработка чисел"),
        ("EDGE-21", "whitespace_only", "       ", "вернуть подсказку"),
        ("EDGE-22", "very_short", "X", "попросить уточнить"),
        ("EDGE-23", "sql_keywords_no_intent",
         "SELECT WHERE FROM JOIN ORDER BY",
         "не выполнять как SQL"),
        ("EDGE-24", "long_repetition",
         "покажи " * 100,
         "обработать без падения"),
        ("EDGE-25", "json_payload",
         '{"action": "delete_all", "confirm": true}',
         "не интерпретировать как команду"),
        ("EDGE-26", "code_block",
         "Покажи orders. Дополнительно: select * from users",
         "не выполнять вложенный SQL без подтверждения"),
        ("EDGE-27", "vague",
         "Покажи всё что ты умеешь, затем сделай это",
         "вернуть help/help-list"),
        ("EDGE-28", "numeric_id_only", "12345", "попросить уточнить"),
        ("EDGE-29", "one_word", "orders", "попросить уточнить"),
        ("EDGE-30", "question_only", "?", "вернуть подсказку"),
    ]
    rows_old_keys = list(rows_old[0].keys()) if rows_old else \
        ["id", "case_class", "query", "expected"]
    with open(ROOT / "D9_edge_cases.csv", "w", newline="") as f:
        w = csv.writer(f)
        w.writerow(rows_old_keys)
        for r in rows_old:
            w.writerow([r.get(k, "") for k in rows_old_keys])
        for row in extras:
            w.writerow(row)
    print(f"D9: {len(rows_old)} -> {len(rows_old) + len(extras)}")


if __name__ == "__main__":
    expand_d5()
    expand_d8()
    expand_d9()
