# Диаграммы и описание проекта

Папка содержит материалы для главы 3 диплома «AI Data Engineer Assistant».

## Состав

| Раздел | Файл | Формат | Инструмент |
| --- | --- | --- | --- |
| Описание дипломной работы | [`diploma_description.md`](./diploma_description.md) | Markdown | — |
| Портфолио проектов | [`projects_report.md`](./projects_report.md) | Markdown | — |
| 3.1 Схема физической реализации (ArchiMate) | [`3_1_physical_deployment_archimate.md`](./3_1_physical_deployment_archimate.md) + [`3_1_physical_architecture.archimate`](./3_1_physical_architecture.archimate) | Markdown + Archi XML | [Archi 5.x](https://www.archimatetool.com) |
| 3.2 Use Case | [`3_2_use_case.puml`](./3_2_use_case.puml) | PlantUML | PlantUML |
| 3.3 Sequence — ReAct loop агента | [`3_3_sequence_agent.puml`](./3_3_sequence_agent.puml) | PlantUML | PlantUML |
| 3.4 State machine — LangGraph узлы | [`3_4_state_agent.puml`](./3_4_state_agent.puml) | PlantUML | PlantUML |
| 3.5 ER-диаграмма БД | [`3_5_er_database.dbml`](./3_5_er_database.dbml) | DBML | [dbdiagram.io](https://dbdiagram.io/d) |
| 3.6 Component — backend internals | [`3_6_component_backend.puml`](./3_6_component_backend.puml) | PlantUML | PlantUML |

## Как открыть `.archimate`

1. Скачать Archi с https://www.archimatetool.com (бесплатно, кроссплатформенный).
2. `File → Open` → выбрать `3_1_physical_architecture.archimate`.
3. В Models → раскрыть модель → `Views` → «3.1 Схема физической реализации».
4. Экспорт для диплома: `Edit → Copy view as image (PNG)` или `File → Export → Image / PDF`.

Файл сгенерирован скриптом [`../scripts/generate_archimate_physical.py`](../scripts/generate_archimate_physical.py). При изменении состава docker-compose:

```bash
python3 scripts/generate_archimate_physical.py
```

## Как рендерить `.dbml` (ER-схема БД)

1. Открыть https://dbdiagram.io/d (бесплатно, без регистрации).
2. Вставить содержимое `3_5_er_database.dbml` в левое поле.
3. Схема построится сразу, FK подсветятся автоматически.
4. Экспорт: `Export → PNG / PDF / SQL DDL (PostgreSQL/MySQL/SQL Server) / MySQL Workbench`.

Альтернатива — CLI `dbml-cli` для batch-экспорта:

```bash
npm install -g @dbml/cli
dbml2sql docs/3_5_er_database.dbml -o docs/3_5_er_database.sql --postgres
```

## Как рендерить `.puml`

Любой из вариантов:

1. **Веб (без установки):** открыть https://plantuml.com → вставить содержимое файла → получить PNG/SVG.
2. **VS Code:** установить расширение `PlantUML` (jebbs), `Alt+D` для предпросмотра.
3. **IntelliJ IDEA / PyCharm:** встроенная поддержка PlantUML, открыть файл и нажать рендер.
4. **CLI (если установлен `plantuml`):**

   ```bash
   brew install plantuml          # macOS
   plantuml docs/*.puml           # генерирует PNG в той же папке
   plantuml -tsvg docs/*.puml     # SVG для масштабируемой вставки в Word
   ```

5. **Docker:**

   ```bash
   docker run --rm -v $PWD/docs:/data plantuml/plantuml -tsvg /data/*.puml
   ```

## Назначение каждой диаграммы

- **3.1 ArchiMate** — физическая архитектура: Docker-узлы, system software, артефакты, коммуникационные пути. Покрывает Technology + Physical Layer.
- **3.2 Use Case** — функциональные требования: какие сценарии доступны Data Engineer / Администратору, какие внешние акторы участвуют.
- **3.3 Sequence** — динамика: цикл `model → tool → observation` агента LangGraph, что происходит с момента POST /agent/query до ответа пользователю.
- **3.4 State machine** — внутренние состояния агента: узлы LangGraph (`select_runtime`, `call_model`, `execute_tools`, `finalize`, `configuration_error`) и переходы между ними.
- **3.5 ER (DBML)** — структура основной БД в формате DBML: 9 таблиц с FK, индексами, типами PostgreSQL и логическими группами. Открывается в dbdiagram.io. Источник — SQLAlchemy модели в `backend/app/models.py`.
- **3.6 Component** — внутреннее устройство FastAPI backend: routes → agent → tools → services → persistence, с указанием внешних интеграций (LLM, MCP, Airflow, Spark, sandbox).
