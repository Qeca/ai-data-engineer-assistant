# Портфолио проектов

GitHub: https://github.com/Qeca

## Основные проекты

### AI Data Engineer Assistant

Репозиторий: https://github.com/Qeca/ai-data-engineer-assistant

Fullstack AI-приложение для data engineering workflows. Агент на LangGraph принимает запросы на естественном языке и выполняет действия через tools: read-only SQL, каталог данных, Airflow DAGs, Spark jobs, внешние MCP-интеграции, запись артефактов, Git-версии и Docker sandbox validation. Стек: Next.js, TypeScript, FastAPI, SQLAlchemy, LangGraph, OpenAI/OpenRouter tool calling, Docker Compose.

### ITMO Agent

Репозиторий: https://github.com/Qeca/itmo-agent

AI-агент для задач и сценариев вокруг ITMO. Проект уже оформлен README и выглядит как основной публичный агентский проект для портфолио.

### ITMO Small Multiagent

Репозиторий: https://github.com/Qeca/itmo_small_multiagent

Небольшой multi-agent framework с orchestrator, агентами и tool-интерфейсами. Есть CLI/Python/search/memory tools, FAISS-память, YAML prompts и примеры запуска. Хороший проект для демонстрации агентской архитектуры и tool orchestration.

### Time Series Forecasting

Репозиторий: https://github.com/Qeca/timeseries

ML/API-проект для прогнозирования временных рядов. Включает модели Informer/LSTM/Newsformer, FastAPI endpoint `POST /predict/{horizon}`, парсеры данных MOEX/CB/RBK и Telegram bot. Подходит для демонстрации ML engineering, API и data pipeline навыков.

### Telegram Events Bot

Репозиторий: https://github.com/Qeca/BOT

Telegram bot на aiogram для агрегации мероприятий. Есть роли student/admin, регистрация, список событий, запись на событие, подтверждение через геолокацию, admin-управление событиями/конкурсами и уведомления.

### Mediaprofi

Репозиторий: https://github.com/Qeca/mediaprofi

Frontend-прототип для SMM/контент-агентства: клиенты, публикации, статусы, календарь и планирование контента. Одностраничный HTML/CSS/JavaScript проект, полезен как лёгкий UI-пример.

## Дополнительные исследовательские проекты

### Triton / vLLM Embeddings Benchmark

Репозиторий: https://github.com/Qeca/test_triton

gRPC-сервис для сравнения скорости инференса эмбеддингов между vLLM и NVIDIA Triton на модели `intfloat/e5-multilingual-large`. В проекте есть proto/gRPC server, backend-переключение vLLM/Triton, конвертация модели под Triton, benchmark и autoscaling-эксперименты. Проект демонстрирует навыки ML infrastructure, serving и performance analysis.

### Karak

Репозиторий: https://github.com/Qeca/karak

Python/RAG/BI assistant: работа с документами, Chroma, CSV/SQL хранилища, smart QA, чат-панели и аналитические UI-компоненты. Проект показывает практический опыт разработки ассистентов для поиска, анализа и визуализации данных.

## Краткий список ссылок

- GitHub profile: https://github.com/Qeca
- AI Data Engineer Assistant: https://github.com/Qeca/ai-data-engineer-assistant
- ITMO Agent: https://github.com/Qeca/itmo-agent
- ITMO Small Multiagent: https://github.com/Qeca/itmo_small_multiagent
- Time Series Forecasting: https://github.com/Qeca/timeseries
- Telegram Events Bot: https://github.com/Qeca/BOT
- Mediaprofi: https://github.com/Qeca/mediaprofi
- Triton / vLLM Embeddings Benchmark: https://github.com/Qeca/test_triton
- Karak: https://github.com/Qeca/karak
