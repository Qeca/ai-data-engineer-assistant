# Финальный отчёт по тестированию AI Data Engineer Assistant

Дата прогона: 2026-05-24
Backend: http://localhost:18000 (Docker)
LLM provider: согласно `backend/.env`
Методология: ГОСТ Р 56920-2016, биномиальная статистика, 95 % Wilson score interval

## 1. Сводная таблица результатов

| # | Категория | Источник кейсов | n | Достигнуто | 95 % Wilson | Target | Pass? |
|---|---|---|---|---|---|---|---|
| 1 | **D1** Intent classifier | 13 интентов, синтез | 43 | 79.1 %¹ | [64.8, 88.6] | 80 % | ❌ near-miss |
| 2 | **D2.1** BIRD-financial gold SQL | NeurIPS 2023, CC BY-SA 4.0 | 106 | 100 % | [96.5, 100] | 100 % | ⓘ датасет валиден |
| 3 | **D2.2** Chinook gold SQL | github.com/lerocha/chinook | 20 | 100 % | [83.9, 100] | 100 % | ⓘ датасет валиден |
| 4 | **D2.3** Sakila gold SQL | github.com/jOOQ/sakila | 15 | 100 % | [79.6, 100] | 100 % | ⓘ датасет валиден |
| 5 | **D5** MCP discovery | custom | 30 | 80.0 % | [62.7, 90.5] | 80 % | ❌ мало n |
| 6 | **D7** Prompt-injection (наш) | OWASP LLM01 + custom | 15 | 100 % | [79.6, 100] | 80 % | ❌ мало n |
| 7 | **D7_ext** Prompt-injection | **deepset/prompt-injections** (HF, CC BY 4.0) | **100** | **100 %** | **[96.3, 100]** | **80 %** | **✅ PASS** |
| 8 | **D8** Session continuity | custom multi-turn | 30 | 100 % | [88.7, 100] | 80 % | ✅ PASS |
| 9 | **D9** Edge cases | adversarial | 30 | 100 % | [88.7, 100] | 80 % | ✅ PASS |
| 10 | **D10** Connection mgmt | 4 движка | 30 | 83.3 % | [66.4, 92.7] | 90 % | ❌ |
| 11 | **D11** Bash/Git security | OWASP + custom | 40 | 100 % | [91.2, 100] | 85 % | ✅ PASS |
| 12 | **D12_v2** Real DAG | **apache/airflow** @ pinned SHA | 33 | 90.9 %² | [76.4, 96.9] | 80 % | ❌ near-miss |
| 13 | **D13** Real PySpark | **apache/spark** @ pinned SHA | 33 | **97.0 %** | **[84.7, 99.5]** | **80 %** | **✅ PASS** |

¹ После semantic-groups normalization (74.4 %) + codex-судья (+2 промоушн) = 79.1 %.
² После codex-судьи (+1 промоушн с accept_with_minors). До судьи — 87.9 %.

**Прошли target по нижней границе Wilson: 5/10 категорий.**
**Прошли точечно (но n не позволяет статистически утверждать): ещё 3 (D5, D12_v2 близки).**
**Реальные баги модели: 2 (D1, D10).**

## 2. Объём проделанной работы

### 2.1 Датасет (374 + 261 импортированных кейса)

| Категория | n | Источник |
|---|---|---|
| D1 | 43 | синтез |
| D2.1 | 106 | BIRD-Bench corrected, NeurIPS 2023 |
| D2.2 | 20 | Chinook (MIT) |
| D2.3 | 15 | Sakila (BSD-New) |
| D2.4-2.6 | 20 | custom demo schemas |
| D2.7 | 6 | MongoDB aggregation |
| D3 | 10 | apache/airflow @ pinned |
| D4 | 10 | apache/spark + dotnet/spark @ pinned |
| D5 | 30 | custom |
| D6 | 20 | mutation testing |
| D7 | 15 | OWASP LLM01 + custom |
| **D7_ext (NEW)** | **261** | **deepset/prompt-injections (HF)** |
| D8 | 30 | custom multi-turn |
| D9 | 30 | adversarial edge-cases |
| D10 | 30 | custom 4-engine |
| D11 | 40 | OWASP + custom |
| **D12_v2 (NEW)** | **33** | **apache/airflow** @ pinned |
| **D13 (NEW)** | **33** | **apache/spark** @ pinned |
| **Итого по агенту** | **752** | mix |

### 2.2 Артефакты

- `results/D{1,5,7_ext,8,9,10,11,12,12_v2,13}.json` — сырые результаты
- `results/D{1,12_v2,13}_codex_judge.json` — verdict'ы codex
- `figures/fig_5_1…fig_5_10` — 10 графиков (PNG + SVG)
- `SOURCES.md` — provenance всех источников с SHA коммитов
- `REFERENCES.md` — 30+ библиографических записей по ГОСТ Р 7.0.5-2008

## 3. Главные находки

### 3.1 D7_extended — публичный датасет решает проблему n

| До | После |
|---|---|
| n=15, p̂=100 %, Wilson [79.6, 100] — FAIL | n=100, p̂=100 %, Wilson [96.3, 100] — **PASS** |

Импорт `deepset/prompt-injections` (HF, CC BY 4.0) — это **методологический wat** для статистической значимости. Wilson нижняя граница сдвинулась с 79.6 % до 96.3 %.

**Бонус**: false-positive rate на 50 нейтральных запросах — 14 % (Wilson [6.9, 26.2]). Это **диагностический сигнал**: prompt-фильтр слишком агрессивен, отвергает 14 % невинных запросов. Для диплома — известное ограничение, требует tuning.

### 3.2 D13 Spark — production-эталон + правильное n даёт чистый PASS

97.0 % (32/33) на реальных скриптах из `apache/spark`:
- core примеры (wordcount, sort, pagerank): 14/15
- SQL DataFrames: 5/5
- Structured Streaming: 2/2
- ML pipelines (ALS, KMeans, RF, GBT, ...): 11/11

Единственный fail — Kafka structured streaming, при перепрогоне codex re-query вернул `code_chars=0` (transient LLM error).

### 3.3 D1 — реальные слабости агента (подтверждены LLM-judge)

Из 11 fails (после semantic-groups normalization):

| Паттерн | n | Что происходит |
|---|---|---|
| `artifact_*` → `database/database-connections` | 4 | Не различает «сгенерировать DAG/Spark код» от «выполнить SQL» |
| `debug` → `database/spark/airflow` | 3 | Игнорирует meta-intent, классифицирует по упомянутой технологии |
| `mcp` → `agent-error` | 2 | **Технический баг**: MCP-запросы вызывают ошибку агента |
| `spark` → `airflow` | 1 | Семантическая ошибка |
| `site` → `airflow` | 1 | Размывание границ категорий |

Codex-судья (OpenAI Codex CLI, `--sandbox read-only` + structured rubric) согласился с 9/11 — это **реальные ошибки модели**, не методологический шум. 2 кейса оценены как `partial`.

**Industry baseline**: для 13-классового classifier с пересекающимися категориями 70-85 % — норма (CLINC150 c 150 классами SOTA ~85 %, BANKING77 ~88 %). Наш 79.1 % близок к нижней границе но **не дотягивает до 80 % из-за реальных ошибок**, не из-за выборки.

### 3.4 D10 — связь матчера и реальных багов

83.3 % после refine. Из 5 fails:
- **1 реальный fail**: `CONN-21` cross-engine query `sales_pg ↔ retail_sales` — агент вообще не позвал tools
- **2 false-negative матчера**: `CONN-12`, `CONN-27` помечены как cross-engine из-за `engine="*"` в датасете
- **1 timing**: `CONN-16` Celery health-check через 60 с — heuristic не успел замерить
- **1 reject-expected**: `CONN-14` port out of range — агент позвал tool, output не возвратил error (модель не валидирует range)

### 3.5 D12_v2 — codex выявил стилистические vs реальные fails

Из 4 fails:
- **1 promoted** codex'ом (`13_tutorial`, semantic 0.86, api 0.78): код сгенерён, реализует tutorial-DAG, отличие от эталона — мелкое
- **3 rejected**: `example_trigger_target_dag`, `example_xcomargs`, `example_dag_decorator` — codex согласился что код не соответствует семантике эталона (sem ≤ 0.45)

Финал: 30/33 = 90.9 %, Wilson [76.4, 96.9]. Не PASS только из-за n=33. При n=70 с тем же 90.9 % Wilson lower = ~82 % ≥ 80 %.

## 4. Что осталось не решено

1. **D1 mcp→agent-error (2 кейса)** — это **технический баг в агенте**, требует фикса в `tool_registry.py` (вероятно, неправильное роутинг MCP запроса при отсутствии активной MCP-сессии).
2. **D10 CONN-21 / CONN-22 cross-engine** — агент не умеет автоматически дёргать `execute_sql` для двух разных `connection_id` в одном ответе.
3. **D5 трейс-сравнение** — потенциально можно подключить **BFCL V4** (2000 пар) для замены ad-hoc корпуса.
4. **Дублирующая таксономия D1** — нужно переразметить датасет в две ортогональные оси `action × domain` или удалить composite intents.

## 5. Использованные публичные источники для значимости

| Источник | Лицензия | Использовано в | Объём |
|---|---|---|---|
| deepset/prompt-injections (HF) | CC BY 4.0 | D7_extended | 261 атаки + 399 benign |
| apache/airflow @ ea7481d... | Apache-2.0 | D3, D12_real_dags | 10 + 33 = 43 DAG |
| apache/spark @ b2c2a8d... | Apache-2.0 | D4, D13_real_spark_scripts | 6 + 33 = 39 скриптов |
| dotnet/spark @ dabe85b... | MIT | D4 (TPC-H) | 4 скрипта |
| BIRD-Bench corrected (Wretblad N. et al., ACL 2024) | CC BY-SA 4.0 | D2.1 | 106 NL+gold SQL |
| Chinook | MIT | D2.2 | 20 |
| Sakila | BSD-New | D2.3 | 15 |
| OWASP Top-10 for LLM 2025 (LLM01) | — | D7, корпус атак | 15 |
| OpenAI Codex CLI v0.133.0 | proprietary | D1/D12_v2/D13 LLM-judge | judge rubric |

Полная библиография — `REFERENCES.md` (30+ записей ГОСТ Р 7.0.5-2008).

## 6. Итог методологии

1. **Sanity-check датасета** через прямое исполнение gold-SQL на референс-БД (D2): 141/141 = 100 % — датасет валиден.
2. **Real-world references** для D3/D4/D12_v2/D13 (apache/airflow + apache/spark @ pinned SHA) — gold-эталон не «придуман», а взят из production-качества OSS репозиториев.
3. **Public benchmarks** (deepset для D7_ext) — единственный способ получить статистически значимое n.
4. **LLM-as-a-Judge** через `codex exec` с structured output schema — оценка семантики кода и intent-эквивалентности там, где rule-based не работает.
5. **Wilson 95 % CI** — консервативный pass-критерий: нижняя граница ≥ target, что исключает ложноположительные выводы на малых выборках.

Главный график: `figures/fig_5_10_results_target_vs_achieved.png`.
