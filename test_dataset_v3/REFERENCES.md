# Список использованных источников (раздел 5 ПЗ)

Оформление — ГОСТ Р 7.0.5-2008 «Библиографическая ссылка. Общие требования
и правила составления». Источники сгруппированы по типу.

---

## I. Нормативные документы и стандарты

1. **ГОСТ Р 56920-2016** (ISO/IEC/IEEE 29119-1:2013). Системная и
   программная инженерия. Тестирование программного обеспечения. Часть 1.
   Понятия и определения. — Москва : Стандартинформ, 2016. — 60 с.

2. **ГОСТ Р 56921-2016** (ISO/IEC/IEEE 29119-2:2013). Системная и
   программная инженерия. Тестирование программного обеспечения. Часть 2.
   Процессы тестирования. — Москва : Стандартинформ, 2016. — 64 с.

3. **ГОСТ Р 56922-2016** (ISO/IEC/IEEE 29119-3:2013). Системная и
   программная инженерия. Тестирование программного обеспечения. Часть 3.
   Документация тестирования. — Москва : Стандартинформ, 2016. — 138 с.
   *(основной стандарт для оформления тест-плана и Test Report)*

---

## II. Академические публикации (бенчмарки и методы оценки LLM)

4. **BIRD-Bench**. *Li J., Hui B., Qu G. [et al.]* Can LLM already
   serve as a database interface? A big bench for large-scale database
   grounded text-to-SQLs // Advances in Neural Information Processing
   Systems (NeurIPS) 2023. — Vol. 36. — URL:
   https://proceedings.neurips.cc/paper_files/paper/2023/hash/83fc8fab1710363050bbd1d4b8cc0021-Abstract-Datasets_and_Benchmarks.html
   (дата обращения: 24.05.2026). — Лицензия: CC BY-SA 4.0.

5. **BIRD financial corrected**. *Wretblad N., Riseby F., Biswas R. [et al.]*
   Understanding the effects of noise in text-to-SQL: an examination of
   the BIRD-Bench benchmark // Proceedings of the 62nd Annual Meeting of
   the Association for Computational Linguistics (ACL). — Bangkok :
   ACL, 2024. — URL: https://github.com/niklaswretblad/the-effects-of-noise-in-text-to-SQL
   (дата обращения: 24.05.2026).

6. **MT-Bench (LLM-as-a-Judge)**. *Zheng L., Chiang W.-L., Sheng Y. [et al.]*
   Judging LLM-as-a-Judge with MT-Bench and Chatbot Arena //
   Advances in Neural Information Processing Systems (NeurIPS) 2023.
   Datasets and Benchmarks Track. — URL:
   https://proceedings.neurips.cc/paper_files/paper/2023/hash/91f18a1287b398d378ef22505bf41832-Abstract-Datasets_and_Benchmarks.html
   (дата обращения: 24.05.2026).

7. **G-Eval**. *Liu Y., Iter D., Xu Y. [et al.]* G-Eval: NLG evaluation
   using GPT-4 with better human alignment // Proceedings of the 2023
   Conference on Empirical Methods in Natural Language Processing (EMNLP).
   — Stroudsburg, PA : ACL, 2023. — P. 2511–2522. — URL:
   https://aclanthology.org/2023.emnlp-main.153/
   (дата обращения: 24.05.2026).

8. **Statistical Power**. *Cohen J.* Statistical power analysis for the
   behavioral sciences. — 2nd ed. — Hillsdale, NJ : Lawrence Erlbaum
   Associates, 1988. — 567 p. — ISBN 0-8058-0283-5.
   *(источник минимальной мощности 1 − β = 0,80)*

9. **Wilson score interval**. *Wilson E. B.* Probable inference, the law
   of succession, and statistical inference // Journal of the American
   Statistical Association. — 1927. — Vol. 22, No. 158. — P. 209–212.

10. **Cochran sample size**. *Cochran W. G.* Sampling techniques. — 3rd
    ed. — New York : John Wiley & Sons, 1977. — 428 p. — ISBN 0-471-16240-X.
    *(формула (5.1) расчёта минимального размера выборки)*

---

## III. Открытые тестовые наборы данных (бенчмарки)

11. **Chinook Database**. *Rocha L.* Sample database for SQL Server, Oracle,
    MySQL, PostgreSQL, SQLite, DB2. — URL:
    https://github.com/lerocha/chinook-database
    (commit `7f67772503d71ba90f19283c38e93923addb43fa`, дата обращения:
    24.05.2026). — Лицензия: MIT.

12. **Sakila Sample Database**. — MySQL AB ; репозиторий jOOQ. — URL:
    https://github.com/jOOQ/sakila
    (commit `e089a5b1ec9af0df7a9c6a5d47d49fa1736a4e84`, дата обращения:
    24.05.2026). — Лицензия: BSD-New.

13. **TPC-H Decision Support Benchmark Specification**. Rev. 3.0.1. —
    Transaction Processing Performance Council, 2022. — URL:
    https://www.tpc.org/tpch/
    (дата обращения: 24.05.2026).

---

## IV. Открытое программное обеспечение (источники эталонных артефактов)

14. **Apache Airflow** (apache/airflow). — Apache Software Foundation. —
    Источник эталонных DAG категории D3 (commit
    `ea7481d7d59b0eb129f8b39c848a24aa111e7ca3`). — URL:
    https://github.com/apache/airflow
    (дата обращения: 24.05.2026). — Лицензия: Apache-2.0.

15. **Apache Spark** (apache/spark). — Apache Software Foundation. —
    Источник эталонных PySpark-примеров категории D4 (commit
    `b2c2a8d68dcbbaca715adc74c0dd543582c9ff02`). — URL:
    https://github.com/apache/spark
    (дата обращения: 24.05.2026). — Лицензия: Apache-2.0.

16. **.NET for Apache Spark** (dotnet/spark). — Microsoft Corporation /
    .NET Foundation. — Источник TPC-H benchmark PySpark-кода категории
    D4 (commit `dabe85b685886901da9707f728da1974a33d44e7`). — URL:
    https://github.com/dotnet/spark
    (дата обращения: 24.05.2026). — Лицензия: MIT.

---

## V. Безопасность LLM-приложений

17. **OWASP Top-10 for Large Language Model Applications, 2025**.
    OWASP Foundation. — URL:
    https://owasp.org/www-project-top-10-for-large-language-model-applications/
    (дата обращения: 24.05.2026).
    *(категория LLM01 — Prompt Injection — используется в корпусе D7)*

17a. **Prompt Injections Dataset** : binary classification corpus of 662
    normal/injection prompts (label 1 = injection, label 0 = benign). —
    deepset GmbH ; HuggingFace `deepset/prompt-injections`. — URL:
    https://huggingface.co/datasets/deepset/prompt-injections
    (дата обращения: 24.05.2026). — Лицензия: CC BY 4.0.
    *(импортирован для D7_extended; 261 атаки + 399 benign)*

---

## V-A. Дополнительные публичные датасеты (для дальнейшего расширения)

17b. **BIRD-Bench dev set** (полный, 1534 вопроса по 95 БД). Может быть
     подключён для значимого расширения D2.1 за пределы текущих 106 кейсов. —
     URL: https://bird-bench.github.io
     (дата обращения: 24.05.2026). — Лицензия: CC BY-SA 4.0.

17c. **Spider 2.0** : Real-world enterprise text-to-SQL workflows
     (600 задач, BigQuery / Snowflake / PostgreSQL диалекты). — Yale NLP
     Group. — URL: https://spider2-sql.github.io/
     (дата обращения: 24.05.2026).
     *(потенциальное расширение D2 на enterprise-сложность)*

17d. **Berkeley Function Calling Leaderboard (BFCL) V4** : 2000+ пар
     (вопрос — определение функции — эталонный вызов) для оценки tool
     calling, AST-based metric. — Gorilla LLM, UC Berkeley. — URL:
     https://gorilla.cs.berkeley.edu/leaderboard.html ;
     dataset: https://huggingface.co/datasets/gorilla-llm/Berkeley-Function-Calling-Leaderboard
     (дата обращения: 24.05.2026). — Лицензия: Apache-2.0.
     *(потенциальное расширение D5 — MCP discovery / tool selection)*

17e. **LLMail-Inject** : adaptive prompt injection challenge dataset,
     Phase 2 — 3 688 confirmed injections. — Microsoft Research. — URL:
     https://huggingface.co/datasets/microsoft/llmail-inject-challenge
     (дата обращения: 24.05.2026).
     *(альтернативный корпус для D7 при необходимости массового расширения)*

17f. **CLINC150** : 150 intents, 23 700 запросов, в т.ч. out-of-scope. —
     UCI Machine Learning Repository. — URL:
     https://archive.ics.uci.edu/ml/datasets/CLINC150
     (дата обращения: 24.05.2026). — Лицензия: CC BY 4.0.
     *(альтернативный корпус для D1 / D9, требует адаптации intents)*

17g. **BANKING77** : 13 083 банковских запросов, 77 fine-grained intents. —
     HuggingFace `legacy-datasets/banking77`. — URL:
     https://huggingface.co/datasets/legacy-datasets/banking77
     (дата обращения: 24.05.2026).
     *(domain-specific, для intent-классификатора)*

## VI. Инструменты тестирования и среды

18. **pytest 8.x** : a mature full-featured Python testing tool. — URL:
    https://docs.pytest.org/en/8.x/
    (дата обращения: 24.05.2026). — Лицензия: MIT.

19. **pytest-asyncio**. Pytest support for asyncio. — URL:
    https://pytest-asyncio.readthedocs.io/
    (дата обращения: 24.05.2026). — Лицензия: Apache-2.0.

20. **testcontainers-python**. Lightweight, throwaway instances of
    common databases, message brokers etc. running inside Docker
    containers. — URL: https://testcontainers-python.readthedocs.io/
    (дата обращения: 24.05.2026). — Лицензия: Apache-2.0.

21. **Locust 2.x** : an open source performance/load testing tool. —
    URL: https://locust.io/
    (дата обращения: 24.05.2026). — Лицензия: MIT.

22. **OpenAI Codex CLI**. Official Codex command-line interface for
    automated code review. — URL: https://github.com/openai/codex
    (дата обращения: 24.05.2026).

---

## VII. Внутренние артефакты тестирования

23. **Test Dataset v3** (настоящий датасет). — Шаронов Д. В., 2026. —
    Каталог `test_dataset_v3/` в репозитории ВКР. — Источники и
    SHA-256 commit'ов перечислены в `SOURCES.md`.

24. **Charts and statistics** для раздела 5 ПЗ. — Сгенерированы скриптом
    `test_dataset_v3/generate_charts.py`. — Артефакты в каталоге
    `test_dataset_v3/figures/`: 9 рисунков в форматах PNG и SVG;
    численная сводка в `stats.json`.

---

## Соответствие цитирований тексту ПЗ

| Утверждение в разделе 5 | Источник |
| --- | --- |
| «уровни тестирования согласно ГОСТ Р 56920-2016» | [1, 2, 3] |
| «Сохранён 6-й уровень Acceptance Testing» | [3, раздел 7] |
| «BIRD-Bench Execution Accuracy», «SOTA окт. 2025 ≈ 0,574» | [4, 5] |
| «LLM-as-a-Judge MT-Bench» | [6] |
| «G-Eval EMNLP 2023» | [7] |
| «мощность 1 − β = 0,80 минимум Cohen 1988» | [8] |
| «Wilson score interval» (формула (5.2)) | [9] |
| Формула Кохрана (5.1) | [10] |
| Chinook | [11] |
| Sakila | [12] |
| TPC-H benchmark SF=5 | [13] |
| Эталонные DAG категории D3 | [14] |
| Эталонные PySpark-скрипты | [15, 16] |
| OWASP LLM01 — корпус D7 | [17] |
| pytest, pytest-asyncio | [18, 19] |
| testcontainers | [20] |
| Locust 2.x | [21] |
| Codex CLI как LLM-судья (M-07, M-08) | [22] |
