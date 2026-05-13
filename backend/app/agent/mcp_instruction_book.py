from dataclasses import dataclass


@dataclass(frozen=True)
class MCPProductLesson:
    product: str
    use_for: str
    examples: tuple[str, ...]
    fallback: str


class MCPInstructionBook:
    lessons: tuple[MCPProductLesson, ...] = (
        MCPProductLesson(
            product="database",
            use_for="PostgreSQL/database discovery, schemas, read-only SQL, row samples, counts.",
            examples=("что лежит в БД через MCP", "покажи schema/tables", "выполни read-only SQL"),
            fallback="inspect_database, list_catalog, execute_sql",
        ),
        MCPProductLesson(
            product="airflow",
            use_for="Airflow health, DAGs, runs, logs, variables, connections, pools and trigger actions.",
            examples=("покажи DAG runs через MCP", "запусти DAG через MCP", "проверь Airflow health/logs через MCP"),
            fallback="list_pipelines, manage_airflow_dags, trigger_airflow_dag, get_airflow_run",
        ),
        MCPProductLesson(
            product="spark",
            use_for="Spark SQL, PySpark version, catalog/database/table checks and query plans.",
            examples=("выполни Spark SQL через MCP", "покажи optimized plan", "проверь Spark catalog"),
            fallback="submit_spark_job, get_spark_job",
        ),
        MCPProductLesson(
            product="artifacts_git",
            use_for="Git status, diff, log, add and commit operations for the artifact repository.",
            examples=("покажи git diff артефактов", "покажи историю файла", "закоммить артефакт"),
            fallback="list_artifact_versions and artifact write tools",
        ),
        MCPProductLesson(
            product="artifacts_filesystem",
            use_for="Direct file read/list/search inside ARTIFACT_ROOT.",
            examples=("прочитай файл артефакта", "найди файл в workspace", "покажи директорию"),
            fallback="write_airflow_dag, write_spark_script and sandbox tools for user-owned writes",
        ),
    )

    def render(self) -> str:
        product_lines = " ".join(self._render_lesson(lesson) for lesson in self.lessons)
        return (
            "MCP PLAYBOOK. "
            "Внешние MCP-серверы используй только при явном запросе MCP или когда локальные product tools не покрывают задачу. "
            "Когда нужно работать с готовым внешним MCP-сервером, всегда действуй циклом discovery -> call -> validate. "
            "Если пользователь спрашивает какие MCP доступны, вызови list_mcp_products. "
            "Если продукт понятен, сначала вызови list_mcp_tools с product. "
            "После ответа list_mcp_tools выбери точный tool_name только из полученного списка и вызови call_mcp_tool. "
            "Никогда не выдумывай external MCP tool_name и не вызывай call_mcp_tool без discovery в текущем запросе, "
            "кроме случая, когда tool_name уже явно пришел от пользователя. "
            "arguments в call_mcp_tool должны соответствовать input_schema найденного tool; если schema пустая, передавай {}. "
            "Если внешний MCP tool вернул error, недоступен или не имеет нужной функции, используй fallback local tool и явно скажи, что был fallback. "
            "Для обычной навигации по сайту используй navigate_site, а не MCP. "
            "Для записи DAG/Spark-скриптов обычного пользователя предпочитай локальные artifact tools, потому что они enforce user scope, sandbox validation и Git versioning; "
            "filesystem MCP используй только для явного чтения/поиска/низкоуровневых файловых операций. "
            f"Product routing: {product_lines}"
        )

    @staticmethod
    def _render_lesson(lesson: MCPProductLesson) -> str:
        examples = "; ".join(lesson.examples)
        return (
            f"{lesson.product}: use for {lesson.use_for} "
            f"Examples: {examples}. "
            f"Fallback: {lesson.fallback}."
        )
