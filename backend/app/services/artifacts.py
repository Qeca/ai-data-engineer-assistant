import hashlib
import py_compile
import re
from dataclasses import dataclass
from pathlib import Path
from tempfile import NamedTemporaryFile

from sqlalchemy import func, select
from sqlalchemy.ext.asyncio import AsyncSession

from app.core.config import settings
from app.models import ArtifactVersion
from app.services.git_versioning import GitArtifactVersioner


@dataclass
class ArtifactWriteResult:
    artifact_type: str
    artifact_name: str
    path: str
    version: int
    content_hash: str
    validation_status: str
    validation_output: str
    git_status: str
    git_repository: str
    git_commit_sha: str | None = None
    git_commit_short_sha: str | None = None
    git_error: str | None = None


class ArtifactService:
    def __init__(self) -> None:
        self.root = Path(settings.artifact_root).resolve()
        git_root = Path(settings.artifact_git_root).resolve() if settings.artifact_git_root else self.root
        self.git = GitArtifactVersioner(git_root, settings.artifact_git_enabled)

    async def write_airflow_dag(
        self,
        db: AsyncSession,
        dag_id: str,
        code: str,
        message: str,
        author_user_id: str | None,
    ) -> ArtifactWriteResult:
        filename = f"{self._safe_name(dag_id)}.py"
        return await self.write_artifact(db, "airflow_dag", filename, code, message, author_user_id)

    async def write_spark_script(
        self,
        db: AsyncSession,
        script_name: str,
        code: str,
        message: str,
        author_user_id: str | None,
    ) -> ArtifactWriteResult:
        filename = self._safe_name(script_name)
        if not filename.endswith(".py"):
            filename += ".py"
        return await self.write_artifact(db, "spark_script", filename, code, message, author_user_id)

    async def write_artifact(
        self,
        db: AsyncSession,
        artifact_type: str,
        artifact_name: str,
        code: str,
        message: str,
        author_user_id: str | None,
    ) -> ArtifactWriteResult:
        validation_status, validation_output = self.validate_python(code)
        path = self.path_for(artifact_type, artifact_name, author_user_id)
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(code, encoding="utf-8")

        latest_version = await db.scalar(
            select(func.max(ArtifactVersion.version)).where(
                ArtifactVersion.artifact_type == artifact_type,
                ArtifactVersion.artifact_name == artifact_name,
                ArtifactVersion.author_user_id == author_user_id,
            )
        )
        version = int(latest_version or 0) + 1
        content_hash = hashlib.sha256(code.encode("utf-8")).hexdigest()
        git_result = self.git.commit_file(path, f"{artifact_type}: {artifact_name} v{version} - {message}")
        db.add(
            ArtifactVersion(
                artifact_type=artifact_type,
                artifact_name=artifact_name,
                path=str(path),
                version=version,
                content_hash=content_hash,
                message=message,
                author_user_id=author_user_id,
                validation_status=validation_status,
                validation_output=validation_output,
                git_status=git_result.status,
                git_repository=git_result.repository,
                git_commit_sha=git_result.commit_sha,
                git_commit_short_sha=git_result.commit_short_sha,
                git_error=git_result.error,
            )
        )
        await db.commit()

        return ArtifactWriteResult(
            artifact_type=artifact_type,
            artifact_name=artifact_name,
            path=str(path),
            version=version,
            content_hash=content_hash,
            validation_status=validation_status,
            validation_output=validation_output,
            git_status=git_result.status,
            git_repository=git_result.repository,
            git_commit_sha=git_result.commit_sha,
            git_commit_short_sha=git_result.commit_short_sha,
            git_error=git_result.error,
        )

    async def list_versions(
        self,
        db: AsyncSession,
        artifact_type: str | None = None,
        artifact_name: str | None = None,
        author_user_id: str | None = None,
    ) -> list[ArtifactVersion]:
        query = select(ArtifactVersion).order_by(ArtifactVersion.created_at.desc())
        if artifact_type:
            query = query.where(ArtifactVersion.artifact_type == artifact_type)
        if artifact_name:
            query = query.where(ArtifactVersion.artifact_name == artifact_name)
        if author_user_id:
            query = query.where(ArtifactVersion.author_user_id == author_user_id)
        rows = await db.scalars(query.limit(50))
        return list(rows)

    def debug_python(self, code: str, error_log: str | None = None) -> dict:
        status, output = self.validate_python(code)
        hints: list[str] = []
        combined = f"{output}\n{error_log or ''}".lower()
        if "syntaxerror" in combined:
            hints.append("Исправить синтаксис Python перед деплоем.")
        if "dag" in combined and "airflow" in combined:
            hints.append("Проверить dag_id, schedule, start_date и импорт операторов Airflow.")
        if "module not found" in combined or "modulenotfounderror" in combined:
            hints.append("Проверить зависимости контейнера Airflow/Spark.")
        if "spark" in combined and "memory" in combined:
            hints.append("Увеличить executor_memory или уменьшить shuffle/partitions.")
        if not hints and status == "valid":
            hints.append("Синтаксис корректен. Следующий шаг: прогнать runtime smoke test.")

        return {"validation_status": status, "validation_output": output, "hints": hints}

    def path_for(self, artifact_type: str, artifact_name: str, author_user_id: str | None = None) -> Path:
        root = self.root / "users" / self._safe_name(author_user_id) if author_user_id else self.root
        if artifact_type == "airflow_dag":
            return root / "airflow" / "dags" / artifact_name
        if artifact_type == "spark_script":
            return root / "spark" / "jobs" / artifact_name
        raise ValueError(f"Unsupported artifact type: {artifact_type}")

    def runtime_path_for(self, artifact_type: str, artifact_name: str) -> Path:
        if artifact_type == "airflow_dag":
            return self.root / "airflow" / "dags" / artifact_name
        if artifact_type == "spark_script":
            return self.root / "spark" / "jobs" / artifact_name
        raise ValueError(f"Unsupported runtime artifact type: {artifact_type}")

    def read_runtime_code(self, artifact_type: str, artifact_name: str) -> tuple[Path, str]:
        path = self.runtime_path_for(artifact_type, artifact_name)
        return path, path.read_text(encoding="utf-8")

    def deploy_runtime_copy(
        self,
        artifact_type: str,
        artifact_name: str,
        code: str,
        message: str,
    ) -> dict[str, str | None]:
        path = self.runtime_path_for(artifact_type, artifact_name)
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(code, encoding="utf-8")
        git_result = self.git.commit_file(path, f"runtime deploy: {artifact_type}: {artifact_name} - {message}")
        return {
            "runtime_path": str(path),
            "runtime_git_status": git_result.status,
            "runtime_git_repository": git_result.repository,
            "runtime_git_commit_sha": git_result.commit_sha,
            "runtime_git_commit_short_sha": git_result.commit_short_sha,
            "runtime_git_error": git_result.error,
        }

    def read_code(
        self,
        artifact_type: str,
        artifact_name: str,
        author_user_id: str | None,
        allow_global_fallback: bool = False,
    ) -> str:
        path = self.path_for(artifact_type, artifact_name, author_user_id)
        if path.exists():
            return path.read_text(encoding="utf-8")
        if not allow_global_fallback:
            raise FileNotFoundError(f"Artifact {artifact_type}/{artifact_name} is not available for this user")
        return self.path_for(artifact_type, artifact_name).read_text(encoding="utf-8")

    def git_history_for_path(self, path: str, limit: int = 20) -> list[dict[str, str]]:
        return self.git.history(Path(path), limit)

    @staticmethod
    def validate_python(code: str) -> tuple[str, str]:
        with NamedTemporaryFile("w", suffix=".py", encoding="utf-8", delete=True) as temp:
            temp.write(code)
            temp.flush()
            try:
                py_compile.compile(temp.name, doraise=True)
                return "valid", "Python syntax is valid"
            except py_compile.PyCompileError as exc:
                return "invalid", str(exc)

    @staticmethod
    def _safe_name(value: str) -> str:
        name = re.sub(r"[^a-zA-Z0-9_.-]+", "_", value.strip())
        name = name.strip("._")
        if not name:
            raise ValueError("Artifact name is empty")
        return name
