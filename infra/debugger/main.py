import py_compile
from dataclasses import dataclass
from pathlib import Path
from tempfile import mkdtemp

import docker
from docker.errors import DockerException, ImageNotFound
from fastapi import FastAPI
from pydantic import BaseModel, Field


app = FastAPI(title="Agent Debug Sandbox", version="0.1.0")


class DebugRequest(BaseModel):
    code: str
    artifact_type: str = "python"
    artifact_name: str = "artifact.py"
    error_log: str = ""
    arguments: list[str] = []
    timeout_seconds: int = 30
    memory_limit: str = "1g"
    user_id: str = "anonymous"
    user_email: str = ""
    user_role: str = ""


class BashRequest(BaseModel):
    command: str
    files: dict[str, str] = Field(default_factory=dict)
    timeout_seconds: int = 30
    memory_limit: str = "1g"
    image: str | None = None
    artifact_type: str = "bash"
    artifact_name: str = "command"
    user_id: str = "anonymous"
    user_email: str = ""
    user_role: str = ""


@dataclass
class RuntimePlan:
    image: str
    command: list[str]
    note: str


@app.get("/health")
def health() -> dict[str, str]:
    return {"status": "ok"}


@app.post("/debug/bash")
def debug_bash(payload: BashRequest) -> dict:
    run_dir = _new_run_dir(payload.user_id)
    for relative_path, content in payload.files.items():
        _write_workspace_file(run_dir, relative_path, content)

    plan = RuntimePlan(
        image=payload.image or _env("SANDBOX_BASH_IMAGE", "python:3.12-slim"),
        command=["/bin/bash", "-lc", payload.command],
        note="Runs a bash command in a Docker container.",
    )
    runtime = _run_in_docker(payload, plan, run_dir)
    return {
        "sandbox": "agent-debugger",
        "sandbox_scope": "user",
        "user_id": payload.user_id,
        "user_email": payload.user_email,
        "user_role": payload.user_role,
        "user_workspace": str(run_dir.host_path.parent),
        "run_workspace": str(run_dir.host_path),
        "artifact_type": payload.artifact_type,
        "artifact_name": payload.artifact_name,
        "runtime_image": plan.image,
        "runtime_command": plan.command,
        "runtime_note": plan.note,
        "runtime_returncode": runtime["returncode"],
        "runtime_status": runtime["runtime_status"],
        "runtime_stdout": runtime["stdout"][-8000:],
        "runtime_stderr": runtime["stderr"][-8000:],
        "files": sorted(payload.files.keys()),
    }


@app.post("/debug/python")
def debug_python(payload: DebugRequest) -> dict:
    run_dir = _new_run_dir(payload.user_id)
    script = run_dir.container_path / _safe_script_name(payload.artifact_name)
    script.write_text(payload.code, encoding="utf-8")

    syntax_status = "valid"
    syntax_output = "Python syntax is valid"
    try:
        py_compile.compile(str(script), doraise=True)
    except py_compile.PyCompileError as exc:
        syntax_status = "invalid"
        syntax_output = str(exc)

    plan = _runtime_plan(payload, script.name)
    runtime = _run_in_docker(payload, plan, run_dir)

    combined = "\n".join([syntax_output, runtime["stderr"], runtime["stdout"], payload.error_log]).lower()
    hints: list[str] = []
    if syntax_status == "invalid":
        hints.append("Исправить синтаксис Python перед сохранением или деплоем.")
    if runtime["runtime_status"] == "docker_error":
        hints.append("Docker sandbox недоступен или не смог запустить контейнер.")
    if "modulenotfounderror" in combined:
        hints.append("В sandbox не хватает зависимости; добавьте ее в debug image или целевое окружение.")
    if "dag" in combined and "airflow" in combined:
        hints.append("Проверить импорты DAG, dag_id, schedule, start_date и аргументы операторов.")
    if "spark" in combined and ("memory" in combined or "executor" in combined):
        hints.append("Настроить Spark executor memory, partitions или тяжелые shuffle-преобразования.")
    if not hints and syntax_status == "valid" and runtime["returncode"] == 0:
        hints.append("Синтаксис и sandbox smoke test прошли.")

    return {
        "sandbox": "agent-debugger",
        "sandbox_scope": "user",
        "user_id": payload.user_id,
        "user_email": payload.user_email,
        "user_role": payload.user_role,
        "user_workspace": str(run_dir.host_path.parent),
        "run_workspace": str(run_dir.host_path),
        "artifact_type": payload.artifact_type,
        "artifact_name": payload.artifact_name,
        "validation_status": syntax_status,
        "validation_output": syntax_output,
        "syntax_status": syntax_status,
        "syntax_output": syntax_output,
        "runtime_image": plan.image,
        "runtime_command": plan.command,
        "runtime_note": plan.note,
        "runtime_returncode": runtime["returncode"],
        "runtime_status": "success" if runtime["returncode"] == 0 and syntax_status == "valid" else runtime["runtime_status"],
        "runtime_stdout": runtime["stdout"][-4000:],
        "runtime_stderr": runtime["stderr"][-4000:],
        "hints": hints,
    }


@dataclass
class RunDirectory:
    host_path: Path
    container_path: Path


def _new_run_dir(user_id: str) -> RunDirectory:
    container_root = Path(_env("SANDBOX_CONTAINER_RUNS_DIR", "/sandbox-runs"))
    host_root = Path(_env("SANDBOX_HOST_RUNS_DIR", str(container_root)))
    user_slug = _safe_user_id(user_id)
    container_user_root = container_root / "users" / user_slug
    host_user_root = host_root / "users" / user_slug
    container_user_root.mkdir(parents=True, exist_ok=True)
    container_path = Path(mkdtemp(prefix="run-", dir=container_user_root))
    host_path = host_user_root / container_path.name
    return RunDirectory(host_path=host_path, container_path=container_path)


def _write_workspace_file(run_dir: RunDirectory, relative_path: str, content: str) -> None:
    path = Path(relative_path)
    if path.is_absolute() or ".." in path.parts:
        raise ValueError("Sandbox file paths must be relative and stay inside workspace")
    target = run_dir.container_path / path
    target.parent.mkdir(parents=True, exist_ok=True)
    target.write_text(content, encoding="utf-8")


def _runtime_plan(payload: DebugRequest, script_name: str) -> RuntimePlan:
    if payload.artifact_type == "airflow_dag":
        command = [
            "python",
            "-c",
            (
                "import importlib.util\n"
                f"spec = importlib.util.spec_from_file_location('agent_dag', '/workspace/{script_name}')\n"
                "module = importlib.util.module_from_spec(spec)\n"
                "assert spec and spec.loader\n"
                "spec.loader.exec_module(module)\n"
                "dags = [getattr(value, 'dag_id', None) for value in vars(module).values()]\n"
                "dags = [dag_id for dag_id in dags if dag_id]\n"
                "print('DAG_IMPORT_OK')\n"
                "print('DAG_IDS=' + ','.join(dags))\n"
            ),
        ]
        return RuntimePlan(
            image=_env("SANDBOX_AIRFLOW_IMAGE", "apache/airflow:2.10.4"),
            command=command,
            note="Imports DAG module and discovers dag_id values in a Docker container.",
        )

    if payload.artifact_type == "spark_script":
        return RuntimePlan(
            image=_env("SANDBOX_SPARK_IMAGE", "apache/spark:3.5.4"),
            command=["/opt/spark/bin/spark-submit", f"/workspace/{script_name}", *payload.arguments],
            note="Runs Spark script through spark-submit in a Docker container.",
        )

    return RuntimePlan(
        image=_env("SANDBOX_PYTHON_IMAGE", "python:3.12-slim"),
        command=["python", f"/workspace/{script_name}", *payload.arguments],
        note="Runs Python script in a Docker container.",
    )


def _run_in_docker(payload: DebugRequest | BashRequest, plan: RuntimePlan, run_dir: RunDirectory) -> dict:
    container = None
    try:
        client = docker.from_env()
        try:
            client.images.get(plan.image)
        except ImageNotFound:
            client.images.pull(plan.image)

        container = client.containers.run(
            image=plan.image,
            command=plan.command,
            detach=True,
            working_dir="/workspace",
            network_mode="none",
            mem_limit=payload.memory_limit,
            volumes={str(run_dir.host_path): {"bind": "/workspace", "mode": "rw"}},
            labels={
                "ai_de.sandbox": "true",
                "ai_de.user_id": payload.user_id,
                "ai_de.user_email": payload.user_email,
                "ai_de.user_role": payload.user_role,
                "ai_de.artifact_type": payload.artifact_type,
                "ai_de.artifact_name": payload.artifact_name,
            },
        )
        try:
            wait_result = container.wait(timeout=payload.timeout_seconds)
            return_code = int(wait_result.get("StatusCode", 1))
            runtime_status = "success" if return_code == 0 else "error"
        except Exception:
            container.kill()
            return_code = 124
            runtime_status = "timeout"

        logs = container.logs(stdout=True, stderr=True).decode("utf-8", errors="replace")
        return {"returncode": return_code, "runtime_status": runtime_status, "stdout": logs, "stderr": ""}
    except DockerException as exc:
        return {"returncode": 125, "runtime_status": "docker_error", "stdout": "", "stderr": str(exc)}
    finally:
        if container is not None:
            try:
                container.remove(force=True)
            except DockerException:
                pass


def _safe_script_name(value: str) -> str:
    cleaned = "".join(char if char.isalnum() or char in {"_", "-", "."} else "_" for char in value)
    if not cleaned.endswith(".py"):
        cleaned += ".py"
    return cleaned or "artifact.py"


def _safe_user_id(value: str) -> str:
    cleaned = "".join(char if char.isalnum() or char in {"_", "-"} else "_" for char in value)
    return cleaned[:96] or "anonymous"


def _env(name: str, default: str) -> str:
    import os

    return os.environ.get(name, default)
