from __future__ import annotations

import shutil
import subprocess
from pathlib import Path
from uuid import uuid4

import pytest
from sqlalchemy import select

from app.agent.orchestrator import AgentOrchestrator
from app.agent.tool_registry import AgentToolRegistry
from app.core.config import settings
from app.db.session import AsyncSessionLocal, init_db
from app.models import User
from app.services.artifacts import ArtifactService
from app.services.git_versioning import GitArtifactVersioner


@pytest.mark.asyncio
async def test_artifact_service_writes_dag_version(monkeypatch, tmp_path):
    monkeypatch.setattr(settings, "artifact_root", str(tmp_path))
    await init_db()

    dag_id = f"unit_test_dag_{uuid4().hex}"
    code = """from __future__ import annotations

print("ok")
"""

    async with AsyncSessionLocal() as session:
        user = await session.scalar(select(User).where(User.email == "admin@local.dev"))
        service = ArtifactService()
        result = await service.write_airflow_dag(session, dag_id, code, "unit test", user.id)
        versions = await service.list_versions(session, "airflow_dag", f"{dag_id}.py")

    path = Path(result.path)
    assert path.exists()
    assert user.id in path.parts
    assert path.read_text(encoding="utf-8") == code
    assert result.version == 1
    assert result.validation_status == "valid"
    assert versions[0].artifact_name == f"{dag_id}.py"
    assert versions[0].git_status == result.git_status
    assert versions[0].git_commit_sha == result.git_commit_sha
    if shutil.which("git"):
        assert result.git_status == "committed"
        assert result.git_commit_sha


@pytest.mark.asyncio
async def test_registry_exposes_artifact_function_tools(monkeypatch, tmp_path):
    monkeypatch.setattr(settings, "artifact_root", str(tmp_path))
    monkeypatch.setattr(settings, "agent_debugger_url", None)
    await init_db()

    async with AsyncSessionLocal() as session:
        user = await session.scalar(select(User).where(User.email == "admin@local.dev"))
        registry = AgentToolRegistry(session, user, {"screen": "ai-agent"})
        names = {spec["name"] for spec in registry.specs()}

        assert {
            "inspect_database",
            "manage_airflow_dags",
            "list_mcp_products",
            "list_mcp_tools",
            "call_mcp_tool",
            "write_airflow_dag",
            "write_spark_script",
            "check_airflow_dag_sandbox",
            "run_spark_script_sandbox",
            "run_python_script_sandbox",
            "list_artifact_versions",
        }.issubset(names)
        mcp_specs = registry.mcp_specs()
        mcp_names = {spec["name"] for spec in mcp_specs}
        assert {"inspect_database", "manage_airflow_dags"}.issubset(mcp_names)
        assert all("inputSchema" in spec for spec in mcp_specs)
        product_specs = registry.mcp_product_specs()
        assert {"site", "database", "airflow", "spark", "artifacts", "external_mcp"}.issubset(product_specs)
        assert any(tool["name"] == "call_mcp_tool" for tool in product_specs["external_mcp"]["tools"])
        assert any(tool["name"] == "manage_airflow_dags" for tool in product_specs["airflow"]["tools"])
        assert any(tool["name"] == "inspect_database" for tool in product_specs["database"]["tools"])

        debug = await registry.execute(
            "run_python_script_sandbox",
            {"script_name": "broken.py", "code": "def broken(:\n    pass", "error_log": "SyntaxError", "arguments": []},
        )

    assert debug.tool_name == "PythonSandboxTool"
    assert debug.status == "error"
    assert debug.output["runtime_status"] == "sandbox_not_configured"


@pytest.mark.asyncio
async def test_registry_passes_user_context_to_sandbox(monkeypatch, tmp_path):
    monkeypatch.setattr(settings, "artifact_root", str(tmp_path))
    await init_db()

    captured = {}

    async def fake_run_artifact(**kwargs):
        captured.update(kwargs)
        return {
            "sandbox": "agent-debugger",
            "runtime_status": "success",
            "runtime_returncode": 0,
            "validation_status": "valid",
            "runtime_stdout": "ok\n",
            "runtime_stderr": "",
        }

    async with AsyncSessionLocal() as session:
        user = await session.scalar(select(User).where(User.email == "admin@local.dev"))
        registry = AgentToolRegistry(session, user, {"screen": "ai-agent"})
        monkeypatch.setattr(registry.debug_sandbox, "run_artifact", fake_run_artifact)
        result = await registry.execute(
            "run_python_script_sandbox",
            {"script_name": "ok.py", "code": "print('ok')", "error_log": "", "arguments": []},
        )

    assert result.status == "success"
    assert captured["user_context"] == {
        "id": user.id,
        "email": user.email,
        "role": user.role,
    }


@pytest.mark.asyncio
async def test_non_admin_lists_only_own_artifacts(monkeypatch, tmp_path):
    monkeypatch.setattr(settings, "artifact_root", str(tmp_path))
    await init_db()

    async with AsyncSessionLocal() as session:
        admin = await session.scalar(select(User).where(User.email == "admin@local.dev"))
        analyst = User(
            email=f"analyst_{uuid4().hex}@local.dev",
            full_name="Scoped Analyst",
            role="analyst",
            status="active",
            password_hash="x",
        )
        session.add(analyst)
        await session.commit()
        await session.refresh(analyst)

        service = ArtifactService()
        code = "from __future__ import annotations\n\nprint('ok')\n"
        await service.write_airflow_dag(session, "admin_only", code, "admin", admin.id)
        await service.write_airflow_dag(session, "analyst_only", code, "analyst", analyst.id)

        registry = AgentToolRegistry(session, analyst, {"screen": "ai-agent"})
        result = await registry.execute("list_artifact_versions", {"artifact_type": "all", "artifact_name": ""})

    names = {item["artifact_name"] for item in result.output["versions"]}
    assert result.output["scope"] == "own"
    assert "analyst_only.py" in names
    assert "admin_only.py" not in names


@pytest.mark.asyncio
async def test_artifact_versions_and_reads_are_user_scoped(monkeypatch, tmp_path):
    monkeypatch.setattr(settings, "artifact_root", str(tmp_path))
    await init_db()

    async with AsyncSessionLocal() as session:
        admin = await session.scalar(select(User).where(User.email == "admin@local.dev"))
        analyst = User(
            email=f"analyst_{uuid4().hex}@local.dev",
            full_name="Scoped Analyst",
            role="analyst",
            status="active",
            password_hash="x",
        )
        session.add(analyst)
        await session.commit()
        await session.refresh(analyst)

        service = ArtifactService()
        dag_id = f"same_name_{uuid4().hex}"
        code = "from __future__ import annotations\n\nprint('ok')\n"
        admin_result = await service.write_airflow_dag(session, dag_id, code, "admin", admin.id)
        analyst_result = await service.write_airflow_dag(session, dag_id, code, "analyst", analyst.id)
        await service.write_artifact(session, "airflow_dag", "legacy_global.py", code, "legacy", None)

        registry = AgentToolRegistry(session, analyst, {"screen": "ai-agent"})
        missing = await registry.execute(
            "check_airflow_dag_sandbox",
            {"dag_id": "legacy_global", "code": "", "error_log": ""},
        )

    assert admin_result.version == 1
    assert analyst_result.version == 1
    assert missing.tool_name == "AirflowSandboxTool"
    assert missing.status == "error"
    assert "not available for this user" in missing.output["error"]


@pytest.mark.skipif(shutil.which("git") is None, reason="git is required")
def test_git_versioner_initializes_configured_root_inside_parent_repo(tmp_path):
    project = tmp_path / "project"
    artifact_root = project / "infra"
    path = artifact_root / "users" / "user-1" / "airflow" / "dags" / "demo.py"
    path.parent.mkdir(parents=True)
    path.write_text("from __future__ import annotations\n\nprint('ok')\n", encoding="utf-8")

    subprocess.run(["git", "init"], cwd=project.parent, check=True, capture_output=True, text=True)
    versioner = GitArtifactVersioner(artifact_root, enabled=True)
    result = versioner.commit_file(path, "artifact commit")
    top_level = subprocess.run(
        ["git", "-C", str(artifact_root), "rev-parse", "--show-toplevel"],
        check=True,
        capture_output=True,
        text=True,
    )

    assert result.status == "committed"
    assert result.repository == str(artifact_root.resolve())
    assert top_level.stdout.strip() == str(artifact_root.resolve())


@pytest.mark.asyncio
async def test_local_agent_can_write_artifact(monkeypatch, tmp_path):
    monkeypatch.setattr(settings, "artifact_root", str(tmp_path))
    await init_db()

    agent = AgentOrchestrator()
    agent.openai.api_key = None
    agent.openrouter.api_key = None

    async with AsyncSessionLocal() as session:
        user = await session.scalar(select(User).where(User.email == "admin@local.dev"))
        result = await agent.run(session, "создай DAG orders_sync", user, {"screen": "ai-agent"})

    assert result.intent == "artifact"
    assert result.tool_calls[0].tool_name == "ArtifactTool"
    assert result.tool_calls[0].output["validation_status"] == "valid"
    assert result.tool_calls[1].tool_name == "AirflowSandboxTool"
    assert result.tool_calls[1].status == "error"
