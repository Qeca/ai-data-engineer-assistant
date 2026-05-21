from pathlib import Path
from typing import Any

from fastapi import APIRouter, Depends
from sqlalchemy.ext.asyncio import AsyncSession

from app.api.deps import get_current_user
from app.db.session import get_db
from app.models import User
from app.services.artifacts import ArtifactService

router = APIRouter(prefix="/artifacts", tags=["artifacts"])
service = ArtifactService()


@router.get("/versions")
async def list_artifact_versions(
    artifact_type: str = "all",
    artifact_name: str = "",
    user: User = Depends(get_current_user),
    db: AsyncSession = Depends(get_db),
) -> dict[str, Any]:
    selected_type = None if artifact_type == "all" else artifact_type
    selected_name = artifact_name or None
    author_user_id = None if user.role == "admin" else user.id
    versions = await service.list_versions(db, selected_type, selected_name, author_user_id)
    return {
        "scope": "all" if user.role == "admin" else "own",
        "versions": [serialize_version(item) for item in versions],
    }


def serialize_version(item) -> dict[str, Any]:
    return {
        "artifact_type": item.artifact_type,
        "artifact_name": item.artifact_name,
        "path": item.path,
        "version": item.version,
        "content_hash": item.content_hash,
        "message": item.message,
        "validation_status": item.validation_status,
        "validation_output": item.validation_output,
        "git_status": item.git_status,
        "git_repository": item.git_repository,
        "git_commit_sha": item.git_commit_sha,
        "git_commit_short_sha": item.git_commit_short_sha,
        "git_error": item.git_error,
        "created_at": item.created_at.isoformat() if item.created_at else None,
        "git_history": service.git_history_for_path(item.path, limit=10),
        "code": read_code(item.path),
    }


def read_code(path: str) -> str | None:
    try:
        return Path(path).read_text(encoding="utf-8")
    except OSError:
        return None
