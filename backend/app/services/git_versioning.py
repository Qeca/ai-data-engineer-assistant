import subprocess
from dataclasses import dataclass
from pathlib import Path


@dataclass
class GitCommitResult:
    status: str
    repository: str
    commit_sha: str | None = None
    commit_short_sha: str | None = None
    message: str = ""
    error: str | None = None


class GitArtifactVersioner:
    def __init__(self, root: Path, enabled: bool = True) -> None:
        self.root = root.resolve()
        self.enabled = enabled

    def commit_file(self, path: Path, message: str) -> GitCommitResult:
        if not self.enabled:
            return GitCommitResult(status="disabled", repository=str(self.root))

        try:
            self.root.mkdir(parents=True, exist_ok=True)
            self._ensure_repo()
            relative_path = path.resolve().relative_to(self.root)
            self._run("add", "--", str(relative_path))
            staged = self._run("diff", "--cached", "--quiet", "--", str(relative_path), check=False)
            if staged.returncode == 0:
                head = self._head()
                return GitCommitResult(
                    status="no_changes",
                    repository=str(self.root),
                    commit_sha=head,
                    commit_short_sha=head[:8] if head else None,
                    message=message,
                )

            self._run(
                "-c",
                "user.name=AI Data Engineer Agent",
                "-c",
                "user.email=agent@local.dev",
                "commit",
                "-m",
                message,
                "--",
                str(relative_path),
            )
            head = self._head()
            return GitCommitResult(
                status="committed",
                repository=str(self.root),
                commit_sha=head,
                commit_short_sha=head[:8] if head else None,
                message=message,
            )
        except Exception as exc:
            return GitCommitResult(status="error", repository=str(self.root), message=message, error=str(exc))

    def history(self, path: Path, limit: int = 20) -> list[dict[str, str]]:
        if not self.enabled:
            return []
        try:
            relative_path = path.resolve().relative_to(self.root)
            result = self._run(
                "log",
                f"-n{limit}",
                "--pretty=format:%H%x1f%h%x1f%ad%x1f%s",
                "--date=iso-strict",
                "--",
                str(relative_path),
            )
        except Exception:
            return []

        commits: list[dict[str, str]] = []
        for line in result.stdout.splitlines():
            parts = line.split("\x1f")
            if len(parts) != 4:
                continue
            commits.append(
                {
                    "commit_sha": parts[0],
                    "commit_short_sha": parts[1],
                    "created_at": parts[2],
                    "message": parts[3],
                }
            )
        return commits

    def _ensure_repo(self) -> None:
        if not (self.root / ".git").exists():
            self._run("init")

    def _head(self) -> str | None:
        result = self._run("rev-parse", "HEAD", check=False)
        if result.returncode != 0:
            return None
        return result.stdout.strip()

    def _run(self, *args: str, check: bool = True) -> subprocess.CompletedProcess[str]:
        return subprocess.run(
            ["git", "-C", str(self.root), *args],
            text=True,
            capture_output=True,
            check=check,
        )
