"""LLM-as-a-Judge validator powered by OpenAI Codex CLI (`codex exec`).

This is a thin wrapper around the `codex exec` non-interactive subcommand
of the OpenAI Codex CLI (https://developers.openai.com/codex/cli). The CLI
runs locally, reads its own credentials from ~/.codex/auth.json (or an
API key from the environment), and produces only the final agent message
on stdout — making it the simplest possible judge integration: no HTTP
client, no API key plumbing, no payload schemas.

Why Codex as a judge: GPT-5-Codex (which Codex CLI uses by default) is
OpenAI's coding-specialised model trained on real-world software-engineering
tasks via RL on PR reviews. It is purpose-built for the kind of evaluation
we need here — comparing a generated Airflow DAG or PySpark script against
a reference implementation.

Prerequisites:
    1. Install Codex CLI:    npm install -g @openai/codex
    2. Authenticate:         codex login   (or set OPENAI_API_KEY)

Configuration (override via env vars):

    JUDGE_MODEL          codex model id          (gpt-5-codex)
    JUDGE_TIMEOUT_S      per-prompt timeout      (120)
    JUDGE_CONCURRENCY    parallel scoring calls  (2)
    JUDGE_SANDBOX        codex sandbox mode      (read-only)

Usage:

    # Dry-run (no codex CLI required, mocked scores, for documentation)
    python3 llm_judge.py

    # Live scoring of all 10 reference DAGs and 10 reference Spark scripts
    python3 llm_judge.py --live
"""
import asyncio
import json
import os
import re
import shutil
import statistics
import sys
import time
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Any

ROOT = Path(__file__).parent.parent


# --------------------------- Config ---------------------------------------

class JudgeConfig:
    def __init__(self) -> None:
        self.model = os.environ.get("JUDGE_MODEL", "gpt-5-codex")
        self.timeout_s = float(os.environ.get("JUDGE_TIMEOUT_S", "120"))
        self.concurrency = int(os.environ.get("JUDGE_CONCURRENCY", "2"))
        self.sandbox = os.environ.get("JUDGE_SANDBOX", "read-only")


CFG = JudgeConfig()


# --------------------------- Data shapes ----------------------------------

@dataclass
class JudgeScore:
    """Rubric returned by Codex for one (spec, generated) pair."""
    spec_id: str
    semantic: int       # 0-10
    api: int            # 0-10
    robustness: int     # 0-10
    style: int          # 0-10
    verdict: str        # accept | accept_with_minors | reject
    critique: str
    overall: float = 0.0

    def __post_init__(self) -> None:
        self.overall = round(
            0.45 * self.semantic + 0.30 * self.api +
            0.15 * self.robustness + 0.10 * self.style, 2
        )


# --------------------------- Prompt ---------------------------------------

PROMPT_TEMPLATE = """You are a strict senior data-engineering code reviewer.

Compare GENERATED code below against a SPEC, using REFERENCE as one of many
valid implementations. Score on semantic equivalence, not lexical similarity.
Equivalent re-implementations of the same intent are fine. Do not reward
verbosity — concise correct code is better than long correct code.

## SPEC ({spec_id})
{intent}

Invariants:
{invariants}

## REFERENCE ({source_repo})
```python
{reference_code}
```

## GENERATED
```python
{generated_code}
```

Output ONLY a JSON object with this exact shape (no markdown, no preamble):
{{
  "semantic":   <int 0-10>,
  "api":        <int 0-10>,
  "robustness": <int 0-10>,
  "style":      <int 0-10>,
  "verdict":    "accept" | "accept_with_minors" | "reject",
  "critique":   "<1-3 sentences, specific, cite symbols>"
}}"""


# --------------------------- Codex CLI runner ----------------------------

class CodexJudge:
    """Invokes the local `codex exec` subcommand to score one pair at a time."""

    def __init__(self, cfg: JudgeConfig = CFG) -> None:
        self.cfg = cfg

    @property
    def enabled(self) -> bool:
        return shutil.which("codex") is not None

    async def score(
        self, spec: dict[str, Any], reference_code: str, generated_code: str,
    ) -> JudgeScore:
        if not self.enabled:
            raise RuntimeError(
                "`codex` CLI not found in PATH. Install with: "
                "npm install -g @openai/codex, then `codex login`."
            )

        def trim(s: str, lim: int = 8000) -> str:
            return s if len(s) <= lim else s[:lim] + "\n# ... (truncated)"

        prompt = PROMPT_TEMPLATE.format(
            spec_id=spec["id"],
            intent=spec.get("spec", ""),
            invariants="\n".join(f"  - {i}" for i in spec.get("invariants", [])),
            source_repo=spec.get("source_repo", "the official project"),
            reference_code=trim(reference_code),
            generated_code=trim(generated_code),
        )

        # codex exec: non-interactive, prints final agent message to stdout.
        # --sandbox read-only — judge must not modify any files.
        # --skip-git-repo-check — judge runs from outside a git repo too.
        proc = await asyncio.create_subprocess_exec(
            "codex", "exec",
            "--model", self.cfg.model,
            "--sandbox", self.cfg.sandbox,
            "--skip-git-repo-check",
            prompt,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
        )
        try:
            stdout, stderr = await asyncio.wait_for(
                proc.communicate(), timeout=self.cfg.timeout_s,
            )
        except asyncio.TimeoutError:
            proc.kill()
            raise RuntimeError(f"codex exec timed out after {self.cfg.timeout_s}s")

        if proc.returncode != 0:
            raise RuntimeError(
                f"codex exec failed (exit {proc.returncode}): "
                f"{stderr.decode('utf-8', 'replace')[:400]}"
            )

        rubric = _extract_json_object(stdout.decode("utf-8", "replace"))
        return JudgeScore(
            spec_id=spec["id"],
            semantic=int(rubric["semantic"]),
            api=int(rubric["api"]),
            robustness=int(rubric["robustness"]),
            style=int(rubric["style"]),
            verdict=str(rubric["verdict"]),
            critique=str(rubric["critique"]),
        )


def _extract_json_object(text: str) -> dict[str, Any]:
    """Find the first JSON object in Codex's stdout.

    Codex usually returns clean JSON when asked, but may occasionally wrap
    it in a code fence or add a brief preamble. This is robust to both.
    """
    text = text.strip()
    # Strip ``` fences if present
    if text.startswith("```"):
        text = re.sub(r"^```(?:json)?\s*", "", text)
        text = re.sub(r"\s*```\s*$", "", text)
    # Find the outermost {...}
    m = re.search(r"\{.*\}", text, re.DOTALL)
    if not m:
        raise ValueError(f"no JSON object in Codex output: {text[:200]}")
    return json.loads(m.group(0))


# --------------------------- Drivers --------------------------------------

async def score_all(
    judge: CodexJudge, specs: list[dict[str, Any]], ref_dir: Path,
) -> list[JudgeScore]:
    sem = asyncio.Semaphore(CFG.concurrency)

    async def one(spec: dict[str, Any]) -> JudgeScore:
        async with sem:
            code = (ref_dir / spec["real_source_file"]).read_text()
            return await judge.score(spec, code, code)

    return await asyncio.gather(*[one(s) for s in specs])


def print_report(category: str, results: list[JudgeScore]) -> None:
    print(f"\n{category}")
    print("-" * 92)
    print(f"{'spec':<12} {'sem':>4} {'api':>4} {'rob':>4} {'sty':>4} "
          f"{'overall':>8} {'verdict':<22} critique")
    print("-" * 92)
    for r in results:
        crit = r.critique[:40] + ("…" if len(r.critique) > 40 else "")
        print(f"{r.spec_id:<12} {r.semantic:>4} {r.api:>4} {r.robustness:>4} "
              f"{r.style:>4} {r.overall:>8.2f} {r.verdict:<22} {crit}")
    if results:
        mean = statistics.fmean([r.overall for r in results])
        non_reject = sum(1 for r in results if r.verdict != "reject") / len(results)
        print("-" * 92)
        print(f"AGGREGATE: mean_overall={mean:.2f}, "
              f"non_reject_rate={non_reject:.2f}, n={len(results)}")


async def main_live() -> None:
    judge = CodexJudge()
    if not judge.enabled:
        print("ERROR: `codex` CLI not found in PATH.")
        print("Install with:  npm install -g @openai/codex")
        print("Then login:    codex login")
        sys.exit(2)

    print(f"Judge: `codex exec` via {CFG.model} (sandbox={CFG.sandbox})")

    d3 = json.loads((ROOT / "D3_airflow_dags.json").read_text())
    d4 = json.loads((ROOT / "D4_pyspark_tasks.json").read_text())

    print("\nScoring D3...")
    d3_scores = await score_all(judge, d3, ROOT / "D3_real_airflow_dags")
    print_report("D3 — Airflow DAGs (Codex judge)", d3_scores)

    print("\nScoring D4...")
    d4_scores = await score_all(judge, d4, ROOT / "D4_real_spark_scripts")
    print_report("D4 — PySpark scripts (Codex judge)", d4_scores)

    out = {
        "config": {
            "judge": "codex exec",
            "model": CFG.model,
            "sandbox": CFG.sandbox,
            "timestamp": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
        },
        "d3": [asdict(s) for s in d3_scores],
        "d4": [asdict(s) for s in d4_scores],
    }
    out_path = ROOT / "D3_D4_validators" / "judge_report.json"
    out_path.write_text(json.dumps(out, indent=2, ensure_ascii=False))
    print(f"\nReport saved: {out_path}")


def main_dry_run() -> None:
    """Heuristic-only scoring without `codex` CLI, for documentation."""
    cli_present = shutil.which("codex") is not None
    print(f"DRY-RUN — codex CLI {'FOUND in PATH' if cli_present else 'NOT FOUND in PATH'}")
    print(f"           model = {CFG.model}, sandbox = {CFG.sandbox}\n")

    def mock(spec_id: str, code: str) -> JudgeScore:
        has_doc = '"""' in code
        has_retry = "retries" in code or "retry_delay" in code
        has_callable = "def " in code or "with DAG" in code or "@dag" in code
        semantic = 9 if has_callable else 6
        api = 9 if "import " in code and has_callable else 6
        robustness = 8 if has_retry else 6
        style = 8 if has_doc else 6
        return JudgeScore(
            spec_id=spec_id, semantic=semantic, api=api,
            robustness=robustness, style=style,
            verdict="accept" if semantic >= 8 else "accept_with_minors",
            critique="canonical pattern; "
                    + ("retries present" if has_retry else "no explicit retries"),
        )

    d3 = json.loads((ROOT / "D3_airflow_dags.json").read_text())
    d4 = json.loads((ROOT / "D4_pyspark_tasks.json").read_text())
    d3_scores = [mock(s["id"],
                      (ROOT / "D3_real_airflow_dags" / s["real_source_file"]).read_text())
                 for s in d3]
    d4_scores = [mock(s["id"],
                      (ROOT / "D4_real_spark_scripts" / s["real_source_file"]).read_text())
                 for s in d4]
    print_report("D3 — Airflow DAGs (mock)", d3_scores)
    print_report("D4 — PySpark scripts (mock)", d4_scores)

    out = {
        "config": {"judge": "codex exec", "model": CFG.model, "note": "dry-run, mocked"},
        "d3": [asdict(s) for s in d3_scores],
        "d4": [asdict(s) for s in d4_scores],
    }
    (ROOT / "D3_D4_validators" / "judge_report_dry_run.json").write_text(
        json.dumps(out, indent=2, ensure_ascii=False)
    )


if __name__ == "__main__":
    if "--live" in sys.argv:
        asyncio.run(main_live())
    else:
        main_dry_run()
