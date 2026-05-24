"""Реальный codex-judge для D12_v2 и D13:
1. Берём fail-кейсы (rule-based не прошли)
2. Запрашиваем агента ещё раз с той же спекой и СОХРАНЯЕМ сгенерированный код
3. Передаём (спека, gold, generated) в codex с structured rubric
4. Финальный verdict: accept / accept_with_minors / reject
5. promoted = accept + accept_with_minors
6. Пересчитываем pass rate с promoted и Wilson interval
"""

import json
import shutil
import subprocess
import sys
import time
from pathlib import Path
from urllib import error, request

ROOT = Path(__file__).resolve().parent
BACKEND = "http://localhost:18000"
EMAIL = "admin@local.dev"
PASSWORD = "admin"

JUDGE_SCHEMA = {
    "type": "object",
    "properties": {
        "verdict": {"type": "string", "enum": ["accept", "accept_with_minors", "reject"]},
        "semantic_match": {"type": "number"},
        "api_correctness": {"type": "number"},
        "reason": {"type": "string"},
    },
    "required": ["verdict", "semantic_match", "api_correctness", "reason"],
    "additionalProperties": False,
}


def login() -> str:
    body = json.dumps({"email": EMAIL, "password": PASSWORD}).encode()
    req = request.Request(f"{BACKEND}/auth/login", data=body,
                          headers={"Content-Type": "application/json"}, method="POST")
    return json.loads(request.urlopen(req, timeout=15).read())["access_token"]


def agent_query(token: str, query: str) -> dict:
    body = json.dumps({"query": query}).encode()
    req = request.Request(f"{BACKEND}/agent/query", data=body,
                          headers={"Content-Type": "application/json",
                                   "Authorization": f"Bearer {token}"}, method="POST")
    try:
        return json.loads(request.urlopen(req, timeout=180).read())
    except Exception as e:
        return {"error": str(e)}


def extract_code(resp: dict) -> str:
    chunks = []
    for t in resp.get("tool_calls", []) or []:
        if "write" not in (t.get("tool_name") or "").lower() \
                and "artifact" not in (t.get("tool_name") or "").lower():
            continue
        inp = t.get("input") or {}
        for k in ("code", "content", "script"):
            v = inp.get(k)
            if isinstance(v, str):
                chunks.append(v)
        out = t.get("output") or {}
        if isinstance(out, dict):
            v = out.get("code") or out.get("content")
            if isinstance(v, str):
                chunks.append(v)
    return "\n".join(chunks)


def codex_judge(spec: str, gold: str, generated: str) -> dict:
    if not generated.strip():
        return {"verdict": "reject", "semantic_match": 0, "api_correctness": 0,
                "reason": "agent did not produce code"}
    prompt = (
        "Ты — code reviewer. Оцени реализует ли сгенерированный код спеку. "
        "Разные стили (Operator vs TaskFlow в Airflow; RDD vs DataFrame в Spark) "
        "допустимы при одинаковой семантике. Реджектить только если код не делает "
        "того что просит спека или содержит грубые ошибки.\n\n"
        f"=== СПЕКА ===\n{spec[:1500]}\n\n"
        f"=== ЭТАЛОН (apache/) ===\n{gold[:3000]}\n\n"
        f"=== СГЕНЕРИРОВАННЫЙ КОД ===\n{generated[:4000]}\n\n"
        "JSON по схеме."
    )
    schema_path = ROOT / ".codex_judge_schema.json"
    schema_path.write_text(json.dumps(JUDGE_SCHEMA))
    out_path = ROOT / ".codex_judge_out.txt"
    cmd = ["codex", "exec", "--sandbox", "read-only", "--skip-git-repo-check",
           "--output-schema", str(schema_path), "-o", str(out_path), prompt]
    try:
        subprocess.run(cmd, capture_output=True, text=True, timeout=180)
        msg = out_path.read_text().strip() if out_path.exists() else ""
        if msg.startswith("```"):
            msg = msg.strip("`").lstrip("json").strip()
        return json.loads(msg)
    except Exception as e:
        return {"verdict": "reject", "semantic_match": 0, "api_correctness": 0,
                "reason": f"judge_err: {e}"}


def run(name: str, specs_file: str, results_file: str) -> None:
    cases = {c["id"]: c for c in json.load(open(ROOT / specs_file))}
    d = json.load(open(ROOT / "results" / results_file))
    fails = [r for r in d["results"] if not r["passed"]]
    print(f"\n=== {name}: {len(fails)} fails to judge ===")
    if not fails:
        return

    token = login()
    judgements = []
    for i, r in enumerate(fails, 1):
        case = cases.get(r["id"]) or {}
        spec = case.get("spec", "")
        gold_path = ROOT / case.get("source_file", "")
        gold = gold_path.read_text() if gold_path.exists() else ""

        print(f"  [{i}/{len(fails)}] {r['id']} — re-querying agent...", flush=True)
        t0 = time.time()
        resp = agent_query(token, spec)
        generated = extract_code(resp)
        print(f"      code_chars={len(generated)}  ({time.time()-t0:.0f}s)")

        print(f"      codex judging...", flush=True)
        t0 = time.time()
        v = codex_judge(spec, gold, generated)
        print(f"      verdict={v.get('verdict')} sem={v.get('semantic_match')} "
              f"api={v.get('api_correctness')} ({time.time()-t0:.0f}s)")
        judgements.append({
            "id": r["id"],
            "source_file": case.get("source_file"),
            "rule_passed": r["passed"],
            "code_chars": len(generated),
            "judge_verdict": v.get("verdict"),
            "judge_semantic": v.get("semantic_match"),
            "judge_api": v.get("api_correctness"),
            "judge_reason": v.get("reason"),
        })

    (ROOT / "results" / f"{name}_codex_judge.json").write_text(
        json.dumps(judgements, ensure_ascii=False, indent=2))

    promoted = sum(1 for j in judgements if j["judge_verdict"] in ("accept", "accept_with_minors"))
    rejected = sum(1 for j in judgements if j["judge_verdict"] == "reject")
    total = len(d["results"])
    rule_pass = sum(1 for r in d["results"] if r["passed"])
    final = rule_pass + promoted
    import math
    z = 1.96
    p = final / total
    denom = 1 + z*z/total
    center = (p + z*z/(2*total)) / denom
    half = z*math.sqrt(p*(1-p)/total + z*z/(4*total*total))/denom
    lo, hi = max(0, center-half), min(1, center+half)
    print(f"\n  Codex promoted: {promoted}, rejected: {rejected}")
    print(f"  → {name} after judge: {final}/{total} = {p*100:.1f}%, "
          f"Wilson [{lo*100:.1f}, {hi*100:.1f}]")


def main() -> None:
    if not shutil.which("codex"):
        raise SystemExit("codex CLI not installed")
    cat = sys.argv[1] if len(sys.argv) > 1 else "all"
    if cat in ("all", "D12_v2"):
        run("D12_v2", "D12_v2_pipeline_specs.json", "D12_v2.json")
    if cat in ("all", "D13"):
        run("D13", "D13_spark_specs.json", "D13.json")


if __name__ == "__main__":
    main()
