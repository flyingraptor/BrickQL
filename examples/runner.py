"""BrinkQL examples runner.

Runs one or more Cases against a live database, optionally invoking Ollama
to produce an actual QueryPlan, compiles both plans, executes them, and
saves a Trial JSON file for each run.

Usage
-----
List all cases::

    python examples/runner.py --list

Run a single case (verbose)::

    python examples/runner.py --case c01_01 -v

Run all cases without Ollama::

    python examples/runner.py --no-ollama

Run a category::

    python examples/runner.py --case c04

Step through cases interactively::

    python examples/runner.py --step
"""
from __future__ import annotations

import argparse
import json
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

# ---------------------------------------------------------------------------
# Adjust sys.path so the package is importable when run as a script
# ---------------------------------------------------------------------------
_REPO_ROOT = Path(__file__).resolve().parent.parent
if str(_REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(_REPO_ROOT))

from examples._case import Case
from examples._ollama import OllamaClient
from examples._seed import make_sqlite_conn
from examples._setup import load_snapshot
from examples._trial import (
    Trial,
    TrialComparison,
    TrialResult,
    compare_results,
    save_trial,
)
from examples.cases import ALL_CASES

from brinkql import QueryBuilder, QueryPlan, PolicyEngine, PromptBuilder, PlanValidator, _COMPILERS
from brinkql.errors import BrinkQLError

# ---------------------------------------------------------------------------
# ANSI colours
# ---------------------------------------------------------------------------
_RESET  = "\033[0m"
_GREEN  = "\033[32m"
_RED    = "\033[31m"
_YELLOW = "\033[33m"
_CYAN   = "\033[36m"
_BOLD   = "\033[1m"

_TRIALS_DIR = Path(__file__).parent / "trials"


# ---------------------------------------------------------------------------
# Core logic: run expected / actual paths
# ---------------------------------------------------------------------------

def _run_expected(
    case: Case,
    db_name: str,
    conn: Any,
    verbose: bool,
) -> TrialResult:
    """Compile and execute the hand-crafted expected plan."""
    result = TrialResult()
    snapshot = load_snapshot()

    try:
        plan = QueryPlan.model_validate(case.expected_plan)
        result.plan = case.expected_plan
        result.plan_raw = json.dumps(case.expected_plan, indent=2)

        validator = PlanValidator(snapshot, case.dialect)
        validator.validate(plan)

        engine = PolicyEngine(case.policy, snapshot, case.dialect)
        plan = engine.apply(plan)

        compiler_cls = _COMPILERS[case.dialect.target]
        builder = QueryBuilder(compiler_cls(), snapshot)
        compiled = builder.build(plan)
        merged_params = compiled.merge_runtime_params(case.runtime_params)
        result.sql = compiled.sql
        result.params = merged_params

        if verbose:
            print(f"  {_CYAN}[expected SQL]{_RESET}\n{compiled.sql}")
            if merged_params:
                print(f"  {_CYAN}[params]{_RESET} {merged_params}")

        rows = _execute(conn, compiled.sql, merged_params)
        result.rows = rows
        result.row_count = len(rows)

        if verbose:
            _print_rows(rows, label="expected")

    except Exception as exc:
        result.error = f"{type(exc).__name__}: {exc}"
        if verbose:
            print(f"  {_RED}[expected ERROR]{_RESET} {result.error}")

    return result


def _run_actual(
    case: Case,
    db_name: str,
    conn: Any,
    ollama: OllamaClient | None,
    verbose: bool,
) -> TrialResult:
    """Ask Ollama to produce a plan, then compile and execute it."""
    result = TrialResult()

    if ollama is None:
        result.error = "Ollama not available (--no-ollama)"
        return result

    snapshot = load_snapshot()
    prompt_builder = PromptBuilder(snapshot, case.dialect, case.policy)
    components = prompt_builder.build(case.question)
    system_prompt = components.system_prompt
    user_prompt = components.user_prompt

    try:
        plan_raw = ollama.get_plan_json(system_prompt, user_prompt)
        result.plan_raw = plan_raw

        if verbose:
            print(f"  {_CYAN}[Ollama raw]{_RESET}\n{plan_raw[:400]}")

        plan_dict = json.loads(plan_raw)
        plan = QueryPlan.model_validate(plan_dict)
        result.plan = plan_dict

        validator = PlanValidator(snapshot, case.dialect)
        validator.validate(plan)

        engine = PolicyEngine(case.policy, snapshot, case.dialect)
        plan = engine.apply(plan)

        compiler_cls = _COMPILERS[case.dialect.target]
        builder = QueryBuilder(compiler_cls(), snapshot)
        compiled = builder.build(plan)
        merged_params = compiled.merge_runtime_params(case.runtime_params)
        result.sql = compiled.sql
        result.params = merged_params

        if verbose:
            print(f"  {_CYAN}[actual SQL]{_RESET}\n{compiled.sql}")
            if merged_params:
                print(f"  {_CYAN}[params]{_RESET} {merged_params}")

        rows = _execute(conn, compiled.sql, merged_params)
        result.rows = rows
        result.row_count = len(rows)

        if verbose:
            _print_rows(rows, label="actual")

    except json.JSONDecodeError as exc:
        result.error = f"ParseError: Not valid JSON — {exc}"
        if verbose:
            print(f"  {_RED}[actual ERROR]{_RESET} {result.error}")
    except BrinkQLError as exc:
        result.error = f"{type(exc).__name__}: {exc}"
        if verbose:
            print(f"  {_RED}[actual ERROR]{_RESET} {result.error}")
    except Exception as exc:
        result.error = f"Unexpected error: {exc}"
        if verbose:
            print(f"  {_RED}[actual ERROR]{_RESET} {result.error}")

    return result


def _execute(conn: Any, sql: str, params: dict[str, Any] | None) -> list[dict[str, Any]]:
    """Execute *sql* with *params* and return list of row dicts."""
    cursor = conn.cursor()
    if params:
        import re
        named = re.sub(r":([A-Za-z_][A-Za-z0-9_]*)", r":\1", sql)
        cursor.execute(named, params)
    else:
        cursor.execute(sql)
    cols = [d[0] for d in cursor.description] if cursor.description else []
    return [dict(zip(cols, row)) for row in cursor.fetchall()]


def _print_rows(rows: list[dict], label: str, max_rows: int = 5) -> None:
    if not rows:
        print(f"  {_YELLOW}[{label}]{_RESET} (no rows)")
        return
    shown = rows[:max_rows]
    print(f"  {_CYAN}[{label} rows — {len(rows)} total]{_RESET}")
    for r in shown:
        print(f"    {r}")
    if len(rows) > max_rows:
        print(f"    … {len(rows) - max_rows} more rows")


# ---------------------------------------------------------------------------
# Trial orchestration
# ---------------------------------------------------------------------------

def run_trial(
    case: Case,
    db_name: str,
    conn: Any,
    ollama: OllamaClient | None,
    verbose: bool,
) -> Trial:
    """Run one Case and return a Trial."""
    ts = Trial.make_timestamp()
    model_tag = ollama.model if ollama else "none"

    print(f"  {_BOLD}{case.id}{_RESET}  {case.question[:70]}", end="", flush=True)

    expected = _run_expected(case, db_name, conn, verbose)
    actual = _run_actual(case, db_name, conn, ollama, verbose)
    comparison = compare_results(expected, actual)

    trial = Trial(
        case_id=case.id,
        category=case.category,
        question=case.question,
        case_notes=case.notes,
        timestamp=ts,
        target_db=db_name,
        ollama_model=model_tag,
        expected=expected,
        actual=actual,
        comparison=comparison,
    )

    _print_inline_status(trial)
    return trial


def _ok(ok: bool) -> str:
    return f"{_GREEN}✓{_RESET}" if ok else f"{_RED}✗{_RESET}"


def _print_inline_status(trial: Trial) -> None:
    exp_ok = trial.expected.error is None
    act_ok = trial.actual.error is None and trial.actual.sql is not None
    sql_match = trial.comparison.sql_match
    rows_match = trial.comparison.rows_match

    print(
        f"  exp={_ok(exp_ok)} "
        f"act={_ok(act_ok)} "
        f"sql={_ok(sql_match)} "
        f"rows={_ok(rows_match)}"
    )


def print_trial_summary(trial: Trial, verbose: bool = False) -> None:
    """Print a formatted summary line (and optionally details) for a Trial."""
    exp_ok = trial.expected.error is None
    act_ok = trial.actual.error is None and trial.actual.sql is not None
    status = _ok(exp_ok and act_ok)

    line = (
        f"    {status} {_BOLD}{trial.case_id}{_RESET}  "
        f"{trial.question[:60]}"
    )
    print(line)

    if not exp_ok:
        print(f"      {_RED}expected error:{_RESET} {trial.expected.error}")
    if not act_ok and trial.actual.error != "Ollama not available (--no-ollama)":
        print(f"      {_RED}actual error:{_RESET}   {trial.actual.error}")
    if verbose and trial.comparison.notes and trial.comparison.notes != "All checks passed":
        print(f"      {_YELLOW}note:{_RESET} {trial.comparison.notes}")
    if verbose and trial.expected.sql:
        print(f"      {_CYAN}expected sql:{_RESET} {trial.expected.sql}")
    if verbose and trial.actual.sql:
        print(f"      {_CYAN}actual sql:{_RESET}   {trial.actual.sql}")


# ---------------------------------------------------------------------------
# Step-mode prompt
# ---------------------------------------------------------------------------

def _step_prompt(case: Case, index: int, total: int) -> bool:
    """Show case header and ask whether to run it.

    Returns True  → run.
    Returns False → skip (user typed 's').
    Exits          → user typed 'q'.
    """
    print(f"\n{'═' * 60}")
    print(
        f"  {_BOLD}[{index}/{total}]  {case.id}{_RESET}  "
        f"{_CYAN}{case.category}{_RESET}"
    )
    print(f"  {_BOLD}Q:{_RESET} {case.question}")
    if case.notes:
        print(f"  {_YELLOW}Note:{_RESET} {case.notes[:120]}")
    print(f"{'─' * 60}")
    try:
        answer = input("  Run? [Enter=yes / s=skip / q=quit] › ").strip().lower()
    except (EOFError, KeyboardInterrupt):
        print()
        sys.exit(0)
    if answer == "q":
        print("Quitting.")
        sys.exit(0)
    if answer == "s":
        print(f"  {_YELLOW}Skipped.{_RESET}")
        return False
    return True


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

def _parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(
        description="Run BrinkQL examples against SQLite (and optionally Ollama).",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=__doc__,
    )
    p.add_argument(
        "--case", metavar="PREFIX",
        help=(
            "Run only cases whose ID starts with PREFIX "
            "(e.g. 'c01', 'c01_01'). Omit for all cases."
        ),
    )
    p.add_argument(
        "--db", choices=["sqlite", "postgres", "both"], default="sqlite",
        help="Which database backend(s) to target (default: sqlite).",
    )
    p.add_argument(
        "--no-ollama", action="store_true",
        help="Skip Ollama calls; only run the expected plan.",
    )
    p.add_argument(
        "--model", default="gpt-oss:latest",
        help="Ollama model tag (default: gpt-oss:latest).",
    )
    p.add_argument(
        "--verbose", "-v", action="store_true",
        help="Print compiled SQL and first rows for each case.",
    )
    p.add_argument(
        "--list", action="store_true",
        help="Print all case IDs and questions, then exit.",
    )
    p.add_argument(
        "--no-save", action="store_true",
        help="Do not save trial JSON files.",
    )
    p.add_argument(
        "--step", "-s", action="store_true",
        help=(
            "Interactive step-through mode: show each case header and wait for "
            "Enter before running it. Press 's' to skip, 'q' to quit."
        ),
    )
    return p.parse_args()


def _select_cases(prefix: str | None) -> list[Case]:
    if prefix is None:
        return ALL_CASES
    return [c for c in ALL_CASES if c.id.startswith(prefix)]


def main() -> None:
    args = _parse_args()

    if args.list:
        print(f"\n{'ID':<12} {'CATEGORY':<22} QUESTION")
        print("-" * 90)
        for c in ALL_CASES:
            print(f"{c.id:<12} {c.category:<22} {c.question[:55]}")
        print(f"\nTotal: {len(ALL_CASES)} cases")
        return

    cases = _select_cases(args.case)
    if not cases:
        print(f"No cases match prefix '{args.case}'.", file=sys.stderr)
        sys.exit(1)

    sqlite_conn = make_sqlite_conn()

    ollama: OllamaClient | None = None
    if not args.no_ollama:
        client = OllamaClient(model=args.model)
        if client.is_available():
            ollama = client
            print(f"{_GREEN}Ollama available{_RESET} — model: {args.model}")
        else:
            print(
                f"{_YELLOW}Warning:{_RESET} Ollama not reachable at "
                f"{client.base_url}. Running in --no-ollama mode."
            )

    dbs_to_run: list[str] = (
        ["sqlite", "postgres"] if args.db == "both" else [args.db]
    )

    total = passed = failed = skipped = 0
    saved_paths: list[Path] = []

    for db_name in dbs_to_run:
        if db_name == "postgres":
            print(
                f"\n{_YELLOW}PostgreSQL support:{_RESET} connect via psycopg. "
                "Ensure docker-compose is running (make test-integration-postgres) "
                "and pass connection details via PG_DSN env var. "
                "PostgreSQL trials are not yet wired in this runner — skipping."
            )
            continue

        conn = sqlite_conn
        print(
            f"\n{_BOLD}Running {len(cases)} cases against {db_name}{_RESET} "
            f"({'with' if ollama else 'without'} Ollama)\n"
        )

        by_category: dict[str, list[Trial]] = {}
        eligible = [c for c in cases if db_name in c.target_dbs]
        n_eligible = len(eligible)

        for idx, case in enumerate(eligible, start=1):
            if args.step:
                if not _step_prompt(case, idx, n_eligible):
                    skipped += 1
                    continue

            trial = run_trial(case, db_name, conn, ollama, args.verbose)
            total += 1

            cat = case.category
            by_category.setdefault(cat, []).append(trial)

            if trial.expected.error is None:
                passed += 1
            else:
                failed += 1

            if not args.no_save:
                path = save_trial(trial, _TRIALS_DIR)
                saved_paths.append(path)

            if args.step:
                print_trial_summary(trial, verbose=True)

        if not args.step:
            for cat, trials in by_category.items():
                print(f"\n  {_BOLD}── {cat}{_RESET}")
                for t in trials:
                    print_trial_summary(t, args.verbose)

    print(f"\n{'─' * 60}")
    print(
        f"  Total: {total}  "
        f"{_GREEN}Expected OK: {passed}{_RESET}  "
        f"{_RED}Expected FAIL: {failed}{_RESET}  "
        f"Skipped: {skipped}"
    )
    if saved_paths:
        print(f"  Trials saved to: {_TRIALS_DIR}")

    sqlite_conn.close()


if __name__ == "__main__":
    main()
