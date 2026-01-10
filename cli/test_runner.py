# cli/test_runner.py
import json
from pathlib import Path

TESTS_DIR = Path(__file__).resolve().parents[1] / "tests"

PHASES = [
    "normalization",
    "signals",
    "pairs",
    "groups",
    "adversarial",
]


def load_json(name):
    path = TESTS_DIR / f"{name}.json"
    if not path.exists():
        raise RuntimeError(f"Missing test file: {path}")
    with open(path, "r", encoding="utf-8") as f:
        return json.load(f)


def run_phase(name, data, verbose=False):
    """
    Phase runner (stub).

    For now:
    - validates structure
    - counts entries
    """
    passed = 0
    failed = 0
    errors = []

    if not isinstance(data, list):
        return {
            "name": name,
            "passed": 0,
            "failed": 1,
            "errors": [f"{name}.json must be a list"]
        }

    for i, entry in enumerate(data):
        if not isinstance(entry, dict):
            failed += 1
            errors.append(f"{name}[{i}] is not an object")
        else:
            passed += 1

    return {
        "name": name,
        "passed": passed,
        "failed": failed,
        "errors": errors
    }


def run_tests(verbose=False, only=None, fail_fast=False):
    print("Pedro Organiza — Test Runner")
    print("----------------------------")

    phases = PHASES
    if only:
        if only not in PHASES:
            print(f"[ERROR] Unknown test phase: {only}")
            return 1
        phases = [only]

    total_failed = 0

    for phase in phases:
        data = load_json(phase)
        result = run_phase(phase, data, verbose)

        print(f"\n[{phase}]")
        print(f"  Passed: {result['passed']}")
        print(f"  Failed: {result['failed']}")

        if verbose and result["errors"]:
            for e in result["errors"]:
                print(f"    - {e}")

        if result["failed"] > 0:
            total_failed += result["failed"]
            if fail_fast:
                print("\n[FAIL-FAST] Stopping on first failure.")
                return 1

    if total_failed == 0:
        print("\n✅ All tests passed.")
        return 0

    print(f"\n❌ Tests failed: {total_failed}")
    return 1
