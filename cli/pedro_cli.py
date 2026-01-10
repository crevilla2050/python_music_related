#!/usr/bin/env python3
"""
pedro_cli.py

Pedro Organiza — CLI Wrapper with i18n support

Responsibilities:
- Execute backend scripts
- Capture stdout line by line
- Translate i18n message objects
- Render human-readable output
- Preserve raw output when needed
- Manage Pedro-level config (language only)

This wrapper NEVER changes backend behavior.
"""

import argparse
import json
import subprocess
import sys
from pathlib import Path

# ---------------- PATHS ----------------

ROOT_DIR = Path(__file__).resolve().parents[1]
CONFIG_PATH = ROOT_DIR / "backend" / "config.json"
I18N_DIR = ROOT_DIR / "music-ui" / "src" / "i18n"

# ---------------- CONFIG ----------------

def default_config():
    return {
        "language": "en",
        "versions": {
            "normalization": "v1.0",
            "signals": "v1.0",
            "grouping": "v1.0",
        },
        "ui": {
            "translate": True
        }
    }


def load_config():
    if not CONFIG_PATH.exists():
        return None
    try:
        return json.loads(CONFIG_PATH.read_text(encoding="utf-8"))
    except Exception as e:
        raise SystemExit(f"[ERROR] Invalid config.json: {e}")


def save_config(cfg):
    CONFIG_PATH.parent.mkdir(parents=True, exist_ok=True)
    CONFIG_PATH.write_text(json.dumps(cfg, indent=2), encoding="utf-8")


# ---------------- I18N ----------------

def available_languages():
    return {p.stem for p in I18N_DIR.glob("*.json")}


def load_translations(lang: str):
    path = I18N_DIR / f"{lang}.json"
    if not path.exists():
        raise FileNotFoundError(lang)
    return json.loads(path.read_text(encoding="utf-8"))


def resolve_language(cli_lang, config, langs):
    if cli_lang:
        if cli_lang in langs:
            return cli_lang, None
        return "en", f"Unknown language '{cli_lang}', falling back to English."

    if config:
        cfg_lang = config.get("language", "en")
        if cfg_lang in langs:
            return cfg_lang, None
        return "en", f"Unknown language '{cfg_lang}' in config, falling back to English."

    return "en", None


def render_message(obj, translations):
    if not isinstance(obj, dict):
        return str(obj)

    key = obj.get("key")
    params = obj.get("params", {})

    if not key:
        return json.dumps(obj, ensure_ascii=False)

    template = translations.get(key, key)

    try:
        return template.format(**params)
    except Exception:
        return template + f" {params}"


# ---------------- EXECUTION ----------------

def run_script(script_path, script_args, translations, raw=False):
    cmd = [sys.executable, script_path] + script_args

    proc = subprocess.Popen(
        cmd,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        text=True,
    )

    for line in proc.stdout:
        line = line.strip()
        if not line:
            continue

        if raw:
            print(line)
            continue

        try:
            obj = json.loads(line.replace("'", '"'))
            print(render_message(obj, translations))
        except Exception:
            print(line)

    proc.wait()
    return proc.returncode


# ---------------- CLI ----------------

def main():
    parser = argparse.ArgumentParser(description="Pedro Organiza CLI")

    parser.add_argument("--lang", help="Override language (en, es, de)")
    parser.add_argument("--raw", action="store_true", help="Do not translate output")

    sub = parser.add_subparsers(dest="command")
    # test
    p_test = sub.add_parser("test", help="Run Pedro test corpus")
    p_test.add_argument("--verbose", action="store_true")
    p_test.add_argument("--only", help="Run only one test phase")
    p_test.add_argument("--fail-fast", action="store_true")


    # init
    p_init = sub.add_parser("init", help="Initialize Pedro config")
    p_init.add_argument("--lang", help="Set default language")
    p_init.add_argument("--force", action="store_true", help="Overwrite existing config")

    # status
    sub.add_parser("status", help="Show Pedro status")

    # run
    p_run = sub.add_parser("run", help="Run a backend script")
    p_run.add_argument("script", help="Path to backend script")
    p_run.add_argument("script_args", nargs=argparse.REMAINDER)
    

    args = parser.parse_args()

    langs = available_languages()
    config = load_config()

    # ---------------- INIT ----------------
    if args.command == "init":
        if CONFIG_PATH.exists() and not args.force:
            print("Config already exists. Use --force to overwrite.")
            sys.exit(1)

        cfg = default_config()
        if args.lang:
            cfg["language"] = args.lang

        save_config(cfg)
        print(f"Config initialized at {CONFIG_PATH}")
        sys.exit(0)

    # ---------------- LANGUAGE RESOLUTION ----------------
    lang, warning = resolve_language(args.lang, config, langs)
    if warning:
        print(f"[WARN] {warning}")

    translations = {}
    try:
        translations = load_translations(lang)
    except FileNotFoundError:
        print("[WARN] Failed to load translations, using raw output.")
        args.raw = True

    # ---------------- STATUS ----------------
    if args.command == "status":
        print("Pedro Organiza")
        print("--------------")
        print(f"Language:     {lang}")
        print(f"Config file:  {CONFIG_PATH if CONFIG_PATH.exists() else 'missing'}")
        sys.exit(0)

    # ---------------- TEST ----------------
    if args.command == "test":
        from cli.test_runner import run_tests

        exit_code = run_tests(
            verbose=args.verbose,
            only=args.only,
            fail_fast=args.fail_fast,
        )
        sys.exit(exit_code)


    # ---------------- RUN ----------------
    
    if args.command == "run":
        script = args.script
        script_args = args.script_args

        exit_code = run_script(
            script,
            script_args,
            translations,
            raw=args.raw
        )
        sys.exit(exit_code)

    parser.print_help()


if __name__ == "__main__":
    main()
