import json
import os
from pathlib import Path

LANG = os.getenv("PEDRO_LANG", "en")

BASE_DIR = Path(__file__).parent
LANG_FILE = BASE_DIR / f"{LANG}.json"

with open(LANG_FILE, "r", encoding="utf-8") as f:
    _MESSAGES = json.load(f)

def msg(key: str) -> str:
    return _MESSAGES.get(key, f"[{key}]")
# ---------------- END OF FILE ----------------