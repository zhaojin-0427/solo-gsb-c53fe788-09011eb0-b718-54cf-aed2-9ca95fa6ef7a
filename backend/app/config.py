import os
from pathlib import Path

# backend/app/config.py -> parents[2] is the repo root (both locally and in image)
_ROOT = Path(__file__).resolve().parents[2]

DATA_DIR = Path(os.getenv("DATA_DIR", "/data"))
DB_PATH = DATA_DIR / "drills.db"
STATIC_DIR = Path(os.getenv("STATIC_DIR", _ROOT / "frontend"))
DRILLS_DIR = Path(os.getenv("DRILLS_DIR", _ROOT / "drills"))

# Single shared presenter secret. Override in production via HOST_KEY.
HOST_KEY = os.getenv("HOST_KEY", "host-1234")
