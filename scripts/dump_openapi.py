#!/usr/bin/env python3
"""
Writes the coordinator's OpenAPI spec to openapi.json at the project root.
FastAPI generates this automatically from the route/model definitions, so it
always reflects the real API -- no separate spec to keep in sync by hand.

    python scripts/dump_openapi.py
"""

import json
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from coordinator.app import app  # noqa: E402

if __name__ == "__main__":
    out = Path(__file__).resolve().parent.parent / "openapi.json"
    out.write_text(json.dumps(app.openapi(), indent=2))
    print(f"wrote {out}")
