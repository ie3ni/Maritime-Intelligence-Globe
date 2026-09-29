from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import yaml
from dotenv import load_dotenv


@dataclass(frozen=True)
class Settings:
    root: Path
    host: str
    port: int
    data_dir: Path
    database_path: Path
    frontend_dist: Path
    app: dict[str, Any]
    network: dict[str, Any]
    sources: dict[str, Any]
    storage: dict[str, Any]
    analytics: dict[str, Any]
    visualization: dict[str, Any]


def load_config(root: Path | None = None) -> Settings:
    root = (root or Path(__file__).resolve().parents[2]).resolve()
    load_dotenv(root / ".env", override=False)
    config_dir = Path(os.getenv("MARITIME_CONFIG_DIR", root / "config"))
    docs = {}
    for name in ("app", "network", "sources", "storage", "analytics", "visualization"):
        with (config_dir / f"{name}.yaml").open(encoding="utf-8") as handle:
            docs[name] = yaml.safe_load(handle) or {}
    app = docs["app"]["app"]
    storage = docs["storage"]["storage"]
    data_dir = Path(os.getenv("MARITIME_DATA_DIR", root / storage["data_dir"]))
    return Settings(
        root=root, host=app["host"], port=int(app["port"]), data_dir=data_dir,
        database_path=root / storage["database_path"], frontend_dist=root / "frontend" / "dist",
        app=app, network=docs["network"]["network"], sources=docs["sources"]["sources"],
        storage=storage, analytics=docs["analytics"], visualization=docs["visualization"]["visualization"],
    )
