"""Single source of truth for the NSE Option Chain Analyzer project config.

Reads a YAML file (optionally pointed at by OCA_CONFIG_PATH) and exposes
named settings. Environment variables always win over the file, so the same
settings can be overridden in CI, Docker, or dev without editing the repo.

Hierarchy (lowest to highest priority):
  1. defaults baked into this module
  2. oca_config.yaml
  3. environment variables

Usage:
    from config import cfg
    print(cfg.db_dsn)            # option-chain snapshot DB
    print(cfg.strat_dsn)        # strategy/signals/paper-trades DB
    print(cfg.snap_table)       # Postgres table name
    print(cfg.server_port)      # live dashboard port
"""

from __future__ import annotations

import os
from pathlib import Path

try:
    import yaml
except ImportError:  # pragma: no cover — project ships PyYAML; guard anyway
    yaml = None  # type: ignore[assignment]

DEFAULTS = {
    "db_dsn": "postgresql://postgres:postgres@localhost:5432/postgres",
    "strat_dsn": "postgresql://postgres:postgres@localhost:5432/postgres",
    "snap_table": "option_chain_snapshots",
    "server_host": "127.0.0.1",
    "server_port": 8899,
    "server_interval": 60,
    "symbols": {
        "nifty": {"symbol": "NIFTY", "strike": 22700},
        "banknifty": {"symbol": "BANKNIFTY", "strike": 49000},
    },
}

_ENV_MAP = {
    "db_dsn": "OCA_DB_DSN",
    "strat_dsn": "OCA_STRAT_DSN",
    "snap_table": "OCA_SNAP_TABLE",
    "server_host": "OCA_SERVER_HOST",
    "server_port": "OCA_SERVER_PORT",
    "server_interval": "OCA_SERVER_INTERVAL",
}

_CONFIG_PATH: Path | None = None
_yaml_loaded: bool = False


def _load_yaml() -> dict:
    """Best-effort YAML load; returns {} on any failure so the project stays
    usable with no config file at all."""
    global _yaml_loaded
    if _yaml_loaded:
        return {}
    _yaml_loaded = True

    path = _config_path()
    if path is None or not path.is_file():
        return {}

    if yaml is None:
        return {}

    try:
        with open(path, "r", encoding="utf-8") as fh:
            data = yaml.safe_load(fh) or {}
        return data if isinstance(data, dict) else {}
    except Exception:
        return {}


def _config_path() -> Path | None:
    global _CONFIG_PATH
    if _CONFIG_PATH is not None:
        return _CONFIG_PATH
    explicit = os.environ.get("OCA_CONFIG_PATH")
    if explicit:
        p = Path(explicit)
        _CONFIG_PATH = p if p.is_absolute() else (Path.cwd() / p)
        return _CONFIG_PATH
    _CONFIG_PATH = Path.cwd() / "oca_config.yaml"
    return _CONFIG_PATH


def _env(name: str) -> str | None:
    return os.environ.get(name)


def _resolve() -> dict:
    """Merge defaults → yaml → env, env winning."""
    out = dict(DEFAULTS)
    yaml_vals = _load_yaml()
    for key, default in DEFAULTS.items():
        if key in yaml_vals and yaml_vals[key] is not None:
            out[key] = yaml_vals[key]
    for key, env_name in _ENV_MAP.items():
        v = _env(env_name)
        if v is None:
            continue
        if key in ("server_port", "server_interval"):
            try:
                out[key] = int(v)
            except ValueError:
                pass
        else:
            out[key] = v
    return out


class _Config:
    def __init__(self) -> None:
        self._data = _resolve()

    def __getattr__(self, name: str):
        if name in self._data:
            return self._data[name]
        raise AttributeError(f"config has no key {name!r}")

    def as_dict(self) -> dict:
        return dict(self._data)


cfg = _Config()
