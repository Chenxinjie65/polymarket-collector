from __future__ import annotations

import json
import time
from datetime import UTC, datetime
from pathlib import Path
from typing import Any


def write_heartbeat(
    *,
    data_root: Path,
    node_id: str,
    role: str,
    status: str = "ok",
    extra: dict[str, Any] | None = None,
) -> Path:
    state_dir = data_root / "state"
    state_dir.mkdir(parents=True, exist_ok=True)
    path = state_dir / f"heartbeat_{node_id}.json"
    payload: dict[str, Any] = {
        "node_id": node_id,
        "role": role,
        "status": status,
        "ts": datetime.now(UTC).isoformat(),
    }
    if extra:
        payload["extra"] = extra

    tmp_path = path.with_suffix(path.suffix + ".tmp")
    tmp_path.write_text(json.dumps(payload, ensure_ascii=True, indent=2), encoding="utf-8")
    tmp_path.replace(path)
    return path


def heartbeat_loop(
    *,
    data_root: Path,
    node_id: str,
    role: str,
    status: str,
    interval_seconds: int,
    duration_seconds: int,
    extra: dict[str, Any] | None = None,
) -> None:
    started = time.monotonic()
    while True:
        write_heartbeat(
            data_root=data_root,
            node_id=node_id,
            role=role,
            status=status,
            extra=extra,
        )
        if duration_seconds > 0 and (time.monotonic() - started) >= duration_seconds:
            return
        time.sleep(interval_seconds)


def evaluate_failover(
    *,
    data_root: Path,
    primary_node_id: str,
    max_stale_seconds: int,
) -> dict[str, Any]:
    hb_path = data_root / "state" / f"heartbeat_{primary_node_id}.json"
    now = datetime.now(UTC)
    result: dict[str, Any] = {
        "primary_node_id": primary_node_id,
        "max_stale_seconds": max_stale_seconds,
        "ts_check": now.isoformat(),
        "primary_heartbeat_exists": hb_path.exists(),
        "primary_healthy": False,
        "stale_seconds": None,
        "recommend_backup_collect": True,
    }

    if not hb_path.exists():
        return result

    payload = json.loads(hb_path.read_text(encoding="utf-8"))
    ts_value = payload.get("ts")
    if not isinstance(ts_value, str):
        return result

    hb_time = _parse_iso_datetime(ts_value)
    if hb_time is None:
        return result

    stale_seconds = int((now - hb_time).total_seconds())
    healthy = stale_seconds <= max_stale_seconds and payload.get("status") == "ok"
    result["stale_seconds"] = stale_seconds
    result["primary_healthy"] = healthy
    result["recommend_backup_collect"] = not healthy
    return result


def write_failover_state(*, data_root: Path, state: dict[str, Any]) -> Path:
    state_dir = data_root / "state"
    state_dir.mkdir(parents=True, exist_ok=True)
    path = state_dir / "failover_state.json"
    tmp_path = path.with_suffix(path.suffix + ".tmp")
    tmp_path.write_text(json.dumps(state, ensure_ascii=True, indent=2), encoding="utf-8")
    tmp_path.replace(path)
    return path


def _parse_iso_datetime(value: str) -> datetime | None:
    try:
        if value.endswith("Z"):
            value = value[:-1] + "+00:00"
        dt = datetime.fromisoformat(value)
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=UTC)
        return dt.astimezone(UTC)
    except ValueError:
        return None

