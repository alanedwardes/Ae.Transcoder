from __future__ import annotations

import hashlib
import json
import os
import pathlib
import threading
import time
from dataclasses import dataclass
from typing import Any, Dict, Optional

from .ffmpeg_runner import start_ffmpeg
import shutil


_sessions_lock = threading.Lock()


@dataclass
class Session:
    session_id: str
    params: Dict[str, Any]
    session_dir: str
    process: Any  # subprocess.Popen
    last_access_utc: float


_sessions: Dict[str, Session] = {}


def _get_sessions_root() -> str:
    # Prefer explicit env var; else use /sessions if present; else ./sessions
    env_dir = os.getenv("SESSIONS_DIR")
    if env_dir:
        return env_dir
    if os.path.isdir("/sessions"):
        return "/sessions"
    return str(pathlib.Path("sessions").absolute())


def get_session_dir(session_id: str) -> str:
    return os.path.join(_get_sessions_root(), session_id)


def normalize_params_from_query(query_params: Any) -> Dict[str, Any]:
    src = query_params.get("src") if hasattr(query_params, "get") else None
    if not src:
        raise ValueError("missing src")

    extra: list[str] = []

    if hasattr(query_params, "multi_items"):
        items = list(query_params.multi_items())
    elif hasattr(query_params, "items"):
        items = list(query_params.items())
    else:
        items = []

    for key, value in items:
        if key == "src":
            continue
        if value is None or value == "" or str(value).lower() in ("1", "true"):
            extra.append(f"-{key}")
        else:
            extra.append(f"-{key}")
            extra.append(str(value))

    return {
        "src": src,
        "extra": extra,
    }


def compute_session_id(params: Dict[str, Any]) -> str:
    # Serialize deterministically
    material = json.dumps(params, sort_keys=True, separators=(",", ":"))
    digest = hashlib.sha256(material.encode("utf-8")).hexdigest()
    return digest[:24]


def ensure_session_running(*, session_id: str, params: Dict[str, Any]) -> None:
    session_dir = get_session_dir(session_id)
    with _sessions_lock:
        existing = _sessions.get(session_id)
        if existing and existing.process and existing.process.poll() is None:
            return

        os.makedirs(session_dir, exist_ok=True)
        proc = start_ffmpeg(session_id=session_id, params=params, session_dir=session_dir)
        _sessions[session_id] = Session(
            session_id=session_id,
            params=params,
            session_dir=session_dir,
            process=proc,
            last_access_utc=time.time(),
        )


def update_last_access(session_id: str) -> None:
    with _sessions_lock:
        s = _sessions.get(session_id)
        if s:
            s.last_access_utc = time.time()


def get_session_process(session_id: str):
    with _sessions_lock:
        s = _sessions.get(session_id)
        return s.process if s else None


def list_sessions() -> Dict[str, Session]:
    with _sessions_lock:
        return dict(_sessions)


def stop_session(session_id: str) -> None:
    with _sessions_lock:
        s = _sessions.get(session_id)
    if not s:
        return
    proc = s.process
    try:
        if proc and proc.poll() is None:
            proc.terminate()
            try:
                proc.wait(timeout=5)
            except Exception:
                proc.kill()
    finally:
        try:
            shutil.rmtree(s.session_dir, ignore_errors=True)
        finally:
            with _sessions_lock:
                _sessions.pop(session_id, None)


def stop_all_sessions() -> None:
    for sid in list(list_sessions().keys()):
        stop_session(sid)


