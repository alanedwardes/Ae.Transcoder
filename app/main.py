from fastapi import FastAPI, HTTPException, Request
from fastapi.responses import FileResponse, RedirectResponse
from typing import Optional
import os
import pathlib
import threading
import time
from contextlib import asynccontextmanager

from .session_manager import (
    normalize_params_from_query,
    compute_session_id,
    ensure_session_running,
    get_session_dir,
    update_last_access,
    list_sessions,
    stop_session,
    stop_all_sessions,
)
from .ffmpeg_runner import wait_for_playlist_ready


@asynccontextmanager
async def lifespan(app: FastAPI):
    idle_ttl = int(os.getenv("IDLE_TTL_SECONDS", "30"))

    def sweeper():
        while not _sweeper_stop.is_set():
            now = time.time()
            for sid, sess in list_sessions().items():
                if now - sess.last_access_utc > idle_ttl:
                    stop_session(sid)
            _sweeper_stop.wait(2)

    global _sweeper_thread
    _sweeper_thread = threading.Thread(target=sweeper, name="idle-sweeper", daemon=True)
    _sweeper_thread.start()
    try:
        yield
    finally:
        stop_all_sessions()
        _sweeper_stop.set()


app = FastAPI(lifespan=lifespan)
_sweeper_thread: Optional[threading.Thread] = None
_sweeper_stop = threading.Event()


@app.get("/health")
def health() -> dict:
    return {"status": "ok"}


@app.get("/hls/index.m3u8")
def hls_bootstrap(request: Request):
    query = request.query_params
    src = query.get("src")
    if not src:
        raise HTTPException(status_code=400, detail="missing src")
    params = normalize_params_from_query(query)
    session_id = compute_session_id(params)
    session_dir = get_session_dir(session_id)
    ensure_session_running(session_id=session_id, params=params)

    try:
        wait_for_playlist_ready(session_dir=session_dir)
    except RuntimeError as e:
        raise HTTPException(status_code=500, detail=str(e))

    update_last_access(session_id)
    playlist_path = pathlib.Path(session_dir) / "index.m3u8"
    if not playlist_path.exists():
        raise HTTPException(status_code=500, detail="playlist not found")
    return RedirectResponse(url=f"/hls/{session_id}/index.m3u8")


@app.get("/hls/{session_id}/index.m3u8")
def hls_playlist(session_id: str):
    session_dir = get_session_dir(session_id)
    update_last_access(session_id)
    playlist_path = pathlib.Path(session_dir) / "index.m3u8"
    if not playlist_path.exists():
        raise HTTPException(status_code=404, detail="playlist not found")
    return FileResponse(path=str(playlist_path), media_type="application/vnd.apple.mpegurl")


@app.get("/hls/{session_id}/{segment_name}")
def hls_segment(session_id: str, segment_name: str):
    session_dir = get_session_dir(session_id)
    update_last_access(session_id)
    seg_path = pathlib.Path(session_dir) / segment_name
    if not seg_path.exists():
        raise HTTPException(status_code=404, detail="segment not found")
    media_type = "video/MP2T" if seg_path.suffix.lower() == ".ts" else "video/mp4"
    return FileResponse(path=str(seg_path), media_type=media_type)


