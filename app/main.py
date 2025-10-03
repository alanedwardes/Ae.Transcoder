from fastapi import FastAPI, HTTPException, Query, Response
from fastapi.responses import FileResponse, RedirectResponse, PlainTextResponse
from typing import Optional
import os
import pathlib
import threading
import time

from .session_manager import (
    normalize_params,
    compute_session_id,
    ensure_session_running,
    get_session_dir,
    update_last_access,
    list_sessions,
    stop_session,
    stop_all_sessions,
)
from .security import validate_source_url
from .ffmpeg_runner import wait_for_playlist_ready


app = FastAPI()
_sweeper_thread: Optional[threading.Thread] = None
_sweeper_stop = threading.Event()


@app.get("/health")
def health() -> dict:
    return {"status": "ok"}


@app.get("/hls/index.m3u8")
def hls_bootstrap(
    src: str = Query(...),
    v: str = Query("h264"),
    a: str = Query("aac"),
    br: str = Query("1500k"),
    res: str = Query("1280x720"),
    fps: Optional[int] = Query(None),
    segDur: int = Query(2),
    listSize: int = Query(6),
    segmentType: str = Query("ts"),
):
    validate_source_url(src)
    params = normalize_params(
        src=src,
        v=v,
        a=a,
        br=br,
        res=res,
        fps=fps,
        seg_dur=segDur,
        list_size=listSize,
        segment_type=segmentType,
    )
    session_id = compute_session_id(params)
    canonical = f"/hls/{session_id}/index.m3u8"
    return RedirectResponse(url=canonical)


@app.get("/hls/{session_id}/index.m3u8")
def hls_playlist(
    session_id: str,
    src: str = Query(...),
    v: str = Query("h264"),
    a: str = Query("aac"),
    br: str = Query("1500k"),
    res: str = Query("1280x720"),
    fps: Optional[int] = Query(None),
    segDur: int = Query(2),
    listSize: int = Query(6),
    segmentType: str = Query("ts"),
):
    validate_source_url(src)
    params = normalize_params(
        src=src,
        v=v,
        a=a,
        br=br,
        res=res,
        fps=fps,
        seg_dur=segDur,
        list_size=listSize,
        segment_type=segmentType,
    )
    computed_id = compute_session_id(params)
    if computed_id != session_id:
        return RedirectResponse(url=f"/hls/{computed_id}/index.m3u8")

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


@app.on_event("startup")
def on_startup():
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


@app.on_event("shutdown")
def on_shutdown():
    stop_all_sessions()
    _sweeper_stop.set()


