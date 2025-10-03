from __future__ import annotations

import os
import pathlib
import subprocess
import sys
import time
from typing import Any, Dict


def _parse_width_height(res: str) -> tuple[int, int]:
    if "x" in res:
        w, h = res.lower().split("x")
        return int(w), int(h)
    return 1280, 720


def start_ffmpeg(*, session_id: str, params: Dict[str, Any], session_dir: str):
    src: str = params["src"]
    v: str = params["v"]
    a: str = params["a"]
    br: str = params["br"]
    res: str = params["res"]
    fps = params.get("fps")
    seg_dur: int = int(params["segDur"])
    list_size: int = int(params["listSize"])
    segment_type: str = params["segmentType"]

    w, h = _parse_width_height(res)
    seg_ext = ".m4s" if segment_type == "fmp4" else ".ts"

    playlist_path = str(pathlib.Path(session_dir) / "index.m3u8")
    segment_pattern = str(pathlib.Path(session_dir) / ("%06d" + seg_ext))

    cmd = [
        "ffmpeg",
        "-hide_banner",
        "-loglevel",
        "error",
        "-nostdin",
        "-re",
        "-i",
        src,
        "-map",
        "0:v:0",
        "-map",
        "0:a:0?",
        "-c:v",
        "libx264" if v == "h264" else v,
        "-preset",
        "veryfast",
        "-profile:v",
        "main",
        "-level",
        "4.1",
        "-b:v",
        br,
        "-maxrate",
        br,
        "-vf",
        f"scale={w}:{h}:force_original_aspect_ratio=decrease",
        "-c:a",
        "aac" if a == "aac" else a,
        "-b:a",
        "128k",
        "-ac",
        "2",
        "-ar",
        "48000",
        "-f",
        "hls",
        "-hls_time",
        str(seg_dur),
        "-hls_list_size",
        str(list_size),
        "-hls_flags",
        "delete_segments+independent_segments+program_date_time",
        "-hls_segment_filename",
        segment_pattern,
    ]

    if fps:
        cmd.extend(["-r", str(int(fps))])
    if segment_type == "fmp4":
        cmd.extend(["-hls_segment_type", "fmp4", "-hls_flags", "delete_segments+independent_segments+cmaf"])

    cmd.append(playlist_path)

    proc = subprocess.Popen(cmd, stdout=subprocess.DEVNULL, stderr=subprocess.STDOUT)
    return proc


def wait_for_playlist_ready(*, session_dir: str, timeout: int | None = None):
    max_wait = timeout or int(os.getenv("MAX_PLAYLIST_WAIT_SECONDS", "20"))
    playlist = pathlib.Path(session_dir) / "index.m3u8"
    start = time.time()
    while time.time() - start < max_wait:
        if playlist.exists() and playlist.stat().st_size > 0:
            return
        time.sleep(0.2)
    raise RuntimeError("playlist not ready in time")


