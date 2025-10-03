from __future__ import annotations

import os
import pathlib
import subprocess
import time
from typing import Any, Dict


def start_ffmpeg(*, session_id: str, params: Dict[str, Any], session_dir: str):
    src: str = params["src"]
    extra: list[str] = params.get("extra", [])

    playlist_path = str(pathlib.Path(session_dir) / "index.m3u8")
    segment_pattern = str(pathlib.Path(session_dir) / "%06d.ts")

    cmd: list[str] = [
        "ffmpeg",
#        "-hide_banner",
#        "-loglevel",
#        "error",
#        "-nostdin",
        "-re",
        "-i",
        src,
    ]

    if extra:
        cmd.extend(extra)

    cmd.extend([
        "-f",
        "hls",
        "-hls_segment_filename",
        segment_pattern,
        playlist_path,
    ])

    win_cmdline = subprocess.list2cmdline(cmd)
    print(f"FFmpeg exec (Windows copy/paste): {win_cmdline}")
    print(f"FFmpeg argv: {cmd}")

    proc = subprocess.Popen(cmd)
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


