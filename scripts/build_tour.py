"""Record Pitwall's tour (scripts/tour.html) into the silent video on the baizakisaev.com home page
card, once in each theme (the light files have no suffix, the dark ones end in -dark):

site/assets/video/pitwall-tour(-dark).webm   six slides: the data, the problem with raw points,
                                             the share-of-points method, a finding, the latest
                                             season, then the dashboard itself
site/assets/img/pitwall-tour(-dark).jpg      its first frame, the still shown until it plays

It matches AI Grid Trader's presenter-mode video on the card above it: 1920 x 1080, about six
seconds a slide, each chart drawing itself. Every number on the slides comes from docs/data.json,
so re-run it after new races to keep the latest season current. It prints the second each slide
starts at: put the earlier of the two videos' times in the video's data-scenes in the portfolio's
index.html (the card's back and forward arrows use them).

Usage: python scripts/build_tour.py
Needs: pip install playwright && python -m playwright install chromium ffmpeg
       (Playwright's ffmpeg build encodes the VP9 video; a system ffmpeg with libvpx works too.)
"""
import base64
import functools
import glob
import http.server
import math
import os
import shutil
import subprocess
import sys
import tempfile
import threading
import time
from pathlib import Path

from playwright.sync_api import sync_playwright

ROOT = Path(__file__).resolve().parent.parent
SITE = ROOT / "site"  # the files publish.json sends to baizakisaev.com
FPS = 25
SIZE = (1280, 720)    # CSS pixels: the slides are laid out at 16:9
SCALE = 1.5           # recorded at 1920 x 1080 so it stays sharp on high-resolution screens
# Seconds on each slide; the charts take two to three seconds to draw, then there is time to read.
HOLDS = [6.0, 6.5, 7.0, 6.5, 7.0, 6.5]


def ffmpeg():
    # A full system ffmpeg first, else Playwright's own build, in its default folder or PLAYWRIGHT_BROWSERS_PATH.
    if shutil.which("ffmpeg"):
        return shutil.which("ffmpeg")
    folders = [os.environ.get("PLAYWRIGHT_BROWSERS_PATH", ""), "~/AppData/Local/ms-playwright",
               "~/.cache/ms-playwright", "~/Library/Caches/ms-playwright"]
    for folder in filter(None, folders):
        found = sorted(glob.glob(os.path.expanduser(f"{folder}/ffmpeg-*/ffmpeg-*")))
        if found:
            return found[-1]
    sys.exit("No ffmpeg found: install ffmpeg, or run  python -m playwright install ffmpeg")


def serve():
    """Serve the repository on a free local port (the tour reads docs/), and return its address."""
    class Quiet(http.server.SimpleHTTPRequestHandler):
        def log_message(self, *args):
            pass

    server = http.server.ThreadingHTTPServer(("127.0.0.1", 0), functools.partial(Quiet, directory=str(ROOT)))
    threading.Thread(target=server.serve_forever, daemon=True).start()
    return f"http://127.0.0.1:{server.server_address[1]}/"


def record(pw, url, scheme, suffix):
    # The screencast captures the browser window, so the window (not an emulated viewport) sets the size.
    browser = pw.chromium.launch(args=[f"--window-size={SIZE[0]},{SIZE[1]}", f"--force-device-scale-factor={SCALE}"])
    page = browser.new_context(no_viewport=True, color_scheme=scheme, reduced_motion="no-preference").new_page()
    page.goto(url + "scripts/tour.html", wait_until="networkidle")
    page.wait_for_function("window.tourReady === true", timeout=30000)
    page.wait_for_timeout(3000)                       # the first chart finishes drawing
    # The poster is the first frame, so nothing jumps when playback starts.
    poster = SITE / "assets" / "img" / f"pitwall-tour{suffix}.jpg"
    poster.parent.mkdir(parents=True, exist_ok=True)
    page.screenshot(path=str(poster), type="jpeg", quality=82)

    frames = []                                       # (timestamp, jpeg bytes)
    cdp = page.context.new_cdp_session(page)

    def on_frame(ev):
        frames.append((ev["metadata"]["timestamp"], ev["data"]))
        try:
            cdp.send("Page.screencastFrameAck", {"sessionId": ev["sessionId"]})
        except Exception:                             # a last frame can arrive as the browser closes
            pass

    cdp.on("Page.screencastFrame", on_frame)
    cdp.send("Page.startScreencast", {"format": "jpeg", "quality": 92,
                                      "maxWidth": int(SIZE[0] * SCALE), "maxHeight": int(SIZE[1] * SCALE)})
    page.wait_for_timeout(200)
    shown = []                                        # when each slide was asked for (wall clock)
    for i, hold in enumerate(HOLDS):
        if i:
            page.evaluate(f"show({i})")
            shown.append(time.time())
        page.wait_for_timeout(int(hold * 1000))
    cdp.send("Page.stopScreencast")
    browser.close()

    # Screencast frames arrive only when the page repaints: hold each one until the next. Playwright's
    # ffmpeg build reads files only (no pipes), so the frames go through an MJPEG file.
    frames.sort()
    start, total = frames[0][0], sum(HOLDS)
    stream = Path(tempfile.gettempdir()) / f"pitwall-tour{suffix}.mjpeg"
    with open(stream, "wb") as fh:
        j = 0
        for n in range(int(total * FPS)):
            t = start + n / FPS
            while j + 1 < len(frames) and frames[j + 1][0] <= t:
                j += 1
            fh.write(base64.b64decode(frames[j][1]))
    out = SITE / "assets" / "video" / f"pitwall-tour{suffix}.webm"
    out.parent.mkdir(parents=True, exist_ok=True)
    result = subprocess.run([ffmpeg(), "-y", "-loglevel", "error", "-f", "image2pipe", "-framerate", str(FPS),
                             "-c:v", "mjpeg", "-i", str(stream), "-c:v", "libvpx-vp9", "-b:v", "0", "-crf", "38",
                             "-deadline", "good", "-cpu-used", "2", "-pix_fmt", "yuv420p", "-an", str(out)])
    stream.unlink()
    if result.returncode:
        sys.exit("ffmpeg failed")
    print(f"wrote {out.relative_to(ROOT)}: {len(frames)} frames, {total:.1f} s, {out.stat().st_size / 1e6:.2f} MB")
    print("  slides start at", " ".join(["0"] + [f"{math.floor((s - start) * 10) / 10:g}" for s in shown]))


if __name__ == "__main__":
    url = serve()
    with sync_playwright() as pw:
        record(pw, url, "light", "")
        record(pw, url, "dark", "-dark")
