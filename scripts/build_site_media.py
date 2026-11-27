"""Render the Pitwall loop and stills used on baizakisaev.com, from the live dashboard, once in
each theme (the light files have no suffix, the dark ones end in -dark):

site/assets/video/pitwall-loop(-dark).webm     wide screens: the overview stepping through five
                                               seasons, from 2024 to 1988, 2002, 2016 and 2023,
                                               then back to 2024
site/assets/img/f1-dashboard(-dark).png        its first frame (2024), the still shown until the
                                               loop plays
site/assets/img/f1-dashboard-phone(-dark).png  phones (up to 640px): the championship chart card only
(and a lossless .webp copy of each still, which the pages offer first)

Pitwall follows the site's light and dark themes, so each theme gets its own video and stills,
and the pages show the one that matches. The dashboard draws its charts without animation, so the
loop is a slideshow that cuts from season to season, just as picking a season does. It starts on
2024, the season the case study discusses, so the still is its first frame and the loop comes back
round to it.

The portfolio's home page card and the Pitwall case study (site/pitwall-case-study.html) both use
these; publish.json lists them. After running it, copy the printed sizes into the width/height
attributes of the Pitwall <source>, <img> and <video> elements in site/pitwall-case-study.html and in
the portfolio repository's index.html.

Usage: python scripts/build_site_media.py [dashboard URL]
       (e.g. http://localhost:3000/ while `npm start` runs, to record changes before they go live)
Needs: pip install playwright pillow && python -m playwright install chromium ffmpeg
       (Playwright's ffmpeg build encodes the VP9 video; a system ffmpeg with libvpx works too.)
"""
import glob
import io
import os
import shutil
import subprocess
import sys
import tempfile
from pathlib import Path

from PIL import Image
from playwright.sync_api import sync_playwright

ROOT = Path(__file__).resolve().parent.parent
SITE = ROOT / "site"  # the files publish.json sends to baizakisaev.com
URL = sys.argv[1] if len(sys.argv) > 1 else "https://baizakisaev.com/pitwall/"
SEASONS = ["2024", "1988", "2002", "2016", "2023"]   # the first season's frame is the still
FPS = 20
HOLD = 2.8                                            # seconds on each season
PAD = 8


def ffmpeg():
    # A full system ffmpeg first (some Playwright builds lack libvpx's -row-mt option), else
    # Playwright's own build, in its default folder or PLAYWRIGHT_BROWSERS_PATH.
    if shutil.which("ffmpeg"):
        return shutil.which("ffmpeg")
    folders = [os.environ.get("PLAYWRIGHT_BROWSERS_PATH", ""), "~/AppData/Local/ms-playwright",
               "~/.cache/ms-playwright", "~/Library/Caches/ms-playwright"]
    for folder in filter(None, folders):
        found = sorted(glob.glob(os.path.expanduser(f"{folder}/ffmpeg-*/ffmpeg-*")))
        if found:
            return found[-1]
    sys.exit("No ffmpeg found: install ffmpeg, or run  python -m playwright install ffmpeg")


def palette_png(img, out):
    # The dashboard is a few flat colours, so a palette PNG is a fraction of the size and stays sharp.
    img.convert("RGB").quantize(colors=256, method=Image.Quantize.MEDIANCUT, dither=Image.Dither.NONE).save(out, optimize=True)
    print(f"wrote {out.relative_to(ROOT)}: {img.size[0]} x {img.size[1]}, {out.stat().st_size // 1024} KB")
    # Lossless WebP copy, offered first by the pages: about 45% smaller, and the text stays sharp.
    webp = out.with_suffix(".webp")
    Image.open(out).save(webp, "WEBP", lossless=True, method=6)
    print(f"wrote {webp.relative_to(ROOT)}: {webp.stat().st_size // 1024} KB")


def encode(frames, out):
    # Playwright's ffmpeg build reads files only (no pipes), so the frames go through an MJPEG file.
    w, h = frames[0].size
    stream = Path(tempfile.gettempdir()) / f"{out.stem}.mjpeg"
    with open(stream, "wb") as fh:
        for f in frames:
            f.save(fh, "JPEG", quality=95)
    cmd = [ffmpeg(), "-y", "-loglevel", "error", "-f", "image2pipe", "-framerate", str(FPS), "-c:v", "mjpeg",
           "-i", str(stream), "-vf", f"crop={w - w % 2}:{h - h % 2}:0:0", "-c:v", "libvpx-vp9", "-b:v", "0",
           "-crf", "34", "-row-mt", "1", "-pix_fmt", "yuv420p", "-an", str(out)]
    result = subprocess.run(cmd)
    stream.unlink()
    if result.returncode:
        sys.exit("ffmpeg failed")
    print(f"wrote {out.relative_to(ROOT)}: {w} x {h}, {len(frames) / FPS:.1f} s, {out.stat().st_size // 1024} KB")


def record(browser, scheme, suffix):
    """The loop and both stills in one theme (scheme "light" or "dark")."""
    # Wide: from the view switcher to the bottom of the championship chart's card (legend included).
    page = browser.new_page(viewport={"width": 1280, "height": 1400}, device_scale_factor=2, color_scheme=scheme)
    page.goto(URL + "#dashboard", wait_until="networkidle")
    page.wait_for_selector("#progress")
    page.wait_for_timeout(800)

    def clip():
        top = page.locator(".views").bounding_box()
        card = page.locator("#progress").evaluate_handle("(e) => e.closest('.panel')").as_element().bounding_box()
        right = page.locator("#filter-form").bounding_box()
        return {"x": top["x"] - PAD, "y": top["y"] - PAD, "width": right["x"] + right["width"] - top["x"] + 2 * PAD,
                "height": card["y"] + card["height"] - top["y"] + 2 * PAD}

    box = clip()
    stills = []
    for season in SEASONS:
        page.select_option("#season", season)
        page.wait_for_function("(s) => document.querySelector('#coverage') && document.querySelector('#season').value === s", arg=season)
        page.wait_for_timeout(600)
        now = clip()
        if abs(now["y"] - box["y"]) > 1 or abs(now["height"] - box["height"]) > 1:
            sys.exit(f"the layout moved in {season}: the clip would cut it off")
        stills.append(Image.open(io.BytesIO(page.screenshot(clip=box))).convert("RGB"))
        print(f"captured {season} ({scheme})")
    page.close()

    frames = [still for still in stills for _ in range(round(HOLD * FPS))]
    encode(frames, SITE / "assets" / "video" / f"pitwall-loop{suffix}.webm")
    palette_png(stills[0], SITE / "assets" / "img" / f"f1-dashboard{suffix}.png")

    # Phone: the whole "Championship progression" card, legend included, in the 2024 season.
    page = browser.new_page(viewport={"width": 600, "height": 1200}, device_scale_factor=2, color_scheme=scheme)
    page.goto(URL + "#dashboard", wait_until="networkidle")
    page.wait_for_selector("#progress")
    page.select_option("#season", SEASONS[0])
    page.wait_for_timeout(800)
    card = page.locator("#progress").evaluate_handle("(e) => e.closest('.panel')").as_element().bounding_box()
    # The card sits below the first screen on a phone, so the clip is taken from the full page.
    shot = page.screenshot(full_page=True, clip={"x": card["x"] - PAD, "y": card["y"] - PAD,
                                                 "width": card["width"] + 2 * PAD, "height": card["height"] + 2 * PAD})
    palette_png(Image.open(io.BytesIO(shot)), SITE / "assets" / "img" / f"f1-dashboard-phone{suffix}.png")
    page.close()


with sync_playwright() as pw:
    browser = pw.chromium.launch()
    record(browser, "light", "")
    record(browser, "dark", "-dark")
    browser.close()
