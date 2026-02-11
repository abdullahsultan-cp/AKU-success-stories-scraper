#!/usr/bin/env python
# run.py
# Success Stories: scrape from AKU site and upload to Storyblok (one script)

import argparse
import json
import logging
import mimetypes
import os
import random
import re
import sys
import time
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Optional
from urllib.parse import urljoin, urlsplit, quote

import requests
from requests.exceptions import HTTPError, SSLError, Timeout, ConnectionError
from bs4 import BeautifulSoup

try:
    from dotenv import load_dotenv
except Exception:
    load_dotenv = None


# ----------------------------
# Env
# ----------------------------
def _load_env_file(path: Path) -> None:
    if not path.exists():
        return
    try:
        with open(path, "r", encoding="utf-8", errors="ignore") as f:
            for line in f:
                line = line.strip()
                if not line or line.startswith("#") or "=" not in line:
                    continue
                key, _, value = line.partition("=")
                key = key.strip()
                value = value.strip().strip('"').strip("'")
                if key and value:
                    os.environ.setdefault(key, value)
    except Exception:
        pass


# ----------------------------
# Storyblok config
# ----------------------------
CONTENT_TYPE = "success_story"
FIELD_TITLE = "title"
FIELD_DESCRIPTION = "description"
FIELD_IMAGE = "image"
CONTENT_PATH = ["Automation", "success-stories"]


# ----------------------------
# Run folders & logging
# ----------------------------
@dataclass
class RunPaths:
    root: Path
    output: Path


def safe_run_folder_name(dt: datetime) -> str:
    return f"ss_scrape_{dt.strftime('%Y-%m-%d_%I%M%S%p')}"


def setup_run_dirs() -> RunPaths:
    dt = datetime.now()
    root = Path(safe_run_folder_name(dt))
    output = root / "output"
    for p in [root, output]:
        p.mkdir(parents=True, exist_ok=True)
    return RunPaths(root=root, output=output)


def setup_logging() -> logging.Logger:
    logger = logging.getLogger("run")
    logger.setLevel(logging.INFO)
    logger.handlers.clear()
    fmt = logging.Formatter("%(asctime)s | %(levelname)s | %(message)s")
    sh = logging.StreamHandler(sys.stdout)
    sh.setLevel(logging.INFO)
    sh.setFormatter(fmt)
    logger.addHandler(sh)
    return logger


# ----------------------------
# Helpers
# ----------------------------
def safe_text(s: str) -> str:
    s = (s or "").replace("\xa0", " ")
    s = re.sub(r"\s+", " ", s).strip()
    return s


def sanitize_filename(filename: str) -> str:
    filename = re.sub(r'[<>:"/\\|?*]', '', filename)
    filename = filename.replace(' ', '_').strip('. ')
    return filename[:200] if filename else "story"


def normalize_url(url: str, page_base: str) -> str:
    if not url:
        return ""
    url = url.strip()
    if url.startswith("//"):
        url = "https:" + url
    if url.startswith("/"):
        url = urljoin(page_base, url)
    parts = urlsplit(url)
    path = quote(parts.path, safe="/%()_-.,~")
    query = quote(parts.query, safe="=&%")
    return f"{parts.scheme}://{parts.netloc}{path}?{query}" if query else f"{parts.scheme}://{parts.netloc}{path}"


def get_page_base(url: str) -> str:
    parts = urlsplit(url)
    return f"{parts.scheme}://{parts.netloc}"


def slugify(s: str, max_len: int = 90) -> str:
    s = (s or "").strip().lower()
    s = re.sub(r"[^\w\s-]", "", s, flags=re.UNICODE)
    s = re.sub(r"[\s_-]+", "-", s, flags=re.UNICODE).strip("-")
    if not s:
        s = f"story-{int(time.time())}"
    return s[:max_len].rstrip("-")


def read_links_file(path: str) -> List[str]:
    p = Path(path)
    if not p.exists():
        raise FileNotFoundError(f"links file not found: {path}")
    urls = []
    for line in p.read_text(encoding="utf-8", errors="ignore").splitlines():
        line = line.strip()
        if not line or line.startswith("#"):
            continue
        urls.append(line)
    seen = set()
    return [u for u in urls if u not in seen and not seen.add(u)]


# ----------------------------
# Data model
# ----------------------------
@dataclass
class SuccessStory:
    source_url: str
    title: str
    date: Optional[str]
    description: str
    body_text: str
    hero_image: Optional[str]


# ----------------------------
# Scraper
# ----------------------------
def extract_title(soup: BeautifulSoup) -> str:
    h1 = soup.find("h1")
    if h1:
        return safe_text(h1.get_text())
    og = soup.find("meta", property="og:title")
    if og:
        return safe_text(og.get("content", ""))
    tt = soup.find("title")
    return safe_text(tt.get_text()) if tt else "Untitled"


def extract_description(soup: BeautifulSoup) -> str:
    og = soup.find("meta", property="og:description")
    if og:
        return safe_text(og.get("content", ""))
    meta = soup.find("meta", attrs={"name": "description"})
    return safe_text(meta.get("content", "")) if meta else ""


def extract_hero_image(soup: BeautifulSoup, page_base: str) -> Optional[str]:
    content_div = soup.find("div", class_="ContentMain") or soup.find("div", class_="MainContentZone")
    search = content_div if content_div else soup
    skip = ["facebook.com/tr", "google-analytics", "pixel", "doubleclick", "logo", "icon", "avatar", "_layouts", "spcommon", "siteassets"]
    for img in search.find_all("img", src=True):
        src = img.get("src", "")
        if not src or any(x in src.lower() for x in skip):
            continue
        if not src.lower().endswith((".png", ".jpg", ".jpeg", ".gif", ".webp")):
            continue
        url = normalize_url(src, page_base)
        if url:
            return url
    return None


def extract_body_text(soup: BeautifulSoup) -> str:
    content_div = soup.find("div", class_="ContentMain") or soup.find("div", class_="MainContentZone")
    if not content_div:
        content_div = soup.find("div", class_=lambda x: x and "content" in str(x).lower())
    blocks = content_div.find_all(["p", "em"]) if content_div else soup.find_all(["p", "em"])
    paragraphs = []
    for tag in blocks:
        if tag.name == "em" and tag.find_parent("p"):
            continue
        text = safe_text(tag.get_text())
        if text and len(text.strip()) >= 3:
            paragraphs.append(text)
    return "\n\n".join(paragraphs)


def extract_date(soup: BeautifulSoup) -> Optional[str]:
    pub = soup.find("meta", attrs={"property": "article:published_time"})
    if pub and pub.get("content"):
        m = re.search(r"(\d{4}-\d{2}-\d{2})", pub["content"])
        if m:
            return m.group(1)
    for el in soup.find_all(["span", "div"], class_=lambda x: x and any(d in str(x).lower() for d in ["date", "published", "modified"])):
        m = re.search(r"(\d{4}-\d{2}-\d{2})", safe_text(el.get_text()))
        if m:
            return m.group(1)
    return None


def parse_story_html(html: str, page_url: str) -> SuccessStory:
    soup = BeautifulSoup(html, "html.parser")
    base = get_page_base(page_url)
    return SuccessStory(
        source_url=page_url,
        title=extract_title(soup),
        date=extract_date(soup),
        description=extract_description(soup),
        body_text=extract_body_text(soup),
        hero_image=extract_hero_image(soup, base),
    )


def fetch_html(url: str, timeout: int = 60) -> str:
    r = requests.get(
        url,
        headers={"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) Chrome/120.0.0.0 Safari/537.36", "Accept": "text/html,application/xhtml+xml,*/*;q=0.8"},
        timeout=timeout,
    )
    r.raise_for_status()
    return r.text


def download_image(image_url: str, save_path: Path, filename_base: str) -> Optional[str]:
    try:
        r = requests.get(image_url, headers={"User-Agent": "Mozilla/5.0 Chrome/120.0.0.0"}, timeout=30)
        r.raise_for_status()
        ct = r.headers.get("content-type", "").lower()
        ext = ".png" if "png" in ct else ".webp" if "webp" in ct else ".gif" if "gif" in ct else ".jpg"
        for e in [".png", ".jpg", ".jpeg", ".gif", ".webp"]:
            if e in image_url.lower():
                ext = e
                break
        fp = save_path / f"{filename_base}{ext}"
        c = 1
        while fp.exists():
            fp = save_path / f"{filename_base}_{c}{ext}"
            c += 1
        fp.write_bytes(r.content)
        return str(fp.relative_to(save_path.parent.parent))
    except Exception:
        return None


def run_scrape(paths: RunPaths, urls: List[str], logger: logging.Logger) -> List[Path]:
    """Scrape URLs, save JSON + images under paths.output. Returns list of JSON paths."""
    (paths.root / "links_used.txt").write_text("\n".join(urls), encoding="utf-8")
    images_folder = paths.output / "images"
    images_folder.mkdir(exist_ok=True)
    json_paths: List[Path] = []
    summary = {"total": len(urls), "success": 0, "failed": 0, "errors": []}

    for idx, url in enumerate(urls, start=1):
        logger.info(f"[{idx}/{len(urls)}] Scraping: {url}")
        try:
            html = fetch_html(url)
        except HTTPError as he:
            code = getattr(he, "response", None)
            code = code.status_code if code else None
            logger.error(f"PAGE SKIP (HTTP {code}): {url}")
            summary["failed"] += 1
            summary["errors"].append({"url": url, "type": "HTTPError", "code": code})
            continue
        except (Timeout, ConnectionError, SSLError) as e:
            logger.error(f"PAGE SKIP (Network): {url} | {type(e).__name__}")
            summary["failed"] += 1
            summary["errors"].append({"url": url, "type": type(e).__name__})
            continue
        except Exception as e:
            logger.error(f"PAGE SKIP: {url} | {type(e).__name__}: {e}")
            summary["failed"] += 1
            summary["errors"].append({"url": url, "type": type(e).__name__, "msg": str(e)})
            continue

        story = parse_story_html(html, url)
        safe_title = sanitize_filename(story.title)
        local_image = None
        if story.hero_image:
            local_image = download_image(story.hero_image, images_folder, safe_title)
            if local_image:
                logger.info(f"   Image saved: {local_image}")

        out_file = paths.output / f"{safe_title}.json"
        c = 1
        while out_file.exists():
            out_file = paths.output / f"{safe_title}_{c}.json"
            c += 1

        out_file.write_text(
            json.dumps(
                {
                    "source_url": story.source_url,
                    "title": story.title,
                    "date": story.date,
                    "description": story.description,
                    "body_text": story.body_text,
                    "hero_image": local_image or story.hero_image,
                },
                ensure_ascii=False,
                indent=2,
            ),
            encoding="utf-8",
        )
        logger.info(f"Saved: {out_file.name}")
        summary["success"] += 1
        json_paths.append(out_file)

    (paths.root / "summary.json").write_text(json.dumps(summary, ensure_ascii=False, indent=2), encoding="utf-8")
    logger.info(f"Scrape summary: {summary['success']} ok, {summary['failed']} failed")
    return json_paths


# ----------------------------
# Storyblok client & upload
# ----------------------------
class StoryblokClient:
    def __init__(self, token: str, space_id: int, logger: logging.Logger):
        self.token = token
        self.space_id = space_id
        self.logger = logger
        self.base = "https://mapi.storyblok.com/v1"
        self.s = requests.Session()
        self.s.headers.update({"Authorization": token, "Content-Type": "application/json", "Accept": "application/json"})

    def _req(self, method: str, path: str, *, params=None, json_body=None, timeout=90, retries=4) -> dict:
        url = f"{self.base}{path}"
        last_err = None
        for attempt in range(1, retries + 1):
            try:
                r = self.s.request(method, url, params=params, data=json.dumps(json_body) if json_body else None, timeout=timeout)
                if r.status_code >= 400:
                    raise requests.HTTPError(f"{r.status_code} {r.text[:2000]}", response=r)
                return r.json()
            except Exception as e:
                last_err = e
                if attempt < retries:
                    time.sleep(1.2 * attempt)
                    continue
                raise last_err

    def create_signed_asset(self, filename: str, asset_folder_id: Optional[int] = None) -> dict:
        body = {"filename": filename}
        if asset_folder_id:
            body["asset_folder_id"] = int(asset_folder_id)
        return self._req("POST", f"/spaces/{self.space_id}/assets", json_body=body, timeout=90, retries=4)

    def upload_asset_from_bytes(self, signed_payload: dict, file_bytes: bytes, filename: str, mime: str) -> None:
        fields = signed_payload.get("fields") or {}
        post_url = signed_payload.get("post_url")
        if not post_url or not fields:
            raise RuntimeError("Signed upload payload missing fields/post_url")
        r = requests.post(post_url, data=fields, files={"file": (filename, file_bytes, mime)}, timeout=180)
        r.raise_for_status()

    @staticmethod
    def _ext_from_mime(mime: str) -> str:
        m = (mime or "").lower()
        if "png" in m:
            return ".png"
        if "jpeg" in m or "jpg" in m:
            return ".jpg"
        if "gif" in m:
            return ".gif"
        if "webp" in m:
            return ".webp"
        return ""

    def list_folders(self) -> list:
        out, page = [], 1
        while True:
            data = self._req("GET", f"/spaces/{self.space_id}/stories", params={"folder_only": 1, "per_page": 100, "page": page})
            items = data.get("stories", []) or []
            out.extend(items)
            if page * 100 >= int(data.get("total") or 0) or not items:
                break
            page += 1
        return out

    def ensure_content_folder_by_path(self, path_parts: list) -> int:
        if not path_parts:
            return 0
        folders = self.list_folders()
        parent_id = 0
        for name in path_parts:
            found = next((f for f in folders if f.get("is_folder") and f.get("name") == name and int(f.get("parent_id") or 0) == parent_id), None)
            if found:
                parent_id = int(found["id"])
                continue
            body = {"story": {"name": name, "slug": slugify(name), "is_folder": True, "parent_id": parent_id, "content": {"component": "folder"}}}
            created = self._req("POST", f"/spaces/{self.space_id}/stories", json_body=body)
            folder = created.get("story") or created
            parent_id = int(folder.get("id"))
            folders.append(folder)
        return parent_id

    def create_story(self, title: str, slug: str, content: dict, parent_id: int = 0, publish: bool = False) -> dict:
        body = {"story": {"name": title, "slug": slug, "parent_id": int(parent_id), "content": content}}
        return self._req("POST", f"/spaces/{self.space_id}/stories", params={"publish": 1} if publish else None, json_body=body)


def upload_image_to_storyblok(client: StoryblokClient, image_path: str, asset_folder_id: Optional[int] = None, max_retries: int = 3) -> Optional[dict]:
    if not image_path:
        return None
    path_obj = Path(image_path)
    if not path_obj.is_absolute() and not path_obj.exists():
        path_obj = Path(__file__).resolve().parent / image_path
    if not path_obj.exists():
        client.logger.warning(f"Image not found: {image_path}")
        return None
    for attempt in range(1, max_retries + 1):
        try:
            file_bytes = path_obj.read_bytes()
            mime, _ = mimetypes.guess_type(str(path_obj))
            if not mime or not mime.startswith("image/"):
                ext = path_obj.suffix.lower()
                mime = "image/png" if ext == ".png" else "image/jpeg" if ext in (".jpg", ".jpeg") else "image/gif" if ext == ".gif" else "image/webp" if ext == ".webp" else "image/jpeg"
            filename = path_obj.name or f"image-{int(time.time())}{client._ext_from_mime(mime)}"
            signed = client.create_signed_asset(filename, asset_folder_id)
            payload = signed.get("data") or signed
            client.upload_asset_from_bytes(payload, file_bytes, filename, mime)
            key = (payload.get("fields") or {}).get("key")
            if not key:
                raise RuntimeError("No asset key")
            asset_obj = {"filename": f"https://a.storyblok.com/{key}", "fieldtype": "asset"}
            if signed.get("id") or (signed.get("asset") or {}).get("id"):
                asset_obj["id"] = int(signed.get("id") or (signed.get("asset") or {}).get("id"))
            client.logger.info(f"Uploaded image: {filename}")
            return asset_obj
        except (SSLError, Timeout, ConnectionError) as e:
            if attempt < max_retries:
                time.sleep(1.5 * attempt)
                continue
        except Exception as e:
            client.logger.error(f"Image upload failed: {image_path} | {e}")
            return None
    return None


def create_storyblok_story(
    client: StoryblokClient, title: str, description: str, image_asset: Optional[dict], parent_id: int = 0, publish: bool = False
) -> Optional[dict]:
    base_slug = slugify(title)
    for slug in [base_slug, f"{base_slug}-{random.randint(1000, 9999)}", f"{base_slug}-{int(time.time())}"]:
        try:
            content = {"component": CONTENT_TYPE, FIELD_TITLE: title, FIELD_DESCRIPTION: description}
            if image_asset:
                content[FIELD_IMAGE] = image_asset
            result = client.create_story(title, slug, content, parent_id=parent_id, publish=publish)
            story = result.get("story") or result
            client.logger.info(f"Created story: {story.get('id')} / {story.get('slug')}")
            return story
        except HTTPError as he:
            resp = getattr(he, "response", None)
            if resp and resp.status_code == 422 and ("already taken" in (resp.text or "").lower() or "slug" in (resp.text or "").lower()):
                continue
            client.logger.error(f"Story creation failed: {he}")
            return None
        except Exception as e:
            client.logger.error(f"Story creation failed: {e}")
            return None
    return None


def run_upload(
    json_paths: List[Path],
    logger: logging.Logger,
    *,
    publish: bool = False,
    asset_folder_id: Optional[int] = None,
) -> None:
    """Upload each JSON to Storyblok. Resolves hero_image relative to each JSON's directory."""
    token = (os.getenv("STORYBLOK_TOKEN") or "").strip()
    space_id_str = (os.getenv("STORYBLOK_SPACE_ID") or "").strip()
    if not token or not space_id_str:
        logger.error("Set STORYBLOK_TOKEN and STORYBLOK_SPACE_ID in .env")
        sys.exit(1)
    try:
        space_id = int(space_id_str)
    except ValueError:
        logger.error("STORYBLOK_SPACE_ID must be an integer")
        sys.exit(1)

    client = StoryblokClient(token, space_id, logger)
    content_parent_id = client.ensure_content_folder_by_path(CONTENT_PATH)
    logger.info(f"Content folder ID: {content_parent_id}")

    for jp in json_paths:
        try:
            data = json.loads(jp.read_text(encoding="utf-8"))
        except Exception as e:
            logger.error(f"Skip {jp}: {e}")
            continue
        title = (data.get("title") or "").strip()
        if not title:
            logger.error(f"Skip {jp}: no title")
            continue
        desc = (data.get("description") or "").strip()
        body = (data.get("body_text") or "").strip()
        final_desc = f"{desc}\n\n{body}".strip() if body else desc
        hero = data.get("hero_image")
        hero_path = None
        if hero:
            for p in [jp.parent / hero, jp.parent.parent / hero]:
                if p.exists():
                    hero_path = str(p)
                    break
            if not hero_path:
                hero_path = hero
        logger.info(f"Uploading: {title[:50]}...")
        image_asset = upload_image_to_storyblok(client, hero_path, asset_folder_id) if hero_path else None
        story = create_storyblok_story(client, title, final_desc, image_asset, parent_id=content_parent_id, publish=publish)
        if story:
            logger.info(f"  -> {story.get('id')} https://app.storyblok.com/#/me/spaces/{space_id}/stories/0/0/{story.get('id')}")
        else:
            logger.error(f"  -> failed")


# ----------------------------
# Main
# ----------------------------
def main():
    script_dir = Path(__file__).resolve().parent
    for env_path in [script_dir / ".env", script_dir.parent / ".env", Path.cwd() / ".env"]:
        _load_env_file(env_path)
    if load_dotenv:
        load_dotenv(script_dir / ".env")
        load_dotenv(script_dir.parent / ".env")
        load_dotenv()

    ap = argparse.ArgumentParser(description="Scrape success stories and/or upload to Storyblok")
    ap.add_argument("--link", action="append", help="Story URL (repeatable)")
    ap.add_argument("--links-file", default="", help="File with one URL per line")
    ap.add_argument("--scrape-only", action="store_true", help="Only scrape, do not upload")
    ap.add_argument("--upload-only", metavar="PATH", help="Only upload: path to folder or single JSON file")
    ap.add_argument("--publish", action="store_true", help="Publish stories in Storyblok (default: draft)")
    ap.add_argument("--asset-folder-id", type=int, help="Storyblok asset folder ID for images")
    args = ap.parse_args()

    logger = setup_logging()
    json_paths: List[Path] = []

    if args.upload_only:
        # Upload only: path is folder or file
        p = Path(args.upload_only)
        if not p.exists():
            logger.error(f"Path not found: {p}")
            sys.exit(1)
        if p.is_dir():
            json_paths = list(p.glob("*.json"))
        else:
            json_paths = [p]
        if not json_paths:
            logger.error("No JSON files to upload")
            sys.exit(1)
        run_upload(json_paths, logger, publish=args.publish, asset_folder_id=args.asset_folder_id)
        return

    # Scrape (and optionally upload)
    urls = []
    if args.links_file:
        urls.extend(read_links_file(args.links_file))
    if args.link:
        urls.extend(args.link)
    seen = set()
    urls = [u.strip() for u in urls if u and u not in seen and not seen.add(u)]
    if not urls:
        logger.error("No URLs. Use --link URL or --links-file path")
        sys.exit(1)

    paths = setup_run_dirs()
    logger.info(f"Run folder: {paths.root}")
    json_paths = run_scrape(paths, urls, logger)
    if not json_paths:
        logger.error("No stories scraped.")
        sys.exit(1)

    if args.scrape_only:
        logger.info("Scrape only. Use --upload-only with the run folder output to upload.")
        return

    logger.info("Uploading to Storyblok...")
    run_upload(json_paths, logger, publish=args.publish, asset_folder_id=args.asset_folder_id)


if __name__ == "__main__":
    main()
