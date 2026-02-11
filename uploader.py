#!/usr/bin/env python
# uploader.py
# Success Stories uploader for Storyblok
# Uploads JSON files created by scraper.py to Storyblok

import argparse
import json
import logging
import mimetypes
import os
import random
import re
import sys
import time
from pathlib import Path
from typing import Dict, Optional

import requests
from requests.exceptions import HTTPError, SSLError, Timeout, ConnectionError

try:
    from dotenv import load_dotenv
except Exception:
    load_dotenv = None


def _load_env_file(path: Path) -> None:
    """Simple .env loader fallback when python-dotenv is not installed."""
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
# Component name (your Storyblok content type)
# ----------------------------
CONTENT_TYPE = "success_story"  # Update this to match your Storyblok content type name

# Field names (update if different in your Storyblok schema)
FIELD_TITLE = "title"
FIELD_DESCRIPTION = "description"
FIELD_IMAGE = "image"

# Content folder path (where stories will be created)
# Root > Automation > success-stories
CONTENT_PATH = ["Automation", "success-stories"]


# ----------------------------
# Helpers
# ----------------------------
def slugify(s: str, max_len: int = 90) -> str:
    """Convert title to URL-safe slug."""
    s = (s or "").strip().lower()
    s = re.sub(r"[^\w\s-]", "", s, flags=re.UNICODE)
    s = re.sub(r"[\s_-]+", "-", s, flags=re.UNICODE).strip("-")
    if not s:
        s = f"story-{int(time.time())}"
    return s[:max_len].rstrip("-")


def make_uid() -> str:
    """Generate a unique ID."""
    return "".join(random.choice("abcdefghijklmnopqrstuvwxyz0123456789") for _ in range(12))


def setup_logging() -> logging.Logger:
    """Setup logging to console."""
    logger = logging.getLogger("storyblok_uploader")
    logger.setLevel(logging.INFO)
    logger.handlers.clear()

    fmt = logging.Formatter("%(asctime)s | %(levelname)s | %(message)s")
    sh = logging.StreamHandler(sys.stdout)
    sh.setLevel(logging.INFO)
    sh.setFormatter(fmt)
    logger.addHandler(sh)

    return logger


# ----------------------------
# Storyblok Client
# ----------------------------
class StoryblokClient:
    def __init__(self, token: str, space_id: int, logger: logging.Logger):
        self.token = token
        self.space_id = space_id
        self.logger = logger
        self.base = "https://mapi.storyblok.com/v1"
        self.s = requests.Session()
        self.s.headers.update(
            {"Authorization": token, "Content-Type": "application/json", "Accept": "application/json"}
        )

    def _req(self, method: str, path: str, *, params=None, json_body=None, timeout=90, retries=4) -> dict:
        """Make a request to Storyblok API with retries."""
        url = f"{self.base}{path}"
        last_err = None
        for attempt in range(1, retries + 1):
            try:
                r = self.s.request(
                    method,
                    url,
                    params=params,
                    data=json.dumps(json_body) if json_body is not None else None,
                    timeout=timeout,
                )
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
        """Create a signed upload URL for an asset."""
        body = {"filename": filename}
        if asset_folder_id:
            body["asset_folder_id"] = int(asset_folder_id)
        return self._req("POST", f"/spaces/{self.space_id}/assets", json_body=body, timeout=90, retries=4)

    def upload_asset_from_bytes(self, signed_payload: dict, file_bytes: bytes, filename: str, mime: str) -> None:
        """Upload file bytes to the signed URL."""
        fields = signed_payload.get("fields") or {}
        post_url = signed_payload.get("post_url")
        if not post_url or not fields:
            raise RuntimeError("Signed upload payload missing fields/post_url")

        files = {"file": (filename, file_bytes, mime)}
        r = requests.post(post_url, data=fields, files=files, timeout=180)
        r.raise_for_status()

    @staticmethod
    def _ext_from_mime(mime: str) -> str:
        """Get file extension from MIME type."""
        mime = (mime or "").lower()
        if "png" in mime:
            return ".png"
        if "jpeg" in mime or "jpg" in mime:
            return ".jpg"
        if "gif" in mime:
            return ".gif"
        if "webp" in mime:
            return ".webp"
        return ""

    def list_folders(self) -> list:
        """List all folders in the space."""
        out = []
        page = 1
        while True:
            data = self._req("GET", f"/spaces/{self.space_id}/stories", params={"folder_only": 1, "per_page": 100, "page": page})
            items = data.get("stories", []) or []
            out.extend(items)
            total = int((data.get("total") or 0))
            if page * 100 >= total or len(items) == 0:
                break
            page += 1
        return out

    def ensure_content_folder_by_path(self, path_parts: list) -> int:
        """Ensure folder path exists, creating if needed. Returns folder ID (0 for root)."""
        if not path_parts:
            return 0
        
        folders = self.list_folders()
        parent_id = 0
        for name in path_parts:
            found = None
            for f in folders:
                if f.get("is_folder") and f.get("name") == name and int(f.get("parent_id") or 0) == int(parent_id):
                    found = f
                    break
            if found:
                parent_id = int(found["id"])
                continue
            # Create folder
            body = {
                "story": {
                    "name": name,
                    "slug": slugify(name),
                    "is_folder": True,
                    "parent_id": parent_id,
                    "content": {"component": "folder"},
                }
            }
            created = self._req("POST", f"/spaces/{self.space_id}/stories", json_body=body)
            folder = created.get("story") or created
            parent_id = int(folder.get("id"))
            folders.append(folder)
        return parent_id

    def create_story(self, title: str, slug: str, content: dict, parent_id: int = 0, publish: bool = False) -> dict:
        """Create a new story in Storyblok."""
        params = {"publish": 1} if publish else None
        body = {
            "story": {
                "name": title,
                "slug": slug,
                "parent_id": int(parent_id),
                "content": content,
            }
        }
        return self._req("POST", f"/spaces/{self.space_id}/stories", params=params, json_body=body)


# ----------------------------
# Upload functions
# ----------------------------
def upload_image_to_storyblok(
    client: StoryblokClient,
    image_path: str,
    asset_folder_id: Optional[int] = None,
    max_retries: int = 3,
) -> Optional[dict]:
    """
    Upload a local image file to Storyblok Assets API.
    
    Args:
        client: StoryblokClient instance
        image_path: Path to local image file (relative or absolute)
        asset_folder_id: Optional asset folder ID to organize assets
        max_retries: Maximum retry attempts for upload
    
    Returns:
        Asset object dict with 'filename' (URL) and 'fieldtype': 'asset', or None if failed
    """
    if not image_path:
        return None

    # Resolve path (handle relative paths)
    path_obj = Path(image_path)
    if not path_obj.is_absolute():
        # Try relative to current directory
        if not path_obj.exists():
            # Try relative to script directory
            script_dir = Path(__file__).parent
            path_obj = script_dir / image_path
    
    if not path_obj.exists():
        client.logger.warning(f"⚠️ Image file not found: {image_path}")
        return None

    last_err = None
    for attempt in range(1, max_retries + 1):
        try:
            # Read file
            file_bytes = path_obj.read_bytes()
            
            # Determine MIME type
            mime_type, _ = mimetypes.guess_type(str(path_obj))
            if not mime_type or not mime_type.startswith("image/"):
                # Fallback to extension-based detection
                ext = path_obj.suffix.lower()
                if ext == ".png":
                    mime_type = "image/png"
                elif ext in (".jpg", ".jpeg"):
                    mime_type = "image/jpeg"
                elif ext == ".gif":
                    mime_type = "image/gif"
                elif ext == ".webp":
                    mime_type = "image/webp"
                else:
                    mime_type = "image/jpeg"  # Default fallback
            
            # Get filename
            filename = path_obj.name
            if not filename:
                filename = f"image-{int(time.time())}{client._ext_from_mime(mime_type)}"

            # Create signed upload URL
            signed = client.create_signed_asset(filename, asset_folder_id)
            
            asset_id = signed.get("id") or (signed.get("asset") or {}).get("id")
            signed_payload = signed.get("data") or signed

            # Upload to S3
            client.upload_asset_from_bytes(signed_payload, file_bytes, filename, mime_type)

            # Get public URL
            fields = signed_payload.get("fields") or {}
            key = fields.get("key")
            if not key:
                raise RuntimeError("Could not determine uploaded asset key")
            public_url = f"https://a.storyblok.com/{key}"

            # Return asset object
            asset_obj = {"filename": public_url, "fieldtype": "asset"}
            if asset_id:
                asset_obj["id"] = int(asset_id)

            client.logger.info(f"✅ Uploaded image: {filename} -> {public_url}")
            return asset_obj

        except (SSLError, Timeout, ConnectionError) as e:
            last_err = e
            client.logger.warning(f"⚠️ IMAGE UPLOAD RETRY {attempt}/{max_retries}: {image_path} | {type(e).__name__}")
            if attempt < max_retries:
                time.sleep(1.5 * attempt)
            continue
        except Exception as e:
            last_err = e
            client.logger.error(f"❌ IMAGE UPLOAD FAILED: {image_path} | {type(e).__name__}: {e}")
            return None

    client.logger.error(f"❌ IMAGE UPLOAD FAILED after retries: {image_path} | last_error={type(last_err).__name__}")
    return None


def create_storyblok_story(
    client: StoryblokClient,
    title: str,
    description: str,
    image_asset: Optional[dict],
    parent_id: int = 0,
    publish: bool = False,
) -> Optional[dict]:
    """
    Create a Storyblok story with title, description, and image.
    
    Args:
        client: StoryblokClient instance
        title: Story title
        description: Combined description (description + body_text)
        image_asset: Asset object from upload_image_to_storyblok() or None
        publish: Whether to publish the story immediately
    
    Returns:
        Created story dict or None if failed
    """
    # Generate slug from title
    base_slug = slugify(title)
    slug_candidates = [
        base_slug,
        f"{base_slug}-{random.randint(1000, 9999)}",
        f"{base_slug}-{int(time.time())}",
    ]

    # Build content
    content = {
        "component": CONTENT_TYPE,
        FIELD_TITLE: title,
        FIELD_DESCRIPTION: description,
    }
    
    if image_asset:
        content[FIELD_IMAGE] = image_asset

    # Try creating story with different slugs if needed
    last_err = None
    for slug in slug_candidates:
        try:
            result = client.create_story(title, slug, content, parent_id=parent_id, publish=publish)
            created_story = result.get("story") or result
            story_id = created_story.get("id")
            story_slug = created_story.get("slug")
            
            client.logger.info(f"✅ Created story: {title}")
            client.logger.info(f"   Story ID: {story_id}")
            client.logger.info(f"   Slug: {story_slug}")
            
            return created_story
        except HTTPError as he:
            last_err = he
            resp = getattr(he, "response", None)
            if resp is not None and resp.status_code == 422:
                error_text = (resp.text or "").lower()
                if "already taken" in error_text or "slug" in error_text:
                    client.logger.warning(f"⚠️ Slug conflict for '{slug}' -> trying next...")
                    continue
            # Other HTTP errors => fail
            client.logger.error(f"❌ STORY CREATION FAILED (HTTP {resp.status_code if resp else '??'}): {he}")
            return None
        except Exception as e:
            last_err = e
            client.logger.error(f"❌ STORY CREATION FAILED: {type(e).__name__}: {e}")
            return None

    client.logger.error(f"❌ STORY CREATION FAILED after trying all slugs")
    return None


# ----------------------------
# Main
# ----------------------------
def main():
    logger = setup_logging()

    # Load .env from script dir, parent dir, then cwd (fallback works without python-dotenv)
    script_dir = Path(__file__).resolve().parent
    for env_path in [script_dir / ".env", script_dir.parent / ".env", Path.cwd() / ".env"]:
        _load_env_file(env_path)
    if load_dotenv:
        load_dotenv(script_dir / ".env")
        load_dotenv(script_dir.parent / ".env")
        load_dotenv()

    # Get environment variables
    token = (os.getenv("STORYBLOK_TOKEN") or "").strip()
    space_id_str = (os.getenv("STORYBLOK_SPACE_ID") or "").strip()

    if not token:
        logger.error("Missing STORYBLOK_TOKEN env var. Set it in .env or environment.")
        sys.exit(1)

    if not space_id_str:
        logger.error("Missing STORYBLOK_SPACE_ID env var. Set it in .env or environment.")
        sys.exit(1)

    try:
        space_id = int(space_id_str)
    except ValueError:
        logger.error(f"Invalid STORYBLOK_SPACE_ID: {space_id_str} (must be an integer)")
        sys.exit(1)

    # Parse arguments
    ap = argparse.ArgumentParser(description="Upload success story JSON files to Storyblok")
    ap.add_argument("json_file", help="Path to JSON file (created by scraper.py)")
    ap.add_argument("--publish", action="store_true", help="Publish story immediately (default: draft)")
    ap.add_argument("--asset-folder-id", type=int, help="Optional asset folder ID to organize images")
    args = ap.parse_args()

    json_path = Path(args.json_file)
    if not json_path.exists():
        logger.error(f"JSON file not found: {json_path}")
        sys.exit(1)

    # Read JSON file
    try:
        with open(json_path, "r", encoding="utf-8") as f:
            data = json.load(f)
    except Exception as e:
        logger.error(f"Failed to read JSON file: {e}")
        sys.exit(1)

    # Extract fields
    title = data.get("title", "").strip()
    description = data.get("description", "").strip()
    body_text = data.get("body_text", "").strip()
    hero_image = data.get("hero_image", "").strip() if data.get("hero_image") else None

    if not title:
        logger.error("JSON file missing 'title' field")
        sys.exit(1)

    # Combine description and body_text
    final_description = description
    if body_text:
        if final_description:
            final_description = final_description + "\n\n" + body_text
        else:
            final_description = body_text

    logger.info(f"Processing: {title}")
    logger.info(f"Description length: {len(final_description)} chars")
    logger.info(f"Hero image: {hero_image if hero_image else 'None'}")

    # Initialize client
    client = StoryblokClient(token, space_id, logger)

    # Ensure content folder exists and get parent_id
    logger.info(f"Ensuring content folder path: {CONTENT_PATH if CONTENT_PATH else 'root'}")
    content_parent_id = client.ensure_content_folder_by_path(CONTENT_PATH)
    logger.info(f"Content folder ID: {content_parent_id}")

    # Resolve hero_image path relative to JSON file (scraper saves paths like "output/images/...")
    hero_image_path = None
    if hero_image:
        p1 = json_path.parent / hero_image
        p2 = json_path.parent.parent / hero_image
        if p1.exists():
            hero_image_path = str(p1)
        elif p2.exists():
            hero_image_path = str(p2)
        else:
            hero_image_path = hero_image  # use as-is (absolute or cwd-relative)

    # Upload image if provided
    image_asset = None
    if hero_image_path:
        logger.info(f"Uploading image: {hero_image_path}")
        image_asset = upload_image_to_storyblok(client, hero_image_path, args.asset_folder_id)
        if not image_asset:
            logger.warning("⚠️ Image upload failed, continuing without image...")

    # Create story
    logger.info("Creating story in Storyblok...")
    story = create_storyblok_story(client, title, final_description, image_asset, parent_id=content_parent_id, publish=args.publish)

    if story:
        story_id = story.get("id")
        story_slug = story.get("slug")
        editor_url = f"https://app.storyblok.com/#/me/spaces/{space_id}/stories/0/0/{story_id}"
        
        logger.info("\n================ SUCCESS ================")
        logger.info(f"Story ID: {story_id}")
        logger.info(f"Slug: {story_slug}")
        logger.info(f"Editor: {editor_url}")
        logger.info("=========================================")
    else:
        logger.error("\n================ FAILED ================")
        logger.error("Failed to create story in Storyblok")
        logger.error("=========================================")
        sys.exit(1)


if __name__ == "__main__":
    main()
