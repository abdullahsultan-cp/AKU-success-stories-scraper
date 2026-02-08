#!/usr/bin/env python
# scraper.py
# Success Stories scraper for AKU
# Scrapes stories and saves as JSON locally

import argparse
import json
import logging
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
from bs4 import BeautifulSoup, Tag

try:
    from dotenv import load_dotenv
except Exception:
    load_dotenv = None


# ----------------------------
# Run folders & Logging
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


def setup_logging(paths: RunPaths) -> logging.Logger:
    logger = logging.getLogger("ss_scraper")
    logger.setLevel(logging.INFO)
    logger.handlers.clear()

    fmt = logging.Formatter("%(asctime)s | %(levelname)s | %(message)s")

    # console log
    sh = logging.StreamHandler(sys.stdout)
    sh.setLevel(logging.INFO)
    sh.setFormatter(fmt)
    logger.addHandler(sh)

    logger.info(f"Run folder: {paths.root}")
    return logger


# ----------------------------
# Helpers
# ----------------------------
def safe_text(s: str) -> str:
    s = (s or "").replace("\xa0", " ")
    s = re.sub(r"\s+", " ", s).strip()
    return s


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
# Scraper functions
# ----------------------------
def extract_title(soup: BeautifulSoup) -> str:
    h1 = soup.find('h1')
    if h1:
        return safe_text(h1.get_text())
    
    og_title = soup.find('meta', property='og:title')
    if og_title:
        return safe_text(og_title.get('content', ''))
    
    title_tag = soup.find('title')
    if title_tag:
        return safe_text(title_tag.get_text())
    
    return "Untitled"


def extract_description(soup: BeautifulSoup) -> str:
    og_desc = soup.find('meta', property='og:description')
    if og_desc:
        return safe_text(og_desc.get('content', ''))
    
    meta_desc = soup.find('meta', attrs={'name': 'description'})
    if meta_desc:
        return safe_text(meta_desc.get('content', ''))
    
    return ""


def extract_hero_image(soup: BeautifulSoup, page_base: str) -> Optional[str]:
    # Look for images in the main content area first (more reliable than og:image)
    content_div = soup.find('div', class_='ContentMain') or soup.find('div', class_='MainContentZone')
    
    search_area = content_div if content_div else soup
    
    # Skip tracking pixels and generic assets
    skip_patterns = ['facebook.com/tr', 'google-analytics', 'pixel', 'doubleclick', 'logo', 'icon', 'avatar', '_layouts', 'spcommon', 'siteassets']
    
    for img in search_area.find_all('img', src=True):
        src = img.get('src', '')
        if not src:
            continue
            
        # Skip if matches skip patterns
        if any(x in src.lower() for x in skip_patterns):
            continue
        
        # Only accept image formats
        if not src.lower().endswith(('.png', '.jpg', '.jpeg', '.gif', '.webp')):
            continue
        
        url = normalize_url(src, page_base)
        if url:
            return url
    
    return None


def extract_body_text(soup: BeautifulSoup) -> str:
    # Find the main content area
    content_div = soup.find('div', class_='ContentMain') or soup.find('div', class_='MainContentZone')
    
    if not content_div:
        content_div = soup.find('div', class_=lambda x: x and 'content' in str(x).lower())
    
    if not content_div:
        # Fallback: get all paragraphs
        ps = soup.find_all('p')
    else:
        ps = content_div.find_all('p')
    
    paragraphs = []
    for p in ps:
        text = safe_text(p.get_text())
        # Skip short lines (likely nav/footer)
        if text and len(text) > 20:
            paragraphs.append(text)
    
    return '\n\n'.join(paragraphs)


def extract_date(soup: BeautifulSoup) -> Optional[str]:
    # Look for publish date meta tag
    pub_date = soup.find('meta', attrs={'property': 'article:published_time'})
    if pub_date:
        date_str = pub_date.get('content', '')
        if date_str:
            # Extract just the date part (YYYY-MM-DD)
            match = re.search(r'(\d{4}-\d{2}-\d{2})', date_str)
            if match:
                return match.group(1)
    
    # Try to find date in a modified/published date element
    for el in soup.find_all(['span', 'div'], class_=lambda x: x and any(d in str(x).lower() for d in ['date', 'published', 'modified'])):
        text = safe_text(el.get_text())
        match = re.search(r'(\d{4}-\d{2}-\d{2})', text)
        if match:
            return match.group(1)
    
    return None


def parse_story_html(html: str, page_url: str) -> SuccessStory:
    soup = BeautifulSoup(html, 'html.parser')
    page_base = get_page_base(page_url)

    title = extract_title(soup)
    description = extract_description(soup)
    body_text = extract_body_text(soup)
    hero_image = extract_hero_image(soup, page_base)
    date_str = extract_date(soup)

    return SuccessStory(
        source_url=page_url,
        title=title,
        date=date_str,
        description=description,
        body_text=body_text,
        hero_image=hero_image,
    )


def fetch_html(url: str, timeout: int = 60) -> str:
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
        'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
        'Accept-Language': 'en-US,en;q=0.9',
        'Connection': 'keep-alive',
    }
    r = requests.get(url, headers=headers, timeout=timeout)
    r.raise_for_status()
    return r.text


# ----------------------------
# Links file
# ----------------------------
def read_links_file(path: str) -> List[str]:
    p = Path(path)
    if not p.exists():
        raise FileNotFoundError(f"links file not found: {path}")

    urls = []
    for line in p.read_text(encoding='utf-8', errors='ignore').splitlines():
        line = line.strip()
        if not line or line.startswith('#'):
            continue
        urls.append(line)
    
    # dedupe preserve order
    seen = set()
    out = []
    for u in urls:
        if u not in seen:
            seen.add(u)
            out.append(u)
    return out


# ----------------------------
# Main
# ----------------------------
def main():
    paths = setup_run_dirs()
    logger = setup_logging(paths)

    if load_dotenv:
        load_dotenv()

    ap = argparse.ArgumentParser(description='Success Stories scraper - saves JSON locally')
    ap.add_argument('--link', action='append', help='Story link (repeatable)')
    ap.add_argument('--links-file', default='', help='Path to links.txt (one URL per line)')
    args = ap.parse_args()

    urls: List[str] = []
    if args.links_file:
        urls.extend(read_links_file(args.links_file))
    if args.link:
        urls.extend(args.link)

    # dedupe preserve order
    seen = set()
    dedup = []
    for u in urls:
        u = (u or '').strip()
        if u and u not in seen:
            seen.add(u)
            dedup.append(u)
    urls = dedup

    if not urls:
        logger.info('No URLs provided. Use --link or --links-file links.txt')
        return

    logger.info(f'Total URLs: {len(urls)}')
    (paths.root / 'links_used.txt').write_text('\n'.join(urls), encoding='utf-8')

    summary = {
        'total': len(urls),
        'success': 0,
        'failed': 0,
        'errors': [],
        'saved_files': [],
    }

    for idx, url in enumerate(urls, start=1):
        logger.info(f'\n[{idx}/{len(urls)}] Scraping: {url}')

        try:
            html = fetch_html(url)
        except HTTPError as he:
            resp = getattr(he, 'response', None)
            code = resp.status_code if resp is not None else None
            logger.error(f'❌ PAGE SKIP (HTTP {code}): {url}')
            summary['failed'] += 1
            summary['errors'].append({'url': url, 'type': 'HTTPError', 'code': code})
            continue
        except (Timeout, ConnectionError, SSLError) as e:
            logger.error(f'❌ PAGE SKIP (Network): {url} | {type(e).__name__}')
            summary['failed'] += 1
            summary['errors'].append({'url': url, 'type': type(e).__name__})
            continue
        except Exception as e:
            logger.error(f'❌ PAGE SKIP (Unexpected): {url} | {type(e).__name__}: {e}')
            summary['failed'] += 1
            summary['errors'].append({'url': url, 'type': type(e).__name__, 'msg': str(e)})
            continue

        # Parse
        story = parse_story_html(html, url)

        # Save as JSON
        output_file = paths.output / f'story_{idx}.json'
        output_file.write_text(
            json.dumps(
                {
                    'source_url': story.source_url,
                    'title': story.title,
                    'date': story.date,
                    'description': story.description,
                    'body_text': story.body_text,
                    'hero_image': story.hero_image,
                },
                ensure_ascii=False,
                indent=2,
            ),
            encoding='utf-8',
        )

        logger.info(f'✅ Saved: {output_file.name}')
        logger.info(f'   Title: {story.title}')
        logger.info(f'   Date: {story.date}')
        logger.info(f'   Hero image: {story.hero_image is not None}')
        logger.info(f'   Body text length: {len(story.body_text)} chars')

        summary['success'] += 1
        summary['saved_files'].append({
            'url': url,
            'filename': output_file.name,
            'title': story.title,
        })

    # Save summary
    summary_file = paths.root / 'summary.json'
    summary_file.write_text(json.dumps(summary, ensure_ascii=False, indent=2), encoding='utf-8')

    logger.info('\n================ SUMMARY ================')
    logger.info(f'Total URLs    : {summary["total"]}')
    logger.info(f'Success       : {summary["success"]}')
    logger.info(f'Failed        : {summary["failed"]}')
    logger.info(f'Output folder : {paths.output}')
    logger.info(f'Summary saved : {summary_file}')
    logger.info('=========================================')


if __name__ == '__main__':
    main()
