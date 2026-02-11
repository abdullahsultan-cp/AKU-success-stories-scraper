# Success Stories – Scrape & Upload to Storyblok

Scrape AKU Pakistan Success Stories and upload them to Storyblok. One script does both.

## Prerequisites

- Python 3.7+
- pip

## Setup

```bash
pip install -r requirements.txt
# Or: pip install requests beautifulsoup4 python-dotenv
```

Create a `.env` in this folder (required for upload):

```
STORYBLOK_TOKEN=your_management_api_token
STORYBLOK_SPACE_ID=your_space_id
```

---

## Combined script: `run.py`

**Scrape and upload in one go (default):**

```bash
# One URL
python run.py --link "https://hospitals.aku.edu/pakistan/success-stories/Pages/a-journey-of-strength.aspx"

# From file (one URL per line)
python run.py --links-file links.txt

# Multiple URLs
python run.py --link "url1" --link "url2" --links-file links.txt
```

This creates a timestamped folder (e.g. `ss_scrape_2026-02-11_025239AM/`), saves JSON + images under `output/`, then uploads each story to Storyblok under **Root → Automation → success-stories**.

**Scrape only (no upload):**

```bash
python run.py --links-file links.txt --scrape-only
```

**Upload only** (e.g. re-upload a previous run):

```bash
# Upload all JSONs in a folder
python run.py --upload-only "ss_scrape_2026-02-11_025239AM/output"

# Upload a single JSON file
python run.py --upload-only "ss_scrape_2026-02-11_025239AM/output/Story_Title.json"
```

**Options:**

| Option | Description |
|--------|-------------|
| `--publish` | Publish stories in Storyblok (default: draft) |
| `--asset-folder-id ID` | Put uploaded images in this Storyblok asset folder |

---

## Output (scrape)

Each run folder contains:

- **output/** – one JSON per story (filename from title) and **output/images/** with hero images
- **summary.json** – success/fail counts
- **links_used.txt** – URLs processed

JSON fields: `source_url`, `title`, `date`, `description`, `body_text` (paragraphs with `\n\n`), `hero_image` (local path).

Paragraph structure from the page is preserved (intro, body, author bio), including lines in `<em>`.

---

## Config in `run.py`

- **CONTENT_PATH** – Storyblok folder path (default: `["Automation", "success-stories"]`)
- **CONTENT_TYPE** – Storyblok content type (default: `"success_story"`)
- **FIELD_TITLE**, **FIELD_DESCRIPTION**, **FIELD_IMAGE** – field names (change if your schema differs)

---

## Separate scripts (optional)

- **scraper.py** – scrape only (same behavior as `run.py --scrape-only`)
- **uploader.py** – upload a single JSON (same as `run.py --upload-only path/to/file.json`)

Use them if you prefer to keep scrape and upload as separate steps or scripts.
