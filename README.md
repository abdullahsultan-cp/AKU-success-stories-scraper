# Success Stories Scraper

A dedicated scraper for AKU Pakistan Success Stories section.

Scrapes and saves success story data (title, body text, images) locally as JSON files.

## Prerequisites

- Python 3.7+
- pip (Python package manager)

## Setup

```bash
# Install dependencies
pip install -r requirements.txt

# Or install manually
pip install requests beautifulsoup4 python-dotenv
```

## Usage

**Scrape single story:**
```bash
python scraper.py --link "https://hospitals.aku.edu/pakistan/success-stories/Pages/a-womans-dream.aspx"
```

**Scrape from file (one URL per line):**
```bash
python scraper.py --links-file all_urls.txt
```

**Combine both:**
```bash
python scraper.py --link "url1" --link "url2" --links-file all_urls.txt
```

## Output

Creates a timestamped folder like `ss_scrape_2026-02-08_115122PM/` with:
- `output/` - JSON files (story_1.json, story_2.json, ...)
- `summary.json` - Stats and file list
- `links_used.txt` - All URLs processed

Each JSON file contains:
- `source_url` - Original page URL
- `title` - Article title
- `date` - Publication date (if found)
- `description` - Meta description
- `body_text` - Full article text (all paragraphs)
- `hero_image` - Main story image URL
