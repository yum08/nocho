# Telegram Scraper

Multi-backend Telegram channel scraper supporting **Apify cloud** and **Telethon** (personal account).

## Scrapers

| Script | Backend | Use Case |
|--------|---------|----------|
| `apify_telegram_scraper.py` | Apify REST API | **Recommended** — Cloud-based, no personal credentials |
| `unified_scraper.py` | Auto (Apify / Telethon) | Hybrid — auto-selects best backend |
| `scrape_telegram.py` | Telethon | Private channels, personal account required |

## Quick Start

### 1. Install dependencies

```bash
pip install -r requirements.txt
```

### 2. Set up credentials

Create a `.env` file in the project root:

```env
# For Apify (recommended for public channels)
APIFY_API_TOKEN=your_apify_token_here

# For Telethon (required for private channels)
# TELEGRAM_API_ID=your_api_id
# TELEGRAM_API_HASH=your_api_hash
```

Get credentials:
- **Apify**: https://console.apify.com/account/integrations
- **Telegram**: https://my.telegram.org

### 3. Run the scraper

**Apify Scraper (recommended):**

```bash
# Last 10 posts from a channel
python scripts/apify_telegram_scraper.py --channels DGPIndia --max-posts 10

# Multiple channels, last 7 days, all output formats
python scripts/apify_telegram_scraper.py --channels durov telegram --days 7 \
  --out messages.csv --out-json messages.json --out-excel messages.xlsx

# 200 posts with media download
python scripts/apify_telegram_scraper.py --channels channel_name --max-posts 200 --download-media

# Use the post-range actor for large scrapes
python scripts/apify_telegram_scraper.py --channels channel_name --actor posts --posts-from 1 --posts-to 500
```

**Unified Scraper:**

```bash
# Auto-select backend
python scripts/unified_scraper.py --channels channel1 channel2 --limit 500

# Force Telethon for private channels
python scripts/unified_scraper.py --channels private_channel --backend telethon

# With keyword and view filters
python scripts/unified_scraper.py --channels channel --keywords bitcoin crypto --min-views 1000

# Generate config file
python scripts/unified_scraper.py --generate-config config.json
```

**Original Telethon Scraper:**

```bash
python scripts/scrape_telegram.py --channel channel_name --limit 1000 --out messages.csv
```

## Apify Actor Backends

The Apify scraper supports 3 actor backends:

| Actor | Flag | Best For |
|-------|------|----------|
| `webfinity/telegram-channel-content-media-scraper-v2` | `--actor media` (default) | Quick scrapes, up to 200 posts, media support |
| `danielmilevski9/telegram-channel-scraper` | `--actor posts` | Large scrapes with post range selection |
| `cheapget/telegram-channel-message` | `--actor messages` | Date-filtered message extraction |

## Feature Comparison

| Feature | Apify Scraper | Unified | Telethon |
|---------|--------------|---------|----------|
| Public channels | ✅ | ✅ | ✅ |
| Private channels | ❌ | ✅ | ✅ |
| No personal account needed | ✅ | ✅ (via Apify) | ❌ |
| Proxy rotation | ✅ (residential) | ✅ | ❌ |
| Cloud execution | ✅ | ✅ | ❌ |
| Multiple channels | ✅ | ✅ | ❌ |
| Date filtering | ✅ | ✅ | ✅ |
| Keyword filtering | ❌ | ✅ | ❌ |
| Media download | ✅ | ❌ | ✅ |
| CSV / JSON / Excel export | ✅ | ✅ | CSV only |

## Output Format

| Column | Description |
|--------|-------------|
| `id` | Post/message ID |
| `channel` | Channel username |
| `date` | Post timestamp (ISO 8601) |
| `text` | Message text content |
| `views` | View count |
| `forwards` | Forward count |
| `replies` | Reply/comment count |
| `url` | Direct link to the post |
| `has_media` | Whether post contains media |
| `media_urls` | Media file URLs (if available) |

## Notes

- Respect Telegram's terms of service and channel owners' privacy
- Apify has usage-based pricing — check your plan at https://console.apify.com
- For private channels, you must be a member and use Telethon
- Rate limiting and retries are handled automatically
- The `truststore` package is used to fix SSL issues on corporate networks
