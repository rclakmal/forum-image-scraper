# Forum Image Scraper

Download images from paginated forum threads with parallel processing and duplicate detection.
No support for forums that require login or forums protected by services like Cloudflare. 

## Quick Start

1. Install dependencies: `pip install -r requirements.txt`
2. **Configure `input.properties`** - This file is required and contains all settings (forum URL, threads, page range, filters)
3. Run: `python forum-image-scraper.py`

## Output Structure

Images are saved in a hierarchical folder structure matching the thread path:
```
../hostname/
  └── path/
      └── to/
          └── thread/
              ├── p1_abc12345.jpg
              ├── p1_def67890.jpg
              ├── p2_ghi11223.jpg
              └── ...
```

- `hostname`: Extracted domain name from the forum
- Full path hierarchy created from thread URL
- `pX_hash.jpg`: Page number and unique hash identifier

## Features

- Parallel downloads (10 workers) for speed
- Resolution filtering to skip small images
- Hash-based duplicate detection across all pages
- Progress feedback and statistics
