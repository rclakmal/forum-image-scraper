import os
import re
import hashlib
import configparser
import requests
from bs4 import BeautifulSoup
from urllib.parse import unquote, urljoin
from PIL import Image
from io import BytesIO
from concurrent.futures import ThreadPoolExecutor, as_completed
import tldextract

# Reusable session for connection pooling (faster downloads)
session = requests.Session()
session.headers.update({
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
    'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
    'Accept-Language': 'en-US,en;q=0.9',
    'Accept-Encoding': 'gzip, deflate, br',
    'DNT': '1',
    'Connection': 'keep-alive',
    'Upgrade-Insecure-Requests': '1'
})


def download_image(url, page, folder, url_hash, minWidth, minHeight):
    """Download a single image. Returns (status, filename, message)."""
    filename = f"p{page}_{url_hash}.jpg"
    filepath = os.path.join(folder, filename)
    
    try:
        img_response = session.get(url, timeout=30)
        if img_response.status_code == 200 and len(img_response.content) > 1000:
            # Check image resolution
            if minWidth > 0 or minHeight > 0:
                try:
                    img = Image.open(BytesIO(img_response.content))
                    width, height = img.size
                    if (minWidth > 0 and width < minWidth) or (minHeight > 0 and height < minHeight):
                        return ('too_small', filename, f'{width}x{height}')
                except:
                    pass  # If can't read image, skip resolution check
            
            with open(filepath, 'wb') as f:
                f.write(img_response.content)
            return ('success', filename, len(img_response.content))
        return ('failed', filename, 'Invalid response')
    except Exception as e:
        return ('error', filename, str(e))


def process_thread(hostSite, threadUrl, pageAppenderBefore, pageAppenderAfter, startPage, endPage, pageValueMultiply, minWidth, minHeight):
    """Process a single thread."""
    # Normalize thread URL - handle both full URLs and path-only URLs
    if threadUrl.startswith('http://') or threadUrl.startswith('https://'):
        # Full URL provided - extract the path for folder naming
        full_thread_url = threadUrl.rstrip('/')
        thread_path = threadUrl.split('://', 1)[1]  # Remove scheme
        thread_path = thread_path.split('/', 1)[1] if '/' in thread_path else ''  # Remove domain
    else:
        # Path only - construct full URL using hostSite
        full_thread_url = hostSite.rstrip('/') + '/' + threadUrl.lstrip('/')
        thread_path = threadUrl.lstrip('/')
    
    # Create output folder structure: ../hostname/full/thread/path/
    hostname = tldextract.extract(hostSite).domain
    
    # Use the full thread path to create folder hierarchy
    # Clean each part of the path (remove special chars)
    path_parts = [p for p in thread_path.rstrip('/').split('/') if p]
    cleaned_parts = []
    for part in path_parts:
        # Decode URL encoding and clean special characters
        cleaned = unquote(part)
        cleaned = re.sub(r'[^\w\-]', '_', cleaned)
        # Limit each part to reasonable length
        cleaned = cleaned[:100]
        cleaned_parts.append(cleaned)
    
    # Build full folder path: ../hostname/part1/part2/part3/
    folder = os.path.join('..', hostname, *cleaned_parts)
    os.makedirs(folder, exist_ok=True)

    print(f"\n{'='*60}")
    print(f"Thread: {thread_path}")
    print(f"Pages {startPage} to {endPage}")
    print(f"Output: {folder}")
    print(f"{'='*60}\n")

    # Track all downloaded hashes to avoid duplicates across pages
    downloaded_hashes = set()
    
    # Check existing files and populate hash set
    if os.path.exists(folder):
        for existing_file in os.listdir(folder):
            if existing_file.endswith('.jpg'):
                # Extract hash from filename (format: pX_hash.jpg)
                parts = existing_file.split('_')
                if len(parts) == 2:
                    hash_part = parts[1].replace('.jpg', '')
                    downloaded_hashes.add(hash_part)

    for page in range(startPage, endPage + 1):
        print(f"\n{'─'*60}")
        print(f"PAGE {page}")
        
        page_url = f"{full_thread_url}{pageAppenderBefore}{page * pageValueMultiply}{pageAppenderAfter}"
        print(f"URL: {page_url}")
        
        try:
            response = session.get(page_url, timeout=30)
            response.raise_for_status()
        except Exception as e:
            print(f"ERROR: {e}")
            print(f"{'─'*60}")
            continue

        soup = BeautifulSoup(response.text, 'html.parser')
        urls = set()
        
        # Find all img tags
        for img in soup.find_all('img'):
            src = img.get('src')
            if src and not src.startswith('data:'):  # Skip data URLs
                url = urljoin(hostSite, src)
                # Only include http/https URLs
                if url.startswith(('http://', 'https://')):
                    urls.add(url)
        
        # Find all links to images
        for a in soup.find_all('a', href=re.compile(r'\.(jpg|jpeg|png|gif)$', re.I)):
            href = a.get('href')
            if href and not href.startswith('data:'):  # Skip data URLs
                url = urljoin(hostSite, href)
                if url.startswith(('http://', 'https://')):
                    urls.add(url)

        print(f"Found: {len(urls)} images")
        downloaded = 0
        skipped = 0
        too_small = 0
        
        # Prepare download tasks
        tasks = []
        for url in urls:
            url_hash = hashlib.md5(url.encode()).hexdigest()[:8]
            
            # Check if this hash was already downloaded (on any page)
            if url_hash in downloaded_hashes:
                skipped += 1
                continue
            
            tasks.append((url, page, folder, url_hash, minWidth, minHeight))
        
        # Download images in parallel (max 10 concurrent downloads)
        with ThreadPoolExecutor(max_workers=10) as executor:
            futures = {executor.submit(download_image, *task): task for task in tasks}
            
            for future in as_completed(futures):
                status, filename, info = future.result()
                
                if status == 'success':
                    url_hash = filename.split('_')[1].replace('.jpg', '')
                    downloaded_hashes.add(url_hash)
                    downloaded += 1
                    print(f"  ✓ {filename} ({info} bytes)")
                elif status == 'too_small':
                    too_small += 1
                elif status == 'error':
                    print(f"  ✗ {filename} - {info}")

        print(f"\nSummary:")
        if downloaded > 0:
            print(f"  Downloaded: {downloaded}")
        if skipped > 0:
            print(f"  Skipped (already have): {skipped}")
        if too_small > 0:
            print(f"  Skipped (too small): {too_small}")
        print(f"{'─'*60}")


def main():
    config = configparser.RawConfigParser()
    with open('input.properties', encoding="utf-8") as f:
        config.read_file(f)

    hostSite = config.get("UserInput", "hostSite")
    threads_raw = config.get("UserInput", "thread")
    pageAppenderBefore = config.get("UserInput", "pageAppenderBefore")
    pageAppenderAfter = config.get("UserInput", "pageAppenderAfter", fallback="")
    startPage = int(config.get("UserInput", "startPage"))
    endPage = int(config.get("UserInput", "endPage"))
    pageValueMultiply = int(config.get("UserInput", "pageValueMultiply", fallback="1"))
    minWidth = int(config.get("UserInput", "minWidth", fallback="0"))
    minHeight = int(config.get("UserInput", "minHeight", fallback="0"))

    # Parse comma-separated threads
    thread_urls = [t.strip() for t in threads_raw.split(',') if t.strip()]
    
    print(f"Forum: {hostSite}")
    print(f"Processing {len(thread_urls)} thread(s)")
    
    # Process each thread
    for threadUrl in thread_urls:
        process_thread(hostSite, threadUrl, pageAppenderBefore, pageAppenderAfter, 
                      startPage, endPage, pageValueMultiply, minWidth, minHeight)
    
    print(f"\n{'='*60}")
    print("All threads completed!")
    print(f"{'='*60}\n")


if __name__ == '__main__':
    main()
