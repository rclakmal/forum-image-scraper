import os
import re
import hashlib
import configparser
import csv
import requests
from bs4 import BeautifulSoup
from urllib.parse import unquote, urljoin, urlparse
from PIL import Image
from io import BytesIO
from concurrent.futures import ThreadPoolExecutor, as_completed
import tldextract
import shutil
import sys

# Reusable session for connection pooling (faster downloads)
session = requests.Session()
session.headers.update({
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'
})


def download_image(url, page, folder, url_hash, minWidth, minHeight):
    """Download a single image. Returns (status, filename, message)."""
    # Detect file extension from URL
    is_svg = url.lower().endswith('.svg') or '.svg?' in url.lower()
    ext = '.svg' if is_svg else '.jpg'
    filename = f"p{page}_{url_hash}{ext}"
    filepath = os.path.join(folder, filename)
    
    try:
        img_response = session.get(url, timeout=30)
        if img_response.status_code == 200 and len(img_response.content) > 1000:
            # Check image resolution (skip for SVG files)
            if not is_svg and (minWidth > 0 or minHeight > 0):
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


def smart_truncate_url(url, max_width):
    """Truncate URL smartly: domain/.../filename"""
    try:
        parsed = urlparse(url)
        domain = parsed.netloc
        path_parts = [p for p in parsed.path.split('/') if p]
        
        if not path_parts:
            return url[:max_width]
        
        filename = path_parts[-1]
        # Try to fit: domain/.../filename
        short = f"{domain}/.../{filename}"
        
        if len(short) <= max_width:
            return short
        
        # If still too long, truncate filename
        available = max_width - len(domain) - 5  # 5 for "/.../"
        if available > 10:
            return f"{domain}/.../{filename[:available-3]}..."
        
        # Last resort: just truncate entire URL
        return url[:max_width-3] + '...'
    except:
        return url[:max_width]

def wrap_text(text, max_width):
    """Wrap text to fit within max_width, breaking at word boundaries."""
    if len(text) <= max_width:
        return [text]
    lines = []
    while text:
        if len(text) <= max_width:
            lines.append(text)
            break
        # Find last space within max_width
        break_point = text.rfind(' ', 0, max_width)
        if break_point == -1:
            break_point = max_width
        lines.append(text[:break_point])
        text = text[break_point:].lstrip()
    return lines

def print_table(rows, terminal_width, page_label=None, skip_header=False, print_header_only=False, last_row_count=0):
    """Print a formatted table that adapts to terminal width.
    
    Args:
        rows: List of row tuples to display
        terminal_width: Terminal width for formatting
        page_label: Page number to display in header
        skip_header: If True, skip printing the page label header
        print_header_only: If True, only print page header and table header
        last_row_count: Number of rows already printed (to print only new rows)
    """
    if not rows and not print_header_only:
        return
    
    cols = ['Filename', 'Size', 'Status']
    
    # Calculate column widths
    min_file_width = 25
    min_size_width = 12
    min_status_width = 25
    padding = 2 * 4 + 2  # column separators and borders
    
    # Distribute remaining width
    remaining = terminal_width - min_file_width - min_size_width - min_status_width - padding
    if remaining > 0:
        min_file_width += remaining // 2
        min_status_width += remaining - (remaining // 2)
    
    widths = [min_file_width, min_size_width, min_status_width]
    
    # Print page header if this is initial header print
    if print_header_only:
        if page_label is not None:
            print(f'\n{"═"*60}')
            print(f'Page {page_label} Results:')
            print(f'{"─"*60}\n')
        
        # Print table header
        print('╔' + '═╤═'.join('═' * w for w in widths) + '╗')
        print('║ ' + ' │ '.join(cols[i].ljust(widths[i]) for i in range(len(cols))) + ' ║')
        print('╠' + '═╪═'.join('═' * w for w in widths) + '╣')
        sys.stdout.flush()
        return
    
    # Print only new rows (from last_row_count onwards)
    for i in range(last_row_count, len(rows)):
        r = rows[i]
        file_str = str(r[0])[:widths[0]] if r[0] else ''
        size_str = str(r[1]) if r[1] else ''
        status_str = str(r[2])[:widths[2]]
        
        print('║ ' + file_str.ljust(widths[0]) + ' │ ' + 
              size_str.ljust(widths[1]) + ' │ ' + 
              status_str.ljust(widths[2]) + ' ║')
    
    sys.stdout.flush()

def process_thread(threadUrl, pageAppenderBefore, pageAppenderAfter, startPage, endPage, pageValueMultiply, minWidth, minHeight, outputBasePath='..', maxWorkers=10, usePagination=True, update_callback=None, terminal_width=120):
    """Process a single thread. `threadUrl` must be a full URL (including scheme)."""
    # Require full URL for threadUrl
    if not (threadUrl.startswith('http://') or threadUrl.startswith('https://')):
        return [], [], 0, 0

    # Normalize full thread URL
    full_thread_url = threadUrl.rstrip('/')
    parsed = urlparse(full_thread_url)
    thread_path = parsed.path.lstrip('/')

    # Create output folder structure based on thread host
    hostname = tldextract.extract(full_thread_url).domain
    # base URL for resolving relative src/href
    base_url = f"{parsed.scheme}://{parsed.netloc}"
    
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
    
    # Build full folder path: <outputBasePath>/hostname/part1/part2/part3/
    folder = os.path.join(outputBasePath, hostname, *cleaned_parts)
    os.makedirs(folder, exist_ok=True)

    # Track all downloaded hashes to avoid duplicates across pages
    downloaded_hashes = set()
    # Collect table rows: (filename, size, status) for display
    table_rows = []
    # Collect full details: (url, size, filename, status) for CSV
    csv_rows = []
    skipped_count = 0  # Track skipped items separately
    too_small_count = 0  # Track too_small items separately
    
    # Check existing files and populate hash set
    if os.path.exists(folder):
        for existing_file in os.listdir(folder):
            if existing_file.endswith(('.jpg', '.svg')):
                # Extract hash from filename (format: pX_hash.ext)
                parts = existing_file.split('_')
                if len(parts) == 2:
                    hash_part = parts[1].replace('.jpg', '').replace('.svg', '')
                    downloaded_hashes.add(hash_part)

    pages = None
    infinite_pagination = False
    if usePagination:
        if endPage == 0 or endPage < startPage:
            # Infinite pagination mode - keep going until HTML repeats
            infinite_pagination = True
            pages = None  # Will iterate manually
        else:
            pages = range(startPage, endPage + 1)
    else:
        pages = [None]

    current_page = startPage
    previous_html = None
    
    while True:
        # Determine current page to process
        if not usePagination:
            if current_page > startPage:
                break
            page = None
            page_label = 1
        elif infinite_pagination:
            page = current_page
            page_label = page
        elif pages is not None:
            if current_page > endPage:
                break
            page = current_page
            page_label = page
        else:
            break
        
        if page is None:
            page_url = full_thread_url
        else:
            page_url = f"{full_thread_url}{pageAppenderBefore}{page * pageValueMultiply}{pageAppenderAfter}"

        try:
            response = session.get(page_url, timeout=30)
            response.raise_for_status()
            
            # Check for duplicate HTML (infinite pagination detection)
            if infinite_pagination and previous_html is not None:
                if response.text == previous_html:
                    # Same HTML as previous page, we've reached the end
                    break
            previous_html = response.text if infinite_pagination else None
            
        except Exception as e:
            current_page += 1
            continue

        soup = BeautifulSoup(response.text, 'html.parser')
        urls = set()
        
        # Track rows for this page only
        page_table_rows = []
        page_csv_rows = []
        page_header_printed = False
        last_printed_count = 0
        
        # Find all img tags
        for img in soup.find_all('img'):
            src = img.get('src')
            if src and not src.startswith('data:'):  # Skip data URLs
                url = urljoin(base_url, src)
                # Only include http/https URLs
                if url.startswith(('http://', 'https://')):
                    urls.add(url)
        
        # Find all links to images (including SVG)
        for a in soup.find_all('a', href=re.compile(r'\.(jpg|jpeg|png|gif|svg)(\?|$)', re.I)):
            href = a.get('href')
            if href and not href.startswith('data:'):  # Skip data URLs
                url = urljoin(base_url, href)
                if url.startswith(('http://', 'https://')):
                    urls.add(url)

        # Prepare download tasks
        tasks = []
        for url in urls:
            url_hash = hashlib.md5(url.encode()).hexdigest()[:8]

            # Check if this hash was already downloaded (on any page)
            if url_hash in downloaded_hashes:
                skipped_count += 1
                continue

            tasks.append((url, page_label, folder, url_hash, minWidth, minHeight))
        
        # Download images in parallel (controlled by maxWorkers)
        with ThreadPoolExecutor(max_workers=maxWorkers) as executor:
            futures = {executor.submit(download_image, *task): task for task in tasks}
            
            for future in as_completed(futures):
                task = futures[future]
                orig_url = task[0]
                orig_hash = task[3]
                status, filename, info = future.result()

                if status == 'success':
                    downloaded_hashes.add(orig_hash)
                    page_table_rows.append((filename, str(info), '✓'))
                    page_csv_rows.append((orig_url, str(info), filename, '✓'))
                elif status == 'too_small':
                    too_small_count += 1
                    page_csv_rows.append((orig_url, '', filename, f'too_small {info}'))
                elif status == 'failed':
                    page_table_rows.append((filename, '', f'failed {info}'))
                    page_csv_rows.append((orig_url, '', filename, f'failed {info}'))
                elif status == 'error':
                    page_table_rows.append((filename, '', f'error {info}'))
                    page_csv_rows.append((orig_url, '', filename, f'error {info}'))
                
                # Update table after each download completes
                if update_callback and page_table_rows:
                    if not page_header_printed:
                        # Print header once at the start
                        update_callback(page_label, page_table_rows, print_header_only=True)
                        page_header_printed = True
                    # Append only new rows
                    update_callback(page_label, page_table_rows, last_row_count=last_printed_count)
                    last_printed_count = len(page_table_rows)
        
        # Add page results to total
        table_rows.extend(page_table_rows)
        csv_rows.extend(page_csv_rows)
        
        # Print table footer after all downloads for this page are complete
        if page_table_rows:
            # Calculate column widths for footer
            min_file_width = 25
            min_size_width = 12
            min_status_width = 25
            padding = 2 * 4 + 2
            remaining = terminal_width - min_file_width - min_size_width - min_status_width - padding
            if remaining > 0:
                min_file_width += remaining // 2
                min_status_width += remaining - (remaining // 2)
            widths = [min_file_width, min_size_width, min_status_width]
            print('╚' + '═╧═'.join('═' * w for w in widths) + '╝')
            sys.stdout.flush()
        
        current_page += 1

    # Return collected table rows, CSV rows, skipped count, and too_small count for this thread
    return table_rows, csv_rows, skipped_count, too_small_count


def main():
    config = configparser.RawConfigParser()
    # Prefer `forum.properties`; fall back to legacy `input.properties` if present.
    forum_props = 'forum.properties'
    input_props = 'input.properties'
    download_props = 'download.properties'

    if os.path.exists(forum_props):
        with open(forum_props, encoding="utf-8") as f:
            config.read_file(f)
        section = 'ForumSettings'
    elif os.path.exists(input_props):
        with open(input_props, encoding="utf-8") as f:
            config.read_file(f)
        section = 'UserInput'
    else:
        print("ERROR: No configuration file found. Please create 'forum.properties' or 'input.properties'.")
        return

    # Read forum-level settings
    try:
        threads_raw = config.get(section, "thread")
        pageAppenderBefore = config.get(section, "pageAppenderBefore")
        pageAppenderAfter = config.get(section, "pageAppenderAfter", fallback="")
        startPage = int(config.get(section, "startPage"))
        endPage = int(config.get(section, "endPage"))
        pageValueMultiply = int(config.get(section, "pageValueMultiply", fallback="1"))
    except Exception as e:
        print(f"ERROR: Missing or invalid forum properties: {e}")
        return

    # Read download settings (optional)
    download_config = configparser.RawConfigParser()
    outputBasePath = '..'
    minWidth = 0
    minHeight = 0
    maxWorkers = 10
    if os.path.exists(download_props):
        try:
            with open(download_props, encoding="utf-8") as f:
                download_config.read_file(f)
            dsec = 'DownloadSettings'
            outputBasePath = download_config.get(dsec, 'outputBasePath', fallback=outputBasePath)
            minWidth = int(download_config.get(dsec, 'minWidth', fallback=str(minWidth)))
            minHeight = int(download_config.get(dsec, 'minHeight', fallback=str(minHeight)))
            maxWorkers = int(download_config.get(dsec, 'maxWorkers', fallback=str(maxWorkers)))
        except Exception as e:
            print(f"WARNING: Could not read '{download_props}': {e}. Using defaults.")

    # Parse comma-separated threads
    thread_urls = [t.strip() for t in threads_raw.split(',') if t.strip()]
    
    print(f"Processing {len(thread_urls)} thread(s)")

    # Determine whether to use pagination (true/false) and process threads
    usePagination = config.get(section, 'usePagination', fallback='true').lower() == 'true'

    # Get terminal width
    terminal_width = shutil.get_terminal_size((120, 40)).columns
    
    # Aggregate rows from all threads with per-page table updates
    all_table_rows = []
    all_csv_rows = []
    total_skipped = 0
    total_too_small = 0
    
    def page_callback(page_num, page_rows, skip_header=False, print_header_only=False, last_row_count=0):
        """Callback to display results incrementally as downloads complete."""
        print_table(page_rows, terminal_width, page_label=page_num, skip_header=skip_header, 
                    print_header_only=print_header_only, last_row_count=last_row_count)
    
    page_callback.first_call = True
    
    for threadUrl in thread_urls:
        print(f'\nProcessing thread: {threadUrl}')
        page_callback.first_call = True  # Reset for each thread
        # Process thread with per-page callback
        table_rows, csv_rows, skipped, too_small = process_thread(threadUrl, pageAppenderBefore, pageAppenderAfter,
                               startPage, endPage, pageValueMultiply, minWidth, minHeight,
                               outputBasePath=outputBasePath, maxWorkers=maxWorkers,
                               usePagination=usePagination, update_callback=page_callback,
                               terminal_width=terminal_width)
        if table_rows:
            all_table_rows.extend(table_rows)
        if csv_rows:
            all_csv_rows.extend(csv_rows)
        total_skipped += skipped
        total_too_small += too_small
    
    # Final summary
    print(f'\n{"═"*60}')
    print('Overall Summary:')
    print(f'{"─"*60}')
    if all_table_rows:
        success_count = sum(1 for r in all_table_rows if r[2] == '✓')
        failed_count = len(all_table_rows) - success_count
        print(f'Total: {success_count} successful, {total_skipped} skipped (duplicates), {total_too_small} too small, {failed_count} failed/error')
        
        # Save full URL details to CSV
        if all_csv_rows:
            csv_path = os.path.join(outputBasePath, 'downloads_log.csv')
            try:
                with open(csv_path, 'w', encoding='utf-8', newline='') as f:
                    f.write('Row,Filename,Full_URL,Size_Bytes,Status\n')
                    for idx, r in enumerate(all_csv_rows, 1):
                        # Escape quotes in URL and status for CSV
                        url = str(r[0]).replace('"', '""')
                        size = str(r[1]) if r[1] else ''
                        filename = str(r[2]).replace('"', '""')
                        status = str(r[3]).replace('"', '""')
                        f.write(f'{idx},"{filename}","{url}",{size},"{status}"\n')
                print(f'Full URL details saved to: {csv_path}')
            except Exception as e:
                print(f'Warning: Could not save CSV log: {e}')
    else:
        msg_parts = []
        if total_skipped > 0:
            msg_parts.append(f'{total_skipped} skipped (duplicates)')
        if total_too_small > 0:
            msg_parts.append(f'{total_too_small} too small')
        print(f'No downloads. {", ".join(msg_parts)}.' if msg_parts else 'No download records to display.')
    print(f'{"═"*60}')
    
    print(f"\n{'='*60}")
    print("All threads completed!")
    print(f"{'='*60}\n")


if __name__ == '__main__':
    main()
