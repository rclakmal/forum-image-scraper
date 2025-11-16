import os
import re
import hashlib
import configparser
import csv
import time
import json
from urllib.parse import quote, urlencode
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.chrome.options import Options
from selenium.common.exceptions import TimeoutException, NoSuchElementException
import requests
from PIL import Image
from io import BytesIO
from concurrent.futures import ThreadPoolExecutor, as_completed
import shutil
import sys

# Reusable session for connection pooling
session = requests.Session()
session.headers.update({
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'
})


def download_image(url, index, folder, url_hash, minWidth, minHeight):
    """Download a single image. Returns (status, filename, message)."""
    # Detect file extension from URL
    is_svg = url.lower().endswith('.svg') or '.svg?' in url.lower()
    is_png = url.lower().endswith('.png') or '.png?' in url.lower()
    is_gif = url.lower().endswith('.gif') or '.gif?' in url.lower()
    
    if is_svg:
        ext = '.svg'
    elif is_png:
        ext = '.png'
    elif is_gif:
        ext = '.gif'
    else:
        ext = '.jpg'
    
    filename = f"img_{index:04d}_{url_hash}{ext}"
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
        return ('error', filename, str(e)[:100])


def handle_consent_dialog(driver, wait_time=5):
    """Handle Google's consent dialog if it appears."""
    try:
        # Wait for consent form to appear - try multiple button texts and languages
        button_texts = [
            "Accept all", "Reject all", "I agree", "Alle akzeptieren", 
            "Alle ablehnen", "Akzeptieren", "Accept", "Agree"
        ]
        
        # Try XPath with multiple text options
        xpath_conditions = " or ".join([f"contains(translate(., 'ABCDEFGHIJKLMNOPQRSTUVWXYZ', 'abcdefghijklmnopqrstuvwxyz'), '{text.lower()}')" for text in button_texts])
        xpath = f"//button[{xpath_conditions}]"
        
        try:
            consent_button = WebDriverWait(driver, wait_time).until(
                EC.element_to_be_clickable((By.XPATH, xpath))
            )
            consent_button.click()
            print("Consent dialog handled (clicked accept button).")
            return True
        except:
            pass
        
        # Alternative: Try finding buttons by common CSS classes
        button_selectors = [
            "button[jsname='b3VHJd']",  # Common Google consent button
            "button[jsname='tWT92d']",
            "button.tHlp8d",
            "button[aria-label*='Accept']",
            "button[aria-label*='Akzeptieren']",
            "form button[type='button']"
        ]
        
        for selector in button_selectors:
            try:
                buttons = driver.find_elements(By.CSS_SELECTOR, selector)
                for btn in buttons:
                    text = btn.text.lower()
                    if any(word in text for word in ['accept', 'agree', 'akzeptieren', 'alle']):
                        btn.click()
                        print(f"Consent dialog handled (clicked button with text: {btn.text}).")
                        return True
            except:
                continue
        
        # Last resort: try clicking any button in a form
        try:
            forms = driver.find_elements(By.TAG_NAME, "form")
            for form in forms:
                buttons = form.find_elements(By.TAG_NAME, "button")
                if len(buttons) >= 2:  # Usually has "Accept" and "Reject" buttons
                    # Click the second button (usually "Accept all")
                    buttons[1].click()
                    print("Consent dialog handled (clicked second button in form).")
                    return True
        except:
            pass
            
        return False
        
    except TimeoutException:
        # No consent dialog appeared
        return False
    except Exception as e:
        print(f"Warning: Could not handle consent dialog: {e}")
        return False


def disable_safe_search(driver):
    """Disable SafeSearch by clicking the toggle to 'Off' state."""
    try:
        # Wait for page to load
        time.sleep(2)
        
        # Try multiple strategies to find and click SafeSearch toggle
        strategies = [
            # Strategy 1: Click SafeSearch button/toggle
            (By.XPATH, "//div[contains(text(), 'SafeSearch')]/.."),
            (By.XPATH, "//div[contains(@aria-label, 'SafeSearch')]"),
            (By.XPATH, "//button[contains(@aria-label, 'SafeSearch')]"),
            # Strategy 2: Direct button click
            (By.CSS_SELECTOR, "div[role='button'][aria-label*='SafeSearch']"),
            (By.CSS_SELECTOR, "button[aria-label*='SafeSearch']"),
        ]
        
        for by, selector in strategies:
            try:
                elements = driver.find_elements(by, selector)
                for element in elements:
                    try:
                        # Check if SafeSearch is not already off
                        if 'off' not in element.text.lower() or 'aus' not in element.text.lower():
                            driver.execute_script("arguments[0].click();", element)
                            time.sleep(1)
                            
                            # Try to click 'Off' option in menu
                            off_options = [
                                (By.XPATH, "//div[contains(text(), 'Off')]"),
                                (By.XPATH, "//span[contains(text(), 'Off')]"),
                                (By.XPATH, "//div[text()='Off']"),
                                (By.XPATH, "//div[contains(text(), 'Aus')]"),  # German
                                (By.XPATH, "//span[contains(text(), 'Aus')]"),  # German
                            ]
                            
                            for off_by, off_selector in off_options:
                                try:
                                    off_button = driver.find_element(off_by, off_selector)
                                    driver.execute_script("arguments[0].click();", off_button)
                                    print("SafeSearch disabled (clicked 'Off')")
                                    time.sleep(1)
                                    return True
                                except:
                                    continue
                            
                            return True
                    except:
                        continue
            except:
                continue
        
        print("SafeSearch toggle not found or already disabled")
        return False
        
    except Exception as e:
        print(f"Could not disable SafeSearch: {str(e)[:60]}")
        return False


def build_google_images_url(search_term, size='any', color='any', time_filter='any', 
                            image_type='any', license_filter='any', usage_rights='any', 
                            safe_search='moderate'):
    """Build Google Images search URL with filters."""
    base_url = "https://www.google.com/search"
    
    # Build query parameters
    params = {
        'q': search_term,
        'tbm': 'isch',  # Image search
        'safe': safe_search
    }
    
    # Build advanced search parameters (tbs)
    tbs_parts = []
    
    # Size filter
    if size == 'large':
        tbs_parts.append('isz:l')
    elif size == 'medium':
        tbs_parts.append('isz:m')
    elif size == 'icon':
        tbs_parts.append('isz:i')
    
    # Color filter
    color_map = {
        'color': 'ic:color',
        'gray': 'ic:gray',
        'transparent': 'ic:trans',
        'red': 'ic:specific,isc:red',
        'orange': 'ic:specific,isc:orange',
        'yellow': 'ic:specific,isc:yellow',
        'green': 'ic:specific,isc:green',
        'teal': 'ic:specific,isc:teal',
        'blue': 'ic:specific,isc:blue',
        'purple': 'ic:specific,isc:purple',
        'pink': 'ic:specific,isc:pink',
        'white': 'ic:specific,isc:white',
        'black': 'ic:specific,isc:black',
        'brown': 'ic:specific,isc:brown'
    }
    if color in color_map:
        tbs_parts.append(color_map[color])
    
    # Time filter
    time_map = {
        'past_day': 'qdr:d',
        'past_week': 'qdr:w',
        'past_month': 'qdr:m',
        'past_year': 'qdr:y'
    }
    if time_filter in time_map:
        tbs_parts.append(time_map[time_filter])
    
    # Image type filter
    type_map = {
        'photo': 'itp:photo',
        'clipart': 'itp:clipart',
        'lineart': 'itp:lineart',
        'gif': 'itp:animated'
    }
    if image_type in type_map:
        tbs_parts.append(type_map[image_type])
    
    # License filter
    if license_filter == 'creative_commons':
        tbs_parts.append('il:cl')
    elif license_filter == 'other_licenses':
        tbs_parts.append('il:ol')
    
    # Usage rights (more specific)
    rights_map = {
        'free_to_use_share': 'sur:fmc',
        'free_to_use_share_modify': 'sur:fm',
        'free_to_use_share_modify_commercially': 'sur:f',
        'free_to_use_modify': 'sur:fc',
        'free_to_use': 'sur:fc',
        'free_to_use_commercially': 'sur:fmc'
    }
    if usage_rights in rights_map:
        tbs_parts.append(rights_map[usage_rights])
    
    # Add tbs parameter if any filters applied
    if tbs_parts:
        params['tbs'] = ','.join(tbs_parts)
    
    return f"{base_url}?{urlencode(params)}"


def extract_and_download_images(driver, max_images, scroll_delay, thumbnail_selector, full_image_selector, 
                               folder, minWidth, minHeight, maxWorkers, terminal_width):
    """Extract image URLs from Google Images and download them as they're found."""
    
    print(f"Starting image extraction and download (target: {max_images})...")
    
    # Scroll to load more thumbnails until target is reached
    last_count = 0
    scroll_attempts = 0
    max_scroll_attempts = 20
    
    while scroll_attempts < max_scroll_attempts:
        thumbnails = driver.find_elements(By.CSS_SELECTOR, "img[class='{}']".format(thumbnail_selector))
        current_count = len(thumbnails)
        
        if current_count >= max_images:
            print(f"Loaded {current_count} thumbnails (target: {max_images})")
            break
        
        if current_count == last_count:
            # No new thumbnails loaded, try scrolling again or stop
            scroll_attempts += 1
            if scroll_attempts >= 3:
                print(f"No more images loading. Found {current_count} thumbnails.")
                break
        else:
            scroll_attempts = 0  # Reset if new images loaded
        
        # Scroll to bottom to trigger lazy loading
        driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
        time.sleep(scroll_delay)
        
        last_count = current_count
    
    thumbnails = driver.find_elements(By.CSS_SELECTOR, "img[class='{}']".format(thumbnail_selector))
    print(f"Found {len(thumbnails)} thumbnails to process...\n")
    
    # Setup for downloads
    downloaded_hashes = set()
    table_rows = []
    csv_rows = []
    skipped_count = 0
    too_small_count = 0
    header_printed = False
    last_printed_count = 0
    
    # Check existing files
    if os.path.exists(folder):
        for existing_file in os.listdir(folder):
            if existing_file.endswith(('.jpg', '.png', '.gif', '.svg')):
                parts = existing_file.split('_')
                if len(parts) >= 3:
                    hash_part = parts[2].split('.')[0]
                    downloaded_hashes.add(hash_part)
    
    # Use thread pool for parallel downloads
    with ThreadPoolExecutor(max_workers=maxWorkers) as executor:
        futures = {}
        extraction_count = 0
        
        for idx, thumbnail in enumerate(thumbnails, 1):
            thumbnail_src = thumbnail.get_attribute("src")  # Get thumbnail URL for timeout cases
            try:
                driver.execute_script("arguments[0].click();", thumbnail)
                full_images = driver.find_elements(By.XPATH, "//img[@jsname='kn3ccd']")
                if (len (full_images) == 0):
                    full_images = driver.find_elements(By.XPATH, "//img[@jsname='JuXqh']")
                                
                src = full_images[0].get_attribute("src")
                
                # Check if already downloaded
                url_hash = hashlib.md5(src.encode()).hexdigest()[:8]
                if url_hash in downloaded_hashes:
                    skipped_count += 1
                    # Add to table as already downloaded
                    filename = f"img_{idx:04d}_{url_hash}"
                    table_rows.append((filename, 'existing', 'already downloaded'))
                    csv_rows.append((src, '', filename, 'already downloaded'))
                    
                    # Update table
                    if not header_printed:
                        print_table(table_rows, terminal_width, print_header_only=True)
                        header_printed = True
                    print_table(table_rows, terminal_width, last_row_count=last_printed_count)
                    last_printed_count = len(table_rows)
                    continue
                
                # Submit download task immediately
                future = executor.submit(download_image, src, idx, folder, url_hash, minWidth, minHeight)
                futures[future] = (src, url_hash)
                extraction_count += 1
                
                # Process any completed downloads while extracting
                for completed_future in list(futures.keys()):
                    if completed_future.done():
                        orig_url, orig_hash = futures[completed_future]
                        del futures[completed_future]
                        
                        try:
                            status, filename, info = completed_future.result()
                            
                            if status == 'success':
                                downloaded_hashes.add(orig_hash)
                                table_rows.append((filename, str(info), '✓'))
                                csv_rows.append((orig_url, str(info), filename, '✓'))
                            elif status == 'too_small':
                                too_small_count += 1
                                csv_rows.append((orig_url, '', filename, f'too_small {info}'))
                            elif status == 'failed':
                                table_rows.append((filename, '', f'failed {info}'))
                                csv_rows.append((orig_url, '', filename, f'failed {info}'))
                            elif status == 'error':
                                table_rows.append((filename, '', f'error {info}'))
                                csv_rows.append((orig_url, '', filename, f'error {info}'))
                            
                            # Update table
                            if table_rows:
                                if not header_printed:
                                    print_table(table_rows, terminal_width, print_header_only=True)
                                    header_printed = True
                                print_table(table_rows, terminal_width, last_row_count=last_printed_count)
                                last_printed_count = len(table_rows)
                        except Exception as e:
                            print(f"Download error: {str(e)[:80]}")
                
            except TimeoutException:
                # Add timeout to table
                filename = f"img_{idx:04d}_timeout"
                table_rows.append((filename, '', 'timeout'))
                csv_rows.append((thumbnail_src, '', filename, 'timeout'))
                
                # Update table
                if not header_printed:
                    print_table(table_rows, terminal_width, print_header_only=True)
                    header_printed = True
                print_table(table_rows, terminal_width, last_row_count=last_printed_count)
                last_printed_count = len(table_rows)
            except Exception as e:
                # Add error to table with thumbnail URL
                filename = f"img_{idx:04d}_error"
                error_msg = str(e)[:40]
                table_rows.append((filename, '', f'error - {error_msg}'))
                csv_rows.append((thumbnail_src, '', filename, f'error: {error_msg}'))
                
                # Update table
                if not header_printed:
                    print_table(table_rows, terminal_width, print_header_only=True)
                    header_printed = True
                print_table(table_rows, terminal_width, last_row_count=last_printed_count)
                last_printed_count = len(table_rows)
        
        # Process any remaining downloads
        for future in as_completed(futures):
            orig_url, orig_hash = futures[future]
            
            try:
                status, filename, info = future.result()
                
                if status == 'success':
                    downloaded_hashes.add(orig_hash)
                    table_rows.append((filename, str(info), '✓'))
                    csv_rows.append((orig_url, str(info), filename, '✓'))
                elif status == 'too_small':
                    too_small_count += 1
                    csv_rows.append((orig_url, '', filename, f'too_small {info}'))
                elif status == 'failed':
                    table_rows.append((filename, '', f'failed {info}'))
                    csv_rows.append((orig_url, '', filename, f'failed {info}'))
                elif status == 'error':
                    table_rows.append((filename, '', f'error {info}'))
                    csv_rows.append((orig_url, '', filename, f'error {info}'))
                
                # Update table
                if table_rows:
                    if not header_printed:
                        print_table(table_rows, terminal_width, print_header_only=True)
                        header_printed = True
                    print_table(table_rows, terminal_width, last_row_count=last_printed_count)
                    last_printed_count = len(table_rows)
            except Exception as e:
                print(f"Download error: {str(e)[:80]}")
    
    return table_rows, csv_rows, skipped_count, too_small_count


def print_table(rows, terminal_width, print_header_only=False, last_row_count=0):
    """Print a formatted table that adapts to terminal width."""
    if not rows and not print_header_only:
        return
    
    cols = ['Filename', 'Size', 'Status']
    
    # Calculate column widths
    min_file_width = 30
    min_size_width = 12
    min_status_width = 30
    padding = 2 * 4 + 2
    
    # Distribute remaining width
    remaining = terminal_width - min_file_width - min_size_width - min_status_width - padding
    if remaining > 0:
        min_file_width += remaining // 2
        min_status_width += remaining - (remaining // 2)
    
    widths = [min_file_width, min_size_width, min_status_width]
    
    # Print header
    if print_header_only:
        print('\n╔' + '═╤═'.join('═' * w for w in widths) + '╗')
        print('║ ' + ' │ '.join(cols[i].ljust(widths[i]) for i in range(len(cols))) + ' ║')
        print('╠' + '═╪═'.join('═' * w for w in widths) + '╣')
        sys.stdout.flush()
        return
    
    # Print only new rows
    for i in range(last_row_count, len(rows)):
        r = rows[i]
        file_str = str(r[0])[:widths[0]] if r[0] else ''
        size_str = str(r[1]) if r[1] else ''
        status_str = str(r[2])[:widths[2]]
        
        print('║ ' + file_str.ljust(widths[0]) + ' │ ' + 
              size_str.ljust(widths[1]) + ' │ ' + 
              status_str.ljust(widths[2]) + ' ║')
    
    sys.stdout.flush()


def main():
    config = configparser.RawConfigParser()
    google_props = 'google.properties'
    download_props = 'download.properties'

    if not os.path.exists(google_props):
        print(f"ERROR: '{google_props}' not found. Please create this configuration file.")
        return

    # Read Google settings
    try:
        with open(google_props, encoding="utf-8") as f:
            config.read_file(f)
        section = 'GoogleSettings'
        
        search_term = config.get(section, "searchTerm")
        max_images = int(config.get(section, "maxImages", fallback="100"))
        thumbnail_selector = config.get(section, "thumbnailClassSelector", fallback="YQ4gaf")
        full_image_selector = config.get(section, "fullImageClassSelector", fallback="sFlh5c FyHeAf iPVvYb")
        size = config.get(section, "size", fallback="any")
        color = config.get(section, "color", fallback="any")
        time_filter = config.get(section, "time", fallback="any")
        image_type = config.get(section, "type", fallback="any")
        license_filter = config.get(section, "license", fallback="any")
        usage_rights = config.get(section, "usageRights", fallback="any")
        safe_search = config.get(section, "safeSearch", fallback="moderate")
        scroll_delay = float(config.get(section, "scrollDelay", fallback="2"))
    except Exception as e:
        print(f"ERROR: Missing or invalid Google properties: {e}")
        return

    # Read download settings
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

    # Create output folder
    safe_search_term = re.sub(r'[^\w\-]', '_', search_term)
    folder = os.path.join(outputBasePath, 'google_images', safe_search_term)
    os.makedirs(folder, exist_ok=True)

    print(f"="*60)
    print(f"Google Images Scraper")
    print(f"="*60)
    print(f"Search term: {search_term}")
    print(f"Max images: {max_images if max_images > 0 else 'unlimited'}")
    print(f"Filters: size={size}, color={color}, time={time_filter}, type={image_type}")
    print(f"Output: {folder}")
    print(f"="*60)

    # Setup Chrome driver
    chrome_options = Options()
    chrome_options.add_argument('--headless')  # Run in background - disabled for debugging
    chrome_options.add_argument('--no-sandbox')
    chrome_options.add_argument('--disable-dev-shm-usage')
    chrome_options.add_argument('--disable-blink-features=AutomationControlled')
    chrome_options.add_argument('--window-size=1920,1080')
    chrome_options.add_argument('user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36')
    chrome_options.add_experimental_option("excludeSwitches", ["enable-automation"])
    chrome_options.add_experimental_option('useAutomationExtension', False)
    
    try:
        driver = webdriver.Chrome(options=chrome_options)
    except Exception as e:
        print(f"ERROR: Could not start Chrome driver: {e}")
        print("Make sure Chrome and chromedriver are installed.")
        return

    try:
        # Build and navigate to Google Images
        url = build_google_images_url(search_term, size, color, time_filter, 
                                     image_type, license_filter, usage_rights, safe_search)
        print(f"\nNavigating to Google Images...")
        driver.get(url)
        
        # Handle consent dialog
        print("Checking for consent dialog...")
        handle_consent_dialog(driver)
        
        # Disable SafeSearch if needed
        if safe_search == 'off':
            print("Disabling SafeSearch...")
            disable_safe_search(driver)
        
        # Get terminal width
        terminal_width = shutil.get_terminal_size((120, 40)).columns
        
        # Extract and download images simultaneously
        table_rows, csv_rows, skipped_count, too_small_count = extract_and_download_images(
            driver, max_images, scroll_delay, thumbnail_selector, full_image_selector,
            folder, minWidth, minHeight, maxWorkers, terminal_width
        )
        
        # Print footer
        if table_rows:
            min_file_width = 30
            min_size_width = 12
            min_status_width = 30
            padding = 2 * 4 + 2
            remaining = terminal_width - min_file_width - min_size_width - min_status_width - padding
            if remaining > 0:
                min_file_width += remaining // 2
                min_status_width += remaining - (remaining // 2)
            widths = [min_file_width, min_size_width, min_status_width]
            print('╚' + '═╧═'.join('═' * w for w in widths) + '╝')
        
        # Summary
        print(f"\n{'='*60}")
        print('Summary:')
        print(f"{'─'*60}")
        if table_rows:
            success_count = sum(1 for r in table_rows if r[2] == '✓')
            failed_count = len(table_rows) - success_count
            print(f'Total: {success_count} successful, {skipped_count} skipped (duplicates), {too_small_count} too small, {failed_count} failed/error')
            
            # Save CSV
            if csv_rows:
                csv_path = os.path.join(folder, 'downloads_log.csv')
                try:
                    with open(csv_path, 'w', encoding='utf-8', newline='') as f:
                        f.write('Row,Filename,Full_URL,Size_Bytes,Status\n')
                        for idx, r in enumerate(csv_rows, 1):
                            url = str(r[0]).replace('"', '""')
                            size = str(r[1]) if r[1] else ''
                            filename = str(r[2]).replace('"', '""')
                            status = str(r[3]).replace('"', '""')
                            f.write(f'{idx},"{filename}","{url}",{size},"{status}"\n')
                    print(f'Full URL details saved to: {csv_path}')
                except Exception as e:
                    print(f'Warning: Could not save CSV log: {e}')
        else:
            print('No downloads completed.')
        print(f"{'='*60}\n")
        
    finally:
        driver.quit()


if __name__ == '__main__':
    main()
