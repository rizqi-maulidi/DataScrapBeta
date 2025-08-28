from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from webdriver_manager.chrome import ChromeDriverManager
import pandas as pd
import time
import random
import re
import os
import schedule
from datetime import datetime, timezone, timedelta
from dateutil import parser
import warnings
import logging

# Suppress warnings dan logs
warnings.filterwarnings("ignore")
os.environ['TF_CPP_MIN_LOG_LEVEL'] = '3'
os.environ['PYTHONWARNINGS'] = 'ignore'
os.environ['WDM_LOG'] = '0'

logging.getLogger('selenium').setLevel(logging.WARNING)
logging.getLogger('urllib3').setLevel(logging.WARNING)
logging.getLogger().setLevel(logging.ERROR)

# ======== KONFIGURASI OTOMATIS ========
CONFIG = {
    "keyword_variations": [
        "politik dinasti",
        "politik indonesia", 
        "pemilu 2024 indonesia",
        "pemerintah indonesia",
        "DPR indonesia",
        "pilpres indonesia",
        "kabinet indonesia",
        "koalisi pemerintah",
        "oposisi indonesia"
    ],
    "current_keyword_index": 0,
    "max_videos": 200,
    "csv_filename": "tiktok_politik_auto.csv",
    "sna_filename": "tiktok_sna_relations.csv",
    "interval_minutes": 15,
    "headless": True,
    "fetch_likes_from_video_page": False
}

# ------------------------------
# Config / Helpers
# ------------------------------
MENTION_RE = re.compile(r'@([A-Za-z0-9_.]+)')

def normalize_timestamp(timestamp_str):
    """FIXED: Normalisasi timestamp ke format ISO 8601 yang konsisten"""
    if not timestamp_str or timestamp_str.strip() == "":
        return datetime.now(timezone.utc).isoformat()
    
    try:
        timestamp_lower = timestamp_str.lower().strip()
        
        if any(word in timestamp_lower for word in ['sekarang', 'now', 'just now']):
            return datetime.now(timezone.utc).isoformat()
        
        # FIXED: Tambah pattern untuk format TikTok seperti "3d ago", "1w ago", "22h ago"
        time_ago_patterns = [
            (r'(\d+)\s*s\s*ago', lambda x: datetime.now(timezone.utc) - timedelta(seconds=int(x))),
            (r'(\d+)\s*m\s*ago', lambda x: datetime.now(timezone.utc) - timedelta(minutes=int(x))),
            (r'(\d+)\s*h\s*ago', lambda x: datetime.now(timezone.utc) - timedelta(hours=int(x))),
            (r'(\d+)\s*d\s*ago', lambda x: datetime.now(timezone.utc) - timedelta(days=int(x))),
            (r'(\d+)\s*w\s*ago', lambda x: datetime.now(timezone.utc) - timedelta(weeks=int(x))),
            (r'(\d+)\s*mo\s*ago', lambda x: datetime.now(timezone.utc) - timedelta(days=int(x)*30)),
            (r'(\d+)\s*y\s*ago', lambda x: datetime.now(timezone.utc) - timedelta(days=int(x)*365)),
            
            # Bahasa Indonesia patterns
            (r'(\d+)\s*(detik|second)s?\s*(yang\s*lalu|ago)', lambda x: datetime.now(timezone.utc) - timedelta(seconds=int(x))),
            (r'(\d+)\s*(menit|minute)s?\s*(yang\s*lalu|ago)', lambda x: datetime.now(timezone.utc) - timedelta(minutes=int(x))),
            (r'(\d+)\s*(jam|hour)s?\s*(yang\s*lalu|ago)', lambda x: datetime.now(timezone.utc) - timedelta(hours=int(x))),
            (r'(\d+)\s*(hari|day)s?\s*(yang\s*lalu|ago)', lambda x: datetime.now(timezone.utc) - timedelta(days=int(x))),
            (r'(\d+)\s*(minggu|week)s?\s*(yang\s*lalu|ago)', lambda x: datetime.now(timezone.utc) - timedelta(weeks=int(x))),
            (r'(\d+)\s*(bulan|month)s?\s*(yang\s*lalu|ago)', lambda x: datetime.now(timezone.utc) - timedelta(days=int(x)*30)),
        ]
        
        for pattern, calc_func in time_ago_patterns:
            match = re.search(pattern, timestamp_lower)
            if match:
                number = match.group(1)
                calculated_time = calc_func(number)
                return calculated_time.isoformat()
        
        # Try parsing standard formats
        try:
            if 'T' in timestamp_str and ('Z' in timestamp_str or '+' in timestamp_str[-6:]):
                dt = parser.parse(timestamp_str)
                return dt.isoformat()
            
            dt = parser.parse(timestamp_str)
            if dt.tzinfo is None:
                dt = dt.replace(tzinfo=timezone.utc)
            return dt.isoformat()
            
        except:
            formats_to_try = [
                '%Y-%m-%d',
                '%Y-%m-%d %H:%M:%S',
                '%d-%m-%Y',
                '%d/%m/%Y',
                '%m/%d/%Y',
                '%Y/%m/%d',
                '%d %b %Y',
                '%b %d, %Y',
                '%B %d, %Y'
            ]
            
            for fmt in formats_to_try:
                try:
                    dt = datetime.strptime(timestamp_str, fmt)
                    dt = dt.replace(tzinfo=timezone.utc)
                    return dt.isoformat()
                except:
                    continue
        
        print(f"Warning: Could not parse timestamp '{timestamp_str}', using current time")
        return datetime.now(timezone.utc).isoformat()
        
    except Exception as e:
        print(f"Error parsing timestamp '{timestamp_str}': {e}")
        return datetime.now(timezone.utc).isoformat()

def setup_driver(headless=True):
    """Setup ChromeDriver dengan anti-detection untuk automation"""
    options = Options()
    if headless:
        options.add_argument("--headless=new")
        options.add_argument("--window-size=1920,1080")
    else:
        options.add_argument("--start-maximized")

    # Anti-detection options
    options.add_argument("--disable-notifications")
    options.add_argument("--disable-popup-blocking")
    options.add_argument("--disable-blink-features=AutomationControlled")
    options.add_experimental_option("excludeSwitches", ["enable-automation"])
    options.add_experimental_option('useAutomationExtension', False)
    options.add_argument("--disable-web-security")
    options.add_argument("--allow-running-insecure-content")
    options.add_argument("--disable-extensions")
    options.add_argument("--no-sandbox")
    options.add_argument("--disable-dev-shm-usage")
    options.add_argument("--disable-gpu")

    # Suppress error messages dan warnings
    options.add_argument("--disable-logging")
    options.add_argument("--log-level=3")
    options.add_argument("--silent")
    options.add_argument("--disable-gpu-logging")
    options.add_argument("--disable-software-rasterizer")
    options.add_argument("--disable-background-timer-throttling")
    options.add_argument("--disable-backgrounding-occluded-windows")
    options.add_argument("--disable-renderer-backgrounding")
    options.add_argument("--disable-features=TranslateUI")
    options.add_argument("--disable-ipc-flooding-protection")
    options.add_argument("--disable-hang-monitor")
    options.add_argument("--disable-client-side-phishing-detection")
    options.add_argument("--disable-component-update")
    options.add_argument("--no-default-browser-check")
    options.add_argument("--no-first-run")
    options.add_argument("--disable-default-apps")
    
    # Suppress GPU/WebGL errors
    options.add_argument("--use-gl=swiftshader")
    options.add_argument("--disable-webgl")
    options.add_argument("--disable-webgl2") 
    options.add_argument("--disable-3d-apis")
    options.add_argument("--disable-accelerated-2d-canvas")

    # Random user agent
    user_agents = [
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
        "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
    ]
    options.add_argument(f"--user-agent={random.choice(user_agents)}")

    service = Service(ChromeDriverManager().install())
    service.creation_flags = 0x08000000  # CREATE_NO_WINDOW flag untuk Windows
    driver = webdriver.Chrome(service=service, options=options)

    try:
        driver.execute_script("Object.defineProperty(navigator, 'webdriver', {get: () => undefined})")
        driver.execute_cdp_cmd("Page.addScriptToEvaluateOnNewDocument", {
            "source": """
                Object.defineProperty(navigator, 'webdriver', {get: () => undefined});
                Object.defineProperty(navigator, 'plugins', {get: () => [1, 2, 3, 4, 5]});
                Object.defineProperty(navigator, 'languages', {get: () => ['en-US', 'en']});
            """
        })
    except Exception:
        pass

    return driver

# ------------------------------
# Extract individual container data
# ------------------------------
def extract_video_data(container_element, driver=None, fetch_likes_from_video_page=False):
    data = {
        "title": "",
        "description": "",
        "link": "",
        "likes": "0",
        "shares": "0",
        "comments": "0",
        "author": "",
        "author_username": "",
        "timestamp": "",
        "mentions_in_caption": [],
        "hashtags": [],
        "scraped_at": datetime.now(timezone.utc).isoformat()
    }

    try:
        try:
            link_elem = container_element.find_element(By.XPATH, './/a[contains(@href, "/video/")]')
            data["link"] = link_elem.get_attribute("href")
        except:
            return None

        try:
            title_elem = container_element.find_element(By.XPATH, './/span[@data-e2e="new-desc-span"]')
            data["title"] = title_elem.text.strip()
        except:
            try:
                title_elem = container_element.find_element(By.XPATH, './/div[@data-e2e="search-card-video-caption"]//span')
                data["title"] = title_elem.text.strip()
            except:
                try:
                    img_elem = container_element.find_element(By.XPATH, './/img[@alt]')
                    alt_text = img_elem.get_attribute("alt")
                    if alt_text:
                        data["title"] = alt_text.strip()[:200]
                        data["description"] = alt_text.strip()
                except:
                    pass

        # Extract mentions and hashtags
        combined_text = " ".join([data.get("title",""), data.get("description","")]).strip()
        if combined_text:
            mentions = MENTION_RE.findall(combined_text)
            data["mentions_in_caption"] = [f"@{m}" for m in mentions] if mentions else []
            data["hashtags"] = re.findall(r'#\w+', combined_text)

        try:
            username_elem = container_element.find_element(By.XPATH, './/p[@data-e2e="search-card-user-unique-id"]')
            uname = username_elem.text.strip()
            data["author_username"] = uname if uname.startswith('@') else uname
            data["author"] = f"@{uname}" if not uname.startswith('@') else uname
        except:
            try:
                user_link = container_element.find_element(By.XPATH, './/a[@data-e2e="search-card-user-link"]')
                href = user_link.get_attribute('href')
                if '/@' in href:
                    username = href.split('/@')[1].split('/')[0]
                    data["author_username"] = username
                    data["author"] = f"@{username}"
            except:
                pass

        # Handle timestamp dengan normalisasi yang sudah diperbaiki
        try:
            date_elem = container_element.find_element(By.XPATH, './/div[contains(@class, "DivTimeTag")]')
            raw_timestamp = date_elem.text.strip()
            data["timestamp"] = normalize_timestamp(raw_timestamp)
        except:
            try:
                date_elem = container_element.find_element(By.XPATH, './/time')
                datetime_attr = date_elem.get_attribute('datetime')
                if datetime_attr:
                    data["timestamp"] = normalize_timestamp(datetime_attr)
                else:
                    raw_timestamp = date_elem.text.strip()
                    data["timestamp"] = normalize_timestamp(raw_timestamp)
            except:
                data["timestamp"] = datetime.now(timezone.utc).isoformat()

        # IMPROVED: Extract engagement metrics dengan lebih detail
        engagement_found = False
        
        # Coba ekstrak likes
        like_selectors = [
            './/strong[@data-e2e="like-count"]',
            './/span[@data-e2e="like-count"]',
            './/strong[contains(@class, "count")]',
            './/span[contains(@data-e2e, "like")]',
            './/button[contains(@data-e2e, "like")]//span',
        ]
        for sel in like_selectors:
            try:
                el = container_element.find_element(By.XPATH, sel)
                txt = el.text.strip()
                if txt and txt != "0":
                    data["likes"] = txt
                    engagement_found = True
                    break
            except:
                continue
        
        # Coba ekstrak shares dan comments jika ada
        try:
            share_elem = container_element.find_element(By.XPATH, './/strong[@data-e2e="share-count"]')
            data["shares"] = share_elem.text.strip() or "0"
        except:
            pass
        
        try:
            comment_elem = container_element.find_element(By.XPATH, './/strong[@data-e2e="comment-count"]')
            data["comments"] = comment_elem.text.strip() or "0"
        except:
            pass

        # Jika belum ada engagement dan diminta untuk fetch dari video page
        if not engagement_found and fetch_likes_from_video_page and driver and data["link"]:
            try:
                original_handle = driver.current_window_handle
                driver.execute_script("window.open('');")
                driver.switch_to.window(driver.window_handles[-1])
                driver.get(data["link"])
                time.sleep(random.uniform(2.0, 4.0))
                try:
                    like_el = WebDriverWait(driver, 3).until(
                        EC.presence_of_element_located((By.XPATH,
                            '//strong[@data-e2e="like-count"] | //button[@data-e2e="like-icon"]//strong | //button[@aria-label and contains(translate(@aria-label, "LIKE", "like"), "like")]//span'))
                    )
                    data["likes"] = like_el.text.strip() or data["likes"]
                except:
                    pass
                driver.close()
                driver.switch_to.window(original_handle)
            except Exception:
                try:
                    if len(driver.window_handles) > 1:
                        driver.close()
                        driver.switch_to.window(driver.window_handles[0])
                except:
                    pass

        # Set default values jika masih kosong
        for metric in ["likes", "shares", "comments"]:
            if not data[metric] or str(data[metric]).strip() == "":
                data[metric] = "0"

        if data["link"] and (data["title"] or data["author"]):
            return data
        else:
            return None

    except Exception as e:
        print("Error ekstraksi container:", e)
        return None

# ------------------------------
# Fungsi untuk intelligent update
# ------------------------------
def compare_and_update_video_data(existing_df, new_df):
    """
    Bandingkan data existing dengan data baru dan pilih yang terbaru
    berdasarkan timestamp scraping dan engagement metrics
    """
    if existing_df.empty:
        return new_df
    
    if new_df.empty:
        return existing_df
    
    existing_df = existing_df.copy()
    new_df = new_df.copy()
    
    # Safe datetime conversion function
    def safe_datetime_conversion(df, col_name):
        try:
            df[col_name] = pd.to_datetime(df[col_name], format='ISO8601', errors='coerce')
        except:
            try:
                df[col_name] = pd.to_datetime(df[col_name], format='mixed', errors='coerce')
            except:
                try:
                    df[col_name] = pd.to_datetime(df[col_name], infer_datetime_format=True, errors='coerce')
                except:
                    print(f"Warning: Tidak dapat mem-parsing tanggal, menggunakan timestamp sekarang")
                    df[col_name] = pd.to_datetime(datetime.now().strftime('%Y-%m-%d %H:%M:%S'))
        
        nat_mask = df[col_name].isna()
        if nat_mask.any():
            print(f"Warning: {nat_mask.sum()} rows memiliki tanggal invalid, menggunakan timestamp sekarang")
            df.loc[nat_mask, col_name] = pd.to_datetime(datetime.now().strftime('%Y-%m-%d %H:%M:%S'))
        
        return df
    
    # Apply safe datetime conversion
    existing_df = safe_datetime_conversion(existing_df, 'scraped_at')
    new_df = safe_datetime_conversion(new_df, 'scraped_at')
    
    # Convert metrics ke numeric untuk perbandingan
    metric_columns = ['likes', 'shares', 'comments']
    for col in metric_columns:
        if col in existing_df.columns:
            existing_df[col] = pd.to_numeric(existing_df[col], errors='coerce').fillna(0)
        if col in new_df.columns:
            new_df[col] = pd.to_numeric(new_df[col], errors='coerce').fillna(0)
    
    updated_videos = []
    updated_count = 0
    
    # Gabungkan semua data dengan indikator source
    existing_df['source'] = 'existing'
    new_df['source'] = 'new'
    combined_df = pd.concat([existing_df, new_df], ignore_index=True)
    
    # Group by link untuk membandingkan data
    for video_link, group in combined_df.groupby('link'):
        if len(group) == 1:
            # Video hanya ada di satu source, ambil apa adanya
            video_data = group.iloc[0].drop('source').to_dict()
            updated_videos.append(video_data)
        else:
            # Video ada di kedua source, pilih yang terbaru atau yang engagement-nya lebih tinggi
            existing_video = group[group['source'] == 'existing']
            new_video = group[group['source'] == 'new']
            
            if not existing_video.empty and not new_video.empty:
                existing_data = existing_video.iloc[0]
                new_data = new_video.iloc[0]
                
                should_update = False
                update_reason = []
                
                # Cek apakah scraping time lebih baru
                try:
                    if new_data['scraped_at'] > existing_data['scraped_at']:
                        should_update = True
                        update_reason.append('waktu_scraping_lebih_baru')
                except:
                    should_update = True
                    update_reason.append('datetime_comparison_fallback')
                
                # Cek apakah engagement metrics meningkat
                for metric in metric_columns:
                    if metric in existing_data and metric in new_data:
                        try:
                            old_val = float(existing_data[metric]) if existing_data[metric] else 0
                            new_val = float(new_data[metric]) if new_data[metric] else 0
                            if new_val > old_val:
                                should_update = True
                                update_reason.append(f'{metric}_meningkat')
                        except:
                            continue
                
                if should_update:
                    selected_data = new_data.drop('source').to_dict()
                    updated_count += 1
                    
                    # Log perubahan untuk debugging
                    changes = []
                    for metric in metric_columns:
                        if metric in existing_data and metric in new_data:
                            try:
                                old_val = int(float(existing_data[metric])) if existing_data[metric] else 0
                                new_val = int(float(new_data[metric])) if new_data[metric] else 0
                                if old_val != new_val:
                                    changes.append(f"{metric}: {old_val} → {new_val}")
                            except:
                                continue
                    
                    if changes:
                        username = selected_data.get('author', 'unknown')
                        print(f"Updated {username}: {', '.join(changes[:3])}")
                else:
                    selected_data = existing_data.drop('source').to_dict()
                
                updated_videos.append(selected_data)
            else:
                video_data = group.iloc[0].drop('source').to_dict()
                updated_videos.append(video_data)
    
    result_df = pd.DataFrame(updated_videos)
    
    # Convert scraped_at kembali ke string format yang konsisten  
    if not result_df.empty and 'scraped_at' in result_df.columns:
        # Check if the column contains datetime objects
        if pd.api.types.is_datetime64_any_dtype(result_df['scraped_at']):
            result_df['scraped_at'] = result_df['scraped_at'].dt.strftime('%Y-%m-%d %H:%M:%S')
        # If it's already strings, keep them as is
    
    print(f"Data comparison completed: {updated_count} videos updated with newer engagement data")
    
    return result_df

# ------------------------------
# FIXED: Monitor engagement changes
# ------------------------------
def monitor_engagement_changes(filename):
    """FIXED: Monitor dan laporan perubahan engagement metrics dari file CSV"""
    if not os.path.exists(filename):
        return
    
    try:
        df = pd.read_csv(filename, encoding='utf-8-sig')
        
        # Convert metrics ke numeric
        metric_columns = ['likes', 'shares', 'comments']
        for col in metric_columns:
            if col in df.columns:
                df[col] = pd.to_numeric(df[col], errors='coerce').fillna(0)
        
        # Tampilkan statistik engagement
        print(f"\nEngagement Statistics (Total {len(df)} videos):")
        for metric in metric_columns:
            if metric in df.columns:
                total = df[metric].sum()
                avg = df[metric].mean()
                max_val = df[metric].max()
                print(f"   • {metric.capitalize()}: Total={total:,.0f}, Avg={avg:.1f}, Max={max_val:,.0f}")
        
        # FIXED: Tampilkan top videos berdasarkan engagement
        print(f"\nTop 5 Videos by Total Engagement:")
        engagement_cols = [col for col in metric_columns if col in df.columns]
        if engagement_cols:
            df['total_engagement'] = df[engagement_cols].sum(axis=1)
            top_videos = df.nlargest(5, 'total_engagement')
            
            for i, (idx, video) in enumerate(top_videos.iterrows(), 1):
                author = video.get('author', 'unknown')
                title = video.get('title', '')
                # FIXED: Safe string handling untuk title
                if pd.isna(title) or not isinstance(title, str):
                    title = ''
                title_preview = title[:50] + "..." if len(title) > 50 else title
                total = int(video.get('total_engagement', 0))
                
                print(f"   {i}. {author} (Total: {total:,})")
                
                engagement_str = []
                for metric in engagement_cols:
                    val = int(video.get(metric, 0))
                    if val > 0:
                        emoji = {'likes': '❤️', 'views': '👀', 'shares': '📤', 'comments': '💬'}.get(metric, '📊')
                        engagement_str.append(f"{emoji} {val:,}")
                
                if engagement_str:
                    print(f"      {' | '.join(engagement_str)}")
                if title_preview:
                    print(f"      📝 \"{title_preview}\"")
                print()
            
    except Exception as e:
        print(f"Warning: Error monitoring engagement: {e}")

# ------------------------------
# SNA Relations extraction (MODIFIED WITH HASHTAG SUPPORT)
# ------------------------------
def extract_sna_relations(video_data):
    """Extract SNA relations from TikTok video data including hashtag relations"""
    relations = []
    
    source_username = video_data.get("author", "")
    if source_username and not source_username.startswith("@"):
        source_username = "@" + source_username
    
    video_title = video_data.get("title", "")
    video_link = video_data.get("link", "")
    mentions = video_data.get("mentions_in_caption", [])
    hashtags = video_data.get("hashtags", [])
    
    if not source_username:
        return relations
    
    # 1. Extract mentions dari caption
    for mention in mentions:
        if mention.lower() != source_username.lower():
            relations.append({
                "source": source_username,
                "target": mention,
                "relation": "mentioned_in_video",
                "video_url": video_link,
                "video_title": video_title[:100],
                "timestamp": video_data.get("timestamp", ""),
                "scraped_at": video_data.get("scraped_at", "")
            })
    
    # 2. Self-mention
    self_mentions = [m for m in mentions if m.lower() == source_username.lower()]
    if self_mentions:
        relations.append({
            "source": source_username,
            "target": source_username,
            "relation": "self_mention",
            "video_url": video_link,
            "video_title": video_title[:100],
            "timestamp": video_data.get("timestamp", ""),
            "scraped_at": video_data.get("scraped_at", "")
        })
    
    # 3. ===== TAMBAHAN: HASHTAG RELATIONS =====
    if not hashtags:
        # Ekstrak hashtag dari video title jika belum ada
        hashtags = re.findall(r'#\w+', video_title)
    
    for hashtag in hashtags:
        # Pastikan hashtag dimulai dengan #
        if not hashtag.startswith("#"):
            hashtag = "#" + hashtag
        
        # Buat relasi user → hashtag
        relations.append({
            "source": source_username,
            "target": hashtag,
            "relation": "hashtag_use",
            "video_url": video_link,
            "video_title": video_title[:100],
            "timestamp": video_data.get("timestamp", ""),
            "scraped_at": video_data.get("scraped_at", "")
        })
    
    return relations

def save_sna_relations(relations_data, filename):
    """Simpan data relasi SNA ke CSV"""
    if not relations_data:
        print("Warning: Tidak ada data relasi SNA untuk disimpan")
        return 0
    
    new_df = pd.DataFrame(relations_data)
    
    if os.path.exists(filename):
        existing_df = pd.read_csv(filename, encoding='utf-8-sig')
        combined_df = pd.concat([existing_df, new_df], ignore_index=True)
        
        before_dedup = len(combined_df)
        combined_df = combined_df.drop_duplicates(
            subset=['source', 'target', 'relation', 'video_url'], 
            keep='last'
        )
        after_dedup = len(combined_df)
        
        new_relations_count = len(new_df)
        unique_new_relations = before_dedup - len(existing_df)
        
        print(f"Relasi SNA sebelumnya: {len(existing_df)}")
        print(f"Relasi baru: {new_relations_count}")
        print(f"Relasi unik baru: {unique_new_relations}")
        print(f"Total setelah deduplication: {after_dedup}")
        
    else:
        combined_df = new_df
        new_relations_count = len(new_df)
        unique_new_relations = new_relations_count
        print(f"File SNA baru dibuat dengan {len(combined_df)} relasi")
    
    combined_df.to_csv(filename, index=False, encoding='utf-8-sig')
    print(f"Data SNA tersimpan di {filename}")
    
    return unique_new_relations

# TAMBAHAN: Analisis Hashtag Network untuk TikTok
def analyze_hashtag_network(filename):
    """Analisis network berdasarkan penggunaan hashtag di TikTok"""
    if not os.path.exists(filename):
        print("File SNA tidak ditemukan")
        return
    
    try:
        df = pd.read_csv(filename, encoding='utf-8-sig')
        
        # Filter relasi hashtag
        hashtag_relations = df[df['relation'] == 'hashtag_use']
        
        if hashtag_relations.empty:
            print("Tidak ada data relasi hashtag ditemukan")
            return
        
        print(f"\nAnalisis Hashtag Network ({len(hashtag_relations)} relasi):")
        print("=" * 50)
        
        # Top hashtags yang paling banyak digunakan
        hashtag_counts = hashtag_relations['target'].value_counts().head(10)
        print("\nTop 10 Hashtags:")
        for i, (hashtag, count) in enumerate(hashtag_counts.items(), 1):
            print(f"   {i:2}. {hashtag}: {count} creator")
        
        # Creators yang paling banyak menggunakan hashtag
        creator_hashtag_counts = hashtag_relations['source'].value_counts().head(10)
        print("\nTop 10 Creators by Hashtag Usage:")
        for i, (creator, count) in enumerate(creator_hashtag_counts.items(), 1):
            print(f"   {i:2}. {creator}: {count} hashtags")
        
        # Hashtag co-occurrence analysis
        print("\nHashtag Co-occurrence (creators yang menggunakan multiple hashtags):")
        creator_hashtags = hashtag_relations.groupby('source')['target'].apply(list)
        
        # Cari creators dengan multiple hashtags
        multi_hashtag_creators = creator_hashtags[creator_hashtags.apply(len) > 1].head(5)
        
        for creator, hashtags in multi_hashtag_creators.items():
            hashtag_str = ", ".join(hashtags[:5])  # Limit to 5 hashtags for display
            if len(hashtags) > 5:
                hashtag_str += f" ... (+{len(hashtags)-5} more)"
            print(f"   • {creator}: {hashtag_str}")
        
        # Statistik umum
        unique_creators = hashtag_relations['source'].nunique()
        unique_hashtags = hashtag_relations['target'].nunique()
        avg_hashtags_per_creator = len(hashtag_relations) / unique_creators if unique_creators > 0 else 0
        
        print(f"\nStatistik Hashtag Network:")
        print(f"   • Total creators: {unique_creators}")
        print(f"   • Total unique hashtags: {unique_hashtags}")
        print(f"   • Avg hashtags per creator: {avg_hashtags_per_creator:.2f}")
        print(f"   • Total hashtag relations: {len(hashtag_relations)}")
        
    except Exception as e:
        print(f"Error analyzing hashtag network: {e}")

# ------------------------------
# Scrape main flow (search)
# ------------------------------
def scrape_tiktok_search(keyword, max_videos=1000, headless=True, fetch_likes_from_video_page=False):
    driver = setup_driver(headless=headless)
    results = []
    sna_relations = []

    try:
        base_url = f"https://www.tiktok.com/search?q={keyword.replace(' ', '%20')}"
        driver.get(base_url)
        time.sleep(6 + random.uniform(0, 2))

        seen_links = set()
        last_count = 0
        consecutive_empty_scrolls = 0
        max_empty_scrolls = 10

        print(f"Mulai scraping untuk keyword: '{keyword}'")
        print(f"Target: {max_videos} video")

        while True:
            containers = driver.find_elements(By.XPATH, '//div[contains(@class, "DivItemContainerForSearch")]') \
                         or driver.find_elements(By.XPATH, '//div[contains(@class, "DivItemContainer")]') \
                         or driver.find_elements(By.XPATH, '//div[contains(@class, "video-feed-item")]')

            if len(containers) >= max_videos:
                print(f"Target tercapai: {len(containers)} containers ditemukan")
                break

            if len(containers) == last_count:
                consecutive_empty_scrolls += 1
                if consecutive_empty_scrolls >= max_empty_scrolls:
                    print(f"Berhenti scroll: tidak ada konten baru setelah {consecutive_empty_scrolls} attempts")
                    break
            else:
                consecutive_empty_scrolls = 0

            last_count = len(containers)
            driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
            time.sleep(random.uniform(2.0, 3.5))

        print(f"Total video yang akan diproses: {len(containers)} (target {max_videos})")

        count = 0
        for container in containers:
            if count >= max_videos:
                break
            video = extract_video_data(container, driver=driver, fetch_likes_from_video_page=fetch_likes_from_video_page)
            if not video:
                continue
            if video["link"] in seen_links:
                continue

            seen_links.add(video["link"])
            results.append(video)
            count += 1

            # Ekstrak relasi SNA
            video_relations = extract_sna_relations(video)
            sna_relations.extend(video_relations)

            if count % 25 == 0:
                print(f"  Progress: {count}/{max_videos} video, {len(sna_relations)} relasi SNA")

        print(f"Scraping selesai. Dapat {len(results)} video dan {len(sna_relations)} relasi SNA.")

    except Exception as e:
        print("Error main scrape:", e)
    finally:
        driver.quit()

    return results, sna_relations

# ------------------------------
# Save to CSV functions dengan intelligent update
# ------------------------------
def save_videos_to_csv_improved(videos_data, filename):
    """
    Simpan data video ke CSV dengan intelligent update untuk engagement metrics
    """
    if not videos_data:
        print("Warning: Tidak ada data video untuk disimpan")
        return 0
    
    new_df = pd.DataFrame(videos_data)
    
    if os.path.exists(filename):
        # Baca existing data
        existing_df = pd.read_csv(filename, encoding='utf-8-sig')
        
        print(f"Data sebelumnya: {len(existing_df)} video")
        print(f"Data scraped: {len(new_df)} video")
        
        # Gunakan fungsi compare_and_update untuk intelligent merging
        combined_df = compare_and_update_video_data(existing_df, new_df)
        
        # Hitung statistik
        new_videos = set(new_df['link']) - set(existing_df['link'])
        new_videos_count = len(new_videos)
        
        print(f"Video baru (unique): {new_videos_count} video")
        print(f"Total setelah intelligent update: {len(combined_df)} video")
        
    else:
        combined_df = new_df
        new_videos_count = len(new_df)
        print(f"File baru dibuat dengan {len(combined_df)} video")
    
    # Simpan ke CSV
    combined_df.to_csv(filename, index=False, encoding='utf-8-sig')
    print(f"Data video tersimpan di {filename}")
    
    return new_videos_count

# ------------------------------
# FIXED: Analisis trend engagement
# ------------------------------
def analyze_engagement_trends(filename, days_back=7):
    """FIXED: Analisis trend engagement dalam beberapa hari terakhir"""
    if not os.path.exists(filename):
        print("Warning: File data tidak ditemukan")
        return
    
    try:
        df = pd.read_csv(filename, encoding='utf-8-sig')
        df['scraped_at'] = pd.to_datetime(df['scraped_at'], errors='coerce')
        
        # Filter data beberapa hari terakhir
        cutoff_date = datetime.now() - pd.Timedelta(days=days_back)
        recent_df = df[df['scraped_at'] >= cutoff_date]
        
        if recent_df.empty:
            print(f"Warning: Tidak ada data dalam {days_back} hari terakhir")
            return
        
        # Convert metrics ke numeric
        metric_columns = ['likes', 'views', 'shares', 'comments']
        for col in metric_columns:
            if col in recent_df.columns:
                recent_df[col] = pd.to_numeric(recent_df[col], errors='coerce').fillna(0)
        
        # Group by date dan hitung metrics
        recent_df['date'] = recent_df['scraped_at'].dt.date
        daily_stats = recent_df.groupby('date').agg({
            'likes': ['sum', 'mean', 'count'],
            'views': ['sum', 'mean'],
            'shares': ['sum', 'mean'],
            'comments': ['sum', 'mean']
        }).round(2)
        
        print(f"\nEngagement Trends ({days_back} hari terakhir):")
        print("=" * 60)
        
        for date, stats in daily_stats.iterrows():
            video_count = int(stats[('likes', 'count')])
            total_likes = int(stats[('likes', 'sum')])
            avg_likes = stats[('likes', 'mean')]
            total_views = int(stats[('views', 'sum')])
            
            print(f"{date}: {video_count} videos")
            print(f"    Total likes: {total_likes:,} (avg: {avg_likes:.1f})")
            print(f"    Total views: {total_views:,}")
        
    except Exception as e:
        print(f"Warning: Error analyzing trends: {e}")

# ------------------------------
# Automated functions
# ------------------------------
def get_next_keyword():
    """Rotate through different keyword variations untuk diversitas"""
    keywords = CONFIG["keyword_variations"]
    current_idx = CONFIG["current_keyword_index"]
    keyword = keywords[current_idx]
    
    # Update index untuk keyword berikutnya
    CONFIG["current_keyword_index"] = (current_idx + 1) % len(keywords)
    return keyword

def automated_scraping_improved():
    """Fungsi scraping otomatis dengan intelligent update"""
    # Gunakan keyword yang berbeda setiap run
    current_keyword = get_next_keyword()
    
    print(f"\n[{datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M:%S')}] Memulai scraping otomatis TikTok...")
    print(f"Keyword: '{current_keyword}'")
    
    # Scraping dengan keyword yang dipilih
    videos, sna_relations = scrape_tiktok_search(
        keyword=current_keyword,
        max_videos=CONFIG["max_videos"],
        headless=CONFIG["headless"],
        fetch_likes_from_video_page=CONFIG["fetch_likes_from_video_page"]
    )
    
    if videos:
        # Simpan data video dengan intelligent update
        new_videos_count = save_videos_to_csv_improved(videos, CONFIG["csv_filename"])
        
        # Simpan data SNA relations
        new_relations_count = save_sna_relations(sna_relations, CONFIG["sna_filename"])
        
        if new_videos_count and new_videos_count > 0:
            print(f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] Scraping selesai:")
            print(f"   {new_videos_count} video baru")
            print(f"   {new_relations_count} relasi SNA baru")
        else:
            print(f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] Scraping selesai, tidak ada data baru ditemukan")
        
        # Monitor engagement changes
        monitor_engagement_changes(CONFIG["csv_filename"])
        
    else:
        print(f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] Scraping selesai, tidak ada data ditemukan")
    
    print(f"Scraping berikutnya dalam {CONFIG['interval_minutes']} menit dengan keyword berikutnya...")

# ------------------------------
# Main runner
# ------------------------------
def main():
    print("=" * 70)
    print("TIKTOK AUTO SCRAPER V2 - INTELLIGENT ENGAGEMENT UPDATE + SNA + HASHTAG")
    print("=" * 70)
    print(f"Konfigurasi:")
    print(f"   • Keyword rotasi: {len(CONFIG['keyword_variations'])} variasi")
    for i, k in enumerate(CONFIG['keyword_variations'], 1):
        print(f"     {i}. '{k}'")
    print(f"   • Max videos per run: {CONFIG['max_videos']}")
    print(f"   • Interval: {CONFIG['interval_minutes']} menit")
    print(f"   • Output files:")
    print(f"     - Videos: {CONFIG['csv_filename']}")
    print(f"     - SNA Relations: {CONFIG['sna_filename']}")
    print(f"   • Mode headless: {'Ya' if CONFIG['headless'] else 'Tidak'}")
    print("=" * 70)
    print("Fitur SNA (Social Network Analysis) + HASHTAG:")
    print("   • Mentioned_in_video: @user1 menyebut @user2 dalam video")
    print("   • Self_mention: @user1 menyebut dirinya sendiri")
    print("   • Hashtag_use: @user1 menggunakan #hashtag")  # BARIS BARU
    print("=" * 70)
    print("Fitur Intelligent Update:")
    print("   • Auto-update engagement metrics (likes, views, shares, comments)")
    print("   • Deteksi perubahan engagement dari scraping sebelumnya")
    print("   • Monitoring trend engagement harian")
    print("   • Deduplication berdasarkan video link")
    print("=" * 70)
    print("Format Timestamp: ISO 8601 (konsisten dengan Twitter scraper)")
    print("   • Contoh: 2024-08-20T14:30:00+00:00")
    print("   • Support relative time: '2 jam lalu' → ISO timestamp")
    print("=" * 70)
    
    # Jalankan scraping pertama kali
    print("Menjalankan scraping pertama kali...")
    automated_scraping_improved()
    
    # Jadwalkan scraping otomatis
    schedule.every(CONFIG["interval_minutes"]).minutes.do(automated_scraping_improved)
    
    print(f"\nScheduler aktif! Press Ctrl+C untuk berhenti.")
    print(f"Tip: Setiap run akan menggunakan keyword yang berbeda untuk diversitas data")
    print(f"Data SNA akan tersimpan dalam format: source | target | relation")
    print(f"Hashtag relations akan menunjukkan: creator | hashtag | hashtag_use")  # BARIS BARU
    print(f"Timestamp akan dinormalisasi ke format ISO 8601 (UTC)")
    print(f"Engagement metrics akan di-update otomatis jika ada peningkatan")
    
    try:
        while True:
            schedule.run_pending()
            time.sleep(30)  # Check setiap 30 detik
    except KeyboardInterrupt:
        print("\n\nScraper dihentikan oleh user.")
        print(f"Data video tersimpan di: {CONFIG['csv_filename']}")
        print(f"Data SNA relations tersimpan di: {CONFIG['sna_filename']}")
        
        # Tampilkan ringkasan data SNA jika file ada
        if os.path.exists(CONFIG['sna_filename']):
            try:
                sna_df = pd.read_csv(CONFIG['sna_filename'], encoding='utf-8-sig')
                print(f"\nRingkasan Data SNA:")
                print(f"   • Total relasi: {len(sna_df)}")
                
                relation_counts = sna_df['relation'].value_counts()
                for relation_type, count in relation_counts.items():
                    print(f"   • {relation_type}: {count} relasi")
                
                unique_nodes = set(sna_df['source'].tolist() + sna_df['target'].tolist())
                print(f"   • Unique nodes (creators + hashtags): {len(unique_nodes)}")  # UBAH DARI "users" ke "creators + hashtags"
                
                # TAMBAHKAN: Analisis hashtag network
                analyze_hashtag_network(CONFIG['sna_filename'])
                
            except Exception as e:
                print(f"   Warning: Tidak dapat membaca ringkasan SNA: {e}")
        
        # Tampilkan analisis trend engagement
        if os.path.exists(CONFIG['csv_filename']):
            try:
                print("\nAnalisis Trend Engagement:")
                analyze_engagement_trends(CONFIG['csv_filename'], days_back=7)
            except Exception as e:
                print(f"   Warning: Tidak dapat menganalisis trend: {e}")

if __name__ == "__main__":
    main()