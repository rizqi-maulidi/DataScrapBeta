from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from webdriver_manager.chrome import ChromeDriverManager
import pandas as pd
import time
import random
import re
import os
import schedule
from datetime import datetime
import os
import warnings
import logging

# Suppress warnings dan logs
warnings.filterwarnings("ignore")
os.environ['TF_CPP_MIN_LOG_LEVEL'] = '3'  # TensorFlow logs
os.environ['PYTHONWARNINGS'] = 'ignore'
os.environ['WDM_LOG'] = '0'  # WebDriverManager logs

# Disable various logging
logging.getLogger('selenium').setLevel(logging.WARNING)
logging.getLogger('urllib3').setLevel(logging.WARNING)
logging.getLogger().setLevel(logging.ERROR)
# ======== KONFIGURASI OTOMATIS ========
CONFIG = {
    "use_login": True,
    "email_or_username": "datakusavior@gmail.com",
    "actual_username": "@datakusavior",
    "password": "#Datakusavior123",
    "query": "politik indonesia",
    "max_tweets": 150,  # Set tinggi untuk mendapatkan lebih banyak data
    "csv_filename": "twitter_politik_indonesia_auto.csv",
    "sna_filename": "twitter_sna_relations.csv",  # File untuk data SNA
    "interval_minutes": 10,  # Perpanjang interval untuk proses yang lebih lama
    "query_variations": [  # Tambah variasi query untuk diversitas
        "politik indonesia",
        "politik dinasti", 
        "pemilu 2024 indonesia", 
        "pemerintah indonesia",
        "DPR indonesia",
        "pilpres indonesia",
        "kabinet indonesia",
        "koalisi pemerintah",
        "oposisi indonesia"
    ],
    "current_query_index": 0
}

# ======== SETUP DRIVER ========
def setup_twitter_driver(headless=True):
    """Setup ChromeDriver untuk Twitter/X dengan opsi anti-detection dan error suppression"""
    options = Options()

    if not headless:
        options.add_argument("--start-maximized")
    else:
        options.add_argument("--headless")
        options.add_argument("--window-size=1920,1080")

    # Anti-detection
    options.add_argument("--disable-notifications")
    options.add_argument("--disable-popup-blocking")
    options.add_argument("--disable-blink-features=AutomationControlled")
    options.add_experimental_option("excludeSwitches", ["enable-automation"])
    options.add_experimental_option('useAutomationExtension', False)
    options.add_argument("--no-sandbox")
    options.add_argument("--disable-dev-shm-usage")
    options.add_argument("--disable-gpu")
    
    # TAMBAHAN: Suppress error messages dan warnings
    options.add_argument("--disable-logging")
    options.add_argument("--log-level=3")  # Only fatal errors
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
    
    # Suppress specific GPU/WebGL errors
    options.add_argument("--use-gl=swiftshader")
    options.add_argument("--disable-webgl")
    options.add_argument("--disable-webgl2") 
    options.add_argument("--disable-3d-apis")
    options.add_argument("--disable-accelerated-2d-canvas")
    options.add_argument("--disable-accelerated-jpeg-decoding")
    options.add_argument("--disable-accelerated-mjpeg-decode")
    options.add_argument("--disable-accelerated-video-decode")
    
    # Experimental options untuk suppress logs
    options.add_experimental_option('excludeSwitches', ['enable-logging'])
    options.add_experimental_option('useAutomationExtension', False)
    
    # Set log preferences untuk minimize output
    prefs = {
        "profile.default_content_setting_values": {
            "notifications": 2
        }
    }
    options.add_experimental_option("prefs", prefs)

    user_agents = [
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
        "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
    ]
    options.add_argument(f"--user-agent={random.choice(user_agents)}")

    service = Service(ChromeDriverManager().install())
    
    # TAMBAHAN: Suppress service logs juga
    service.creation_flags = 0x08000000  # CREATE_NO_WINDOW flag untuk Windows
    
    driver = webdriver.Chrome(service=service, options=options)

    # Hapus properti webdriver untuk anti-bot
    driver.execute_cdp_cmd("Page.addScriptToEvaluateOnNewDocument", {
        "source": """
            Object.defineProperty(navigator, 'webdriver', {get: () => undefined});
            Object.defineProperty(navigator, 'plugins', {get: () => [1, 2, 3, 4, 5]});
            Object.defineProperty(navigator, 'languages', {get: () => ['en-US', 'en']});
        """
    })

    return driver

# ======== LOGIN ========
def login_to_twitter(driver, email_or_username, password, actual_username=None):
    """Login ke Twitter/X (support flow Email → Username → Password)"""
    try:
        print(f"🔐 [{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] Mencoba login ke Twitter/X...")
        driver.get("https://twitter.com/login")
        WebDriverWait(driver, 15).until(
            EC.presence_of_element_located((By.XPATH, '//input[@name="text"]'))
        )

        # Step 1: Input email/username pertama
        username_input = driver.find_element(By.XPATH, '//input[@name="text"]')
        username_input.clear()
        username_input.send_keys(email_or_username)
        driver.find_element(By.XPATH, '//span[text()="Next"]').click()
        time.sleep(3)

        # Step 2 (opsional): Kalau Twitter minta username
        try:
            username_field = WebDriverWait(driver, 3).until(
                EC.presence_of_element_located((By.XPATH, '//input[@name="text"]'))
            )
            if username_field.is_displayed():
                uname_to_use = actual_username or email_or_username
                username_field.clear()
                username_field.send_keys(uname_to_use)
                driver.find_element(By.XPATH, '//span[text()="Next"]').click()
                time.sleep(3)
        except:
            pass  # Tidak minta username → lanjut ke password

        # Step 3: Input password
        password_input = WebDriverWait(driver, 10).until(
            EC.presence_of_element_located((By.XPATH, '//input[@name="password"]'))
        )
        password_input.clear()
        password_input.send_keys(password)
        driver.find_element(By.XPATH, '//span[text()="Log in"]').click()
        time.sleep(5)

        if "home" in driver.current_url.lower():
            print("✅ Login berhasil!")
            return True
        else:
            print("⚠️ Login mungkin gagal atau butuh verifikasi tambahan")
            return False

    except Exception as e:
        print(f"❌ Error saat login: {e}")
        return False

# ======== FUNGSI SNA - EKSTRAK RELASI (MODIFIED WITH HASHTAG) ========
def extract_sna_relations(tweet_data):
    """Ekstrak relasi social network dari tweet data termasuk hashtag relations"""
    relations = []
    
    # Pastikan username dimulai dengan @
    source_username = tweet_data.get("username", "")
    if source_username and not source_username.startswith("@"):
        source_username = "@" + source_username
    
    tweet_text = tweet_data.get("tweet_text", "")
    tweet_url = tweet_data.get("tweet_url", "")
    
    if not source_username:
        return relations
    
    # 1. Ekstrak mentions (@username)
    mentions = re.findall(r'@\w+', tweet_text)
    for mention in mentions:
        if mention.lower() != source_username.lower():  # Jangan mention diri sendiri
            relations.append({
                "source": source_username,
                "target": mention,
                "relation": "mention",
                "tweet_url": tweet_url,
                "timestamp": tweet_data.get("timestamp", ""),
                "scraped_at": tweet_data.get("scraped_at", "")
            })
    
    # 2. Deteksi reply (dari konteks atau URL pattern)
    is_reply = False
    replied_to = None
    
    # Cek apakah ini adalah reply berdasarkan URL pattern atau konten
    if "/status/" in tweet_url and "reply" in tweet_text.lower():
        # Cari mention pertama sebagai target reply
        if mentions:
            replied_to = mentions[0]
            is_reply = True
    
    # Bisa juga deteksi dari struktur tweet (jika ada data parent tweet)
    # Atau dari indikator visual lainnya yang bisa di-scrape
    
    if is_reply and replied_to:
        relations.append({
            "source": source_username,
            "target": replied_to,
            "relation": "reply",
            "tweet_url": tweet_url,
            "timestamp": tweet_data.get("timestamp", ""),
            "scraped_at": tweet_data.get("scraped_at", "")
        })
    
    # 3. Deteksi retweet/quote tweet
    is_retweet = tweet_data.get("is_retweet", False)
    if is_retweet or "RT @" in tweet_text:
        # Ekstrak username yang di-retweet
        rt_match = re.search(r'RT @(\w+)', tweet_text)
        if rt_match:
            retweeted_user = "@" + rt_match.group(1)
            relations.append({
                "source": source_username,
                "target": retweeted_user,
                "relation": "retweet",
                "tweet_url": tweet_url,
                "timestamp": tweet_data.get("timestamp", ""),
                "scraped_at": tweet_data.get("scraped_at", "")
            })
    
    # 4. Self-mention (untuk kasus khusus seperti thread)
    self_mentions = [m for m in mentions if m.lower() == source_username.lower()]
    if self_mentions:
        relations.append({
            "source": source_username,
            "target": source_username,
            "relation": "self_mention",
            "tweet_url": tweet_url,
            "timestamp": tweet_data.get("timestamp", ""),
            "scraped_at": tweet_data.get("scraped_at", "")
        })
    
    # 5. ===== TAMBAHAN: HASHTAG RELATIONS =====
    hashtags = tweet_data.get("hashtags", [])
    if not hashtags:
        # Ekstrak hashtag dari tweet text jika belum ada
        hashtags = re.findall(r'#\w+', tweet_text)
    
    for hashtag in hashtags:
        # Pastikan hashtag dimulai dengan #
        if not hashtag.startswith("#"):
            hashtag = "#" + hashtag
        
        # Buat relasi user → hashtag
        relations.append({
            "source": source_username,
            "target": hashtag,
            "relation": "hashtag_use",
            "tweet_url": tweet_url,
            "timestamp": tweet_data.get("timestamp", ""),
            "scraped_at": tweet_data.get("scraped_at", "")
        })
    
    return relations

def save_sna_relations(relations_data, filename):
    """Simpan data relasi SNA ke CSV"""
    if not relations_data:
        print("⚠️ Tidak ada data relasi SNA untuk disimpan")
        return 0
    
    new_df = pd.DataFrame(relations_data)
    
    # Cek apakah file sudah ada
    if os.path.exists(filename):
        existing_df = pd.read_csv(filename, encoding='utf-8-sig')
        
        # Gabungkan dengan data baru
        combined_df = pd.concat([existing_df, new_df], ignore_index=True)
        
        # Hapus duplikat berdasarkan kombinasi source-target-relation-tweet_url
        before_dedup = len(combined_df)
        combined_df = combined_df.drop_duplicates(
            subset=['source', 'target', 'relation', 'tweet_url'], 
            keep='last'
        )
        after_dedup = len(combined_df)
        
        new_relations_count = len(new_df)
        unique_new_relations = before_dedup - len(existing_df)
        
        print(f"📊 Relasi SNA sebelumnya: {len(existing_df)}")
        print(f"📊 Relasi baru: {new_relations_count}")
        print(f"📊 Relasi unik baru: {unique_new_relations}")
        print(f"📊 Total setelah deduplication: {after_dedup}")
        
    else:
        combined_df = new_df
        new_relations_count = len(new_df)
        unique_new_relations = new_relations_count
        print(f"📊 File SNA baru dibuat dengan {len(combined_df)} relasi")
    
    # Simpan ke CSV
    combined_df.to_csv(filename, index=False, encoding='utf-8-sig')
    print(f"✅ Data SNA tersimpan di {filename}")
    
    return unique_new_relations

# ======== FUNGSI ANALISIS HASHTAG SNA ========
def analyze_hashtag_network(filename):
    """Analisis network berdasarkan penggunaan hashtag"""
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
        
        print(f"\n📊 Analisis Hashtag Network ({len(hashtag_relations)} relasi):")
        print("=" * 50)
        
        # Top hashtags yang paling banyak digunakan
        hashtag_counts = hashtag_relations['target'].value_counts().head(10)
        print("\n🏷️ Top 10 Hashtags:")
        for i, (hashtag, count) in enumerate(hashtag_counts.items(), 1):
            print(f"   {i:2}. {hashtag}: {count} pengguna")
        
        # Users yang paling banyak menggunakan hashtag
        user_hashtag_counts = hashtag_relations['source'].value_counts().head(10)
        print("\n👥 Top 10 Users by Hashtag Usage:")
        for i, (user, count) in enumerate(user_hashtag_counts.items(), 1):
            print(f"   {i:2}. {user}: {count} hashtags")
        
        # Hashtag co-occurrence analysis
        print("\n🔗 Hashtag Co-occurrence (users yang menggunakan multiple hashtags):")
        user_hashtags = hashtag_relations.groupby('source')['target'].apply(list)
        
        # Cari users dengan multiple hashtags
        multi_hashtag_users = user_hashtags[user_hashtags.apply(len) > 1].head(5)
        
        for user, hashtags in multi_hashtag_users.items():
            hashtag_str = ", ".join(hashtags[:5])  # Limit to 5 hashtags for display
            if len(hashtags) > 5:
                hashtag_str += f" ... (+{len(hashtags)-5} more)"
            print(f"   • {user}: {hashtag_str}")
        
        # Statistik umum
        unique_users = hashtag_relations['source'].nunique()
        unique_hashtags = hashtag_relations['target'].nunique()
        avg_hashtags_per_user = len(hashtag_relations) / unique_users if unique_users > 0 else 0
        
        print(f"\n📈 Statistik Hashtag Network:")
        print(f"   • Total users: {unique_users}")
        print(f"   • Total unique hashtags: {unique_hashtags}")
        print(f"   • Avg hashtags per user: {avg_hashtags_per_user:.2f}")
        print(f"   • Total hashtag relations: {len(hashtag_relations)}")
        
    except Exception as e:
        print(f"Error analyzing hashtag network: {e}")

# ======== EKSTRAK TWEET (MODIFIED) ========
def extract_tweet_data(tweet_element):
    """Ekstrak data dari elemen tweet"""
    data = {
        "username": "",
        "display_name": "",
        "tweet_text": "",
        "timestamp": "",
        "replies": "0",
        "retweets": "0",
        "likes": "0",
        "views": "0",
        "tweet_url": "",
        "is_retweet": False,
        "hashtags": [],
        "mentions": [],
        "scraped_at": datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    }

    try:
        # Username & Display name
        try:
            username_elem = tweet_element.find_element(By.XPATH, './/div[@data-testid="User-Name"]//span[contains(text(), "@")]')
            data["username"] = username_elem.text.strip()
            display_elem = tweet_element.find_element(By.XPATH, './/div[@data-testid="User-Name"]//span[not(contains(text(), "@"))]')
            data["display_name"] = display_elem.text.strip()
        except:
            pass

        # Tweet text
        try:
            tweet_text_elem = tweet_element.find_element(By.XPATH, './/div[@data-testid="tweetText"]')
            data["tweet_text"] = tweet_text_elem.text.strip()
            data["hashtags"] = re.findall(r'#\w+', data["tweet_text"])
            data["mentions"] = re.findall(r'@\w+', data["tweet_text"])
            
            # Deteksi retweet
            data["is_retweet"] = data["tweet_text"].startswith("RT @") or "retweeted" in data["tweet_text"].lower()
        except:
            pass

        # Timestamp
        try:
            time_elem = tweet_element.find_element(By.XPATH, './/time')
            data["timestamp"] = time_elem.get_attribute('datetime')
        except:
            pass

        # Permalink tweet → tweet_url
        try:
            link_el = tweet_element.find_element(
                By.XPATH,
                './/a[contains(@href, "/status/")][.//time]'
            )
            href = link_el.get_attribute("href")
            if href:
                data["tweet_url"] = href.split("?")[0]
        except:
            try:
                link_el = tweet_element.find_element(By.XPATH, './/a[contains(@href, "/status/")]')
                href = link_el.get_attribute("href")
                if href:
                    data["tweet_url"] = href.split("?")[0]
            except:
                pass

        # Engagement metrics
        try:
            # Replies
            reply_btn = tweet_element.find_element(By.XPATH, './/button[@data-testid="reply"]')
            reply_text = reply_btn.get_attribute("aria-label") or ""
            reply_nums = re.findall(r"(\d+)", reply_text)
            data["replies"] = reply_nums[0] if reply_nums else "0"
        except:
            data["replies"] = "0"

        try:
            # Retweets (reposts)
            retweet_btn = tweet_element.find_element(By.XPATH, './/button[@data-testid="retweet"]')
            retweet_text = retweet_btn.get_attribute("aria-label") or ""
            retweet_nums = re.findall(r"(\d+)", retweet_text)
            data["retweets"] = retweet_nums[0] if retweet_nums else "0"
        except:
            data["retweets"] = "0"

        try:
            # Likes
            like_btn = tweet_element.find_element(By.XPATH, './/button[@data-testid="like"]')
            like_text = like_btn.get_attribute("aria-label") or ""
            like_nums = re.findall(r"(\d+)", like_text)
            data["likes"] = like_nums[0] if like_nums else "0"
        except:
            data["likes"] = "0"

        try:
            # Views
            try:
                view_elem = tweet_element.find_element(
                    By.XPATH, './/a[contains(@href,"/analytics") and contains(@aria-label,"Views")]'
                )
                view_text = view_elem.get_attribute("aria-label") or ""
                view_nums = re.findall(r"(\d+)", view_text)
                data["views"] = view_nums[0] if view_nums else "0"
            except:
                group_div = tweet_element.find_element(
                    By.XPATH, './/div[@role="group" and contains(@aria-label,"views")]'
                )
                aria_text = group_div.get_attribute("aria-label") or ""
                match = re.search(r"(\d+)\s+views", aria_text, re.IGNORECASE)
                data["views"] = match.group(1) if match else "0"
        except:
            data["views"] = "0"

        return data if data["tweet_text"] else None

    except Exception as e:
        print(f"❌ Error ekstrak tweet: {e}")
        return None

# ======== SCRAPER (MODIFIED) ========
def scrape_twitter_search(query, max_tweets=50, use_login=False, email_or_username="", password="", actual_username=None, since_id=None):
    driver = setup_twitter_driver(headless=True)  # Ubah ke True untuk headless
    tweets_data = []
    sna_relations = []  # List untuk menyimpan relasi SNA
    
    try:
        if use_login:
            if not login_to_twitter(driver, email_or_username, password, actual_username):
                print("⚠️ Login gagal → lanjut tanpa login")
        else:
            driver.get("https://twitter.com")
            time.sleep(3)

        # Strategi pencarian
        search_strategies = [
            f"https://twitter.com/search?q={query}&src=typed_query&f=live",  # Latest
            f"https://twitter.com/search?q={query}%20-filter%3Areplies&src=typed_query&f=live",  # Tanpa replies
            f"https://twitter.com/search?q={query}%20min_faves%3A1&src=typed_query&f=live",  # Min 1 like
        ]
        
        for strategy_idx, search_url in enumerate(search_strategies):
            if len(tweets_data) >= max_tweets:
                break
                
            print(f"🔍 Strategi {strategy_idx + 1}/3: {['Latest', 'Tanpa replies', 'Min 1 like'][strategy_idx]}")
            driver.get(search_url)
            time.sleep(5)

            seen_in_strategy = set()
            scroll_attempts = 0
            max_scroll_attempts = min(100, max(30, max_tweets // 10))
            consecutive_empty_scrolls = 0
            strategy_tweets = 0
            last_height = 0
            
            print(f"📊 Target untuk strategi ini: {max_tweets - len(tweets_data)} tweets (max {max_scroll_attempts} scrolls)")
            
            while len(tweets_data) < max_tweets and scroll_attempts < max_scroll_attempts:
                tweets = driver.find_elements(By.XPATH, '//article[@data-testid="tweet"]')
                tweets_found_this_scroll = 0
                
                for tweet in tweets:
                    try:
                        # Gunakan tweet URL sebagai unique identifier
                        link_el = tweet.find_element(By.XPATH, './/a[contains(@href, "/status/")]')
                        tweet_url = link_el.get_attribute("href").split("?")[0] if link_el else None
                        
                        if tweet_url and tweet_url not in seen_in_strategy:
                            seen_in_strategy.add(tweet_url)
                            
                            # Cek apakah tweet sudah ada di tweets_data global
                            existing_urls = {t['tweet_url'] for t in tweets_data}
                            if tweet_url not in existing_urls:
                                data = extract_tweet_data(tweet)
                                if data and data['tweet_text'].strip():
                                    tweets_data.append(data)
                                    strategy_tweets += 1
                                    tweets_found_this_scroll += 1
                                    
                                    # ===== EKSTRAK RELASI SNA =====
                                    tweet_relations = extract_sna_relations(data)
                                    sna_relations.extend(tweet_relations)
                                    
                                    if len(tweets_data) >= max_tweets:
                                        break
                    except:
                        continue
                
                # Progress control
                if tweets_found_this_scroll == 0:
                    consecutive_empty_scrolls += 1
                else:
                    consecutive_empty_scrolls = 0
                
                if consecutive_empty_scrolls >= 15:
                    print(f"⏹️ Strategi {strategy_idx + 1} berhenti: tidak ada tweet baru setelah {consecutive_empty_scrolls} scrolls")
                    break
                
                # Scroll logic
                current_height = driver.execute_script("return document.body.scrollHeight")
                if current_height == last_height and consecutive_empty_scrolls >= 2:
                    print(f"⏹️ Strategi {strategy_idx + 1} berhenti: mencapai end of feed")
                    break
                
                scroll_distance = random.randint(800, 1500)
                driver.execute_script(f"window.scrollBy(0, {scroll_distance});")
                
                delay = random.uniform(0.5, 1.5) if max_tweets <= 500 else random.uniform(1, 2)
                time.sleep(delay)
                
                scroll_attempts += 1
                last_height = current_height
                
                # Progress update
                if scroll_attempts % 10 == 0:
                    print(f"   📊 Progress: {len(tweets_data)}/{max_tweets} tweets | {len(sna_relations)} relasi SNA | Scroll {scroll_attempts}/{max_scroll_attempts}")
                
                # Refresh strategy
                if scroll_attempts % 50 == 0:
                    print(f"   🔄 Refresh untuk konten baru (scroll {scroll_attempts})")
                    driver.refresh()
                    time.sleep(3)
            
            print(f"✅ Strategi {strategy_idx + 1} selesai: +{strategy_tweets} tweets, +{len([r for r in sna_relations if 'strategy' not in r])} relasi (total: {len(tweets_data)} tweets, {len(sna_relations)} relasi)")
            
            if len(tweets_data) >= max_tweets:
                print(f"🎯 Target tercapai: {len(tweets_data)} tweets, {len(sna_relations)} relasi SNA")
                break
                
            if strategy_idx < len(search_strategies) - 1:
                time.sleep(3)

        print(f"📊 Total tweets dikumpulkan: {len(tweets_data)}")
        print(f"📊 Total relasi SNA dikumpulkan: {len(sna_relations)}")

    except Exception as e:
        print(f"❌ Error scraping: {e}")
    finally:
        driver.quit()

    return tweets_data, sna_relations
# ======== FUNGSI UNTUK MENANGANI UPDATE DATA ENGAGEMENT ========
def compare_and_update_tweet_data(existing_df, new_df):
    """
    Bandingkan data existing dengan data baru dan pilih yang terbaru
    berdasarkan timestamp scraping dan engagement metrics
    """
    if existing_df.empty:
        return new_df
    
    if new_df.empty:
        return existing_df
    
    # Buat copy untuk menghindari SettingWithCopyWarning
    existing_df = existing_df.copy()
    new_df = new_df.copy()
    
    # Convert scraped_at ke datetime dengan error handling untuk berbagai format
    def safe_datetime_conversion(df, col_name):
        """Safely convert datetime column with multiple format handling"""
        try:
            # Coba format ISO8601 terlebih dahulu
            df[col_name] = pd.to_datetime(df[col_name], format='ISO8601', errors='coerce')
        except:
            try:
                # Coba format mixed jika ISO8601 gagal
                df[col_name] = pd.to_datetime(df[col_name], format='mixed', errors='coerce')
            except:
                try:
                    # Coba infer format otomatis
                    df[col_name] = pd.to_datetime(df[col_name], infer_datetime_format=True, errors='coerce')
                except:
                    # Jika semua gagal, gunakan datetime sekarang sebagai fallback
                    print(f"⚠️ Warning: Tidak dapat mem-parsing tanggal, menggunakan timestamp sekarang")
                    df[col_name] = pd.to_datetime(datetime.now().strftime('%Y-%m-%d %H:%M:%S'))
        
        # Handle NaT (Not a Time) values
        nat_mask = df[col_name].isna()
        if nat_mask.any():
            print(f"⚠️ Warning: {nat_mask.sum()} rows memiliki tanggal invalid, menggunakan timestamp sekarang")
            df.loc[nat_mask, col_name] = pd.to_datetime(datetime.now().strftime('%Y-%m-%d %H:%M:%S'))
        
        return df
    
    # Apply safe datetime conversion
    existing_df = safe_datetime_conversion(existing_df, 'scraped_at')
    new_df = safe_datetime_conversion(new_df, 'scraped_at')
    
    # Convert metrics ke numeric untuk perbandingan
    metric_columns = ['likes', 'retweets', 'replies', 'views']
    for col in metric_columns:
        if col in existing_df.columns:
            existing_df[col] = pd.to_numeric(existing_df[col], errors='coerce').fillna(0)
        if col in new_df.columns:
            new_df[col] = pd.to_numeric(new_df[col], errors='coerce').fillna(0)
    
    updated_tweets = []
    updated_count = 0
    
    # Gabungkan semua data dengan indikator source
    existing_df['source'] = 'existing'
    new_df['source'] = 'new'
    combined_df = pd.concat([existing_df, new_df], ignore_index=True)
    
    # Group by tweet_url untuk membandingkan data
    for tweet_url, group in combined_df.groupby('tweet_url'):
        if len(group) == 1:
            # Tweet hanya ada di satu source, ambil apa adanya
            tweet_data = group.iloc[0].drop('source').to_dict()
            updated_tweets.append(tweet_data)
        else:
            # Tweet ada di kedua source, pilih yang terbaru atau yang engagement-nya lebih tinggi
            existing_tweet = group[group['source'] == 'existing']
            new_tweet = group[group['source'] == 'new']
            
            if not existing_tweet.empty and not new_tweet.empty:
                existing_data = existing_tweet.iloc[0]
                new_data = new_tweet.iloc[0]
                
                # Logika pemilihan data:
                # 1. Jika scraped_at baru lebih baru, pilih yang baru
                # 2. Jika engagement metrics baru lebih tinggi, pilih yang baru
                # 3. Jika tidak ada perbedaan signifikan, pilih yang baru (asumsi data terbaru)
                
                should_update = False
                update_reason = []
                
                # Cek apakah scraping time lebih baru
                try:
                    if new_data['scraped_at'] > existing_data['scraped_at']:
                        should_update = True
                        update_reason.append('waktu_scraping_lebih_baru')
                except:
                    # Jika ada masalah dengan comparison datetime, default ke update
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
                        username = selected_data.get('username', 'unknown')
                        print(f"📊 Updated {username}: {', '.join(changes[:3])}")  # Limit to 3 changes for readability
                else:
                    # Tidak ada update signifikan, gunakan data existing
                    selected_data = existing_data.drop('source').to_dict()
                
                updated_tweets.append(selected_data)
            else:
                # Fallback: ambil data yang ada
                tweet_data = group.iloc[0].drop('source').to_dict()
                updated_tweets.append(tweet_data)
    
    result_df = pd.DataFrame(updated_tweets)
    
    # Convert scraped_at kembali ke string format yang konsisten
    if 'scraped_at' in result_df.columns:
        result_df['scraped_at'] = result_df['scraped_at'].dt.strftime('%Y-%m-%d %H:%M:%S')
    
    print(f"🔄 Data comparison completed: {updated_count} tweets updated with newer engagement data")
    
    return result_df

# ======== FUNGSI UNTUK MONITORING PERUBAHAN ENGAGEMENT ========
def monitor_engagement_changes(filename):
    """
    Monitor dan laporan perubahan engagement metrics dari file CSV
    """
    if not os.path.exists(filename):
        return
    
    try:
        df = pd.read_csv(filename, encoding='utf-8-sig')
        
        # Convert metrics ke numeric
        metric_columns = ['likes', 'retweets', 'replies', 'views']
        for col in metric_columns:
            df[col] = pd.to_numeric(df[col], errors='coerce').fillna(0)
        
        # Tampilkan statistik engagement
        print(f"\n📈 Engagement Statistics (Total {len(df)} tweets):")
        for metric in metric_columns:
            total = df[metric].sum()
            avg = df[metric].mean()
            max_val = df[metric].max()
            print(f"   • {metric.capitalize()}: Total={total:,.0f}, Avg={avg:.1f}, Max={max_val:,.0f}")
        
        # Tampilkan top tweets berdasarkan engagement
        print(f"\n🏆 Top 5 Tweets by Total Engagement:")
        df['total_engagement'] = df['likes'] + df['retweets'] + df['replies']
        top_tweets = df.nlargest(5, 'total_engagement')
        
        for idx, tweet in top_tweets.iterrows():
            username = tweet.get('username', 'unknown')
            likes = int(tweet.get('likes', 0))
            retweets = int(tweet.get('retweets', 0))
            replies = int(tweet.get('replies', 0))
            total = int(tweet.get('total_engagement', 0))
            text_preview = tweet.get('tweet_text', '')[:50] + "..." if len(tweet.get('tweet_text', '')) > 50 else tweet.get('tweet_text', '')
            
            print(f"   {idx+1}. @{username} (Total: {total:,})")
            print(f"      👍 {likes:,} ❤️ | 🔄 {retweets:,} RT | 💬 {replies:,} replies")
            print(f"      📝 \"{text_preview}\"")
            print()
            
    except Exception as e:
        print(f"⚠️ Error monitoring engagement: {e}")

# ======== FUNGSI UTILITAS UNTUK ANALISIS DATA ========
def analyze_engagement_trends(filename, days_back=7):
    """
    Analisis trend engagement dalam beberapa hari terakhir
    """
    if not os.path.exists(filename):
        print("⚠️ File data tidak ditemukan")
        return
    
    try:
        df = pd.read_csv(filename, encoding='utf-8-sig')
        df['scraped_at'] = pd.to_datetime(df['scraped_at'])
        
        # Filter data beberapa hari terakhir
        cutoff_date = datetime.now() - pd.Timedelta(days=days_back)
        recent_df = df[df['scraped_at'] >= cutoff_date]
        
        if recent_df.empty:
            print(f"⚠️ Tidak ada data dalam {days_back} hari terakhir")
            return
        
        # Group by date dan hitung metrics
        recent_df['date'] = recent_df['scraped_at'].dt.date
        daily_stats = recent_df.groupby('date').agg({
            'likes': ['sum', 'mean', 'count'],
            'retweets': ['sum', 'mean'],
            'replies': ['sum', 'mean'],
            'views': ['sum', 'mean']
        }).round(2)
        
        print(f"\n📊 Engagement Trends ({days_back} hari terakhir):")
        print("=" * 60)
        
        for date, stats in daily_stats.iterrows():
            tweet_count = int(stats[('likes', 'count')])
            total_likes = int(stats[('likes', 'sum')])
            avg_likes = stats[('likes', 'mean')]
            
            print(f"📅 {date}: {tweet_count} tweets, {total_likes:,} total likes (avg: {avg_likes:.1f})")
        
    except Exception as e:
        print(f"⚠️ Error analyzing trends: {e}")

# ======== SAVE TO CSV (IMPROVED VERSION) ========
def save_tweets_to_csv_improved(tweets_data, filename):
    """
    Simpan data tweets ke CSV dengan intelligent update untuk engagement metrics
    """
    if not tweets_data:
        print("⚠️ Tidak ada data untuk disimpan")
        return None, None
    
    new_df = pd.DataFrame(tweets_data)
    
    if os.path.exists(filename):
        # Baca existing data
        existing_df = pd.read_csv(filename, encoding='utf-8-sig')
        
        print(f"📊 Data sebelumnya: {len(existing_df)} tweets")
        print(f"📊 Data scraped: {len(new_df)} tweets")
        
        # Gunakan fungsi compare_and_update untuk intelligent merging
        combined_df = compare_and_update_tweet_data(existing_df, new_df)
        
        # Hitung statistik
        new_tweets = set(new_df['tweet_url']) - set(existing_df['tweet_url'])
        new_tweets_count = len(new_tweets)
        
        print(f"📊 Tweet baru (unique): {new_tweets_count} tweets")
        print(f"📊 Total setelah intelligent update: {len(combined_df)} tweets")
        
    else:
        combined_df = new_df
        new_tweets_count = len(new_df)
        print(f"📊 File baru dibuat dengan {len(combined_df)} tweets")
    
    # Simpan ke CSV
    combined_df.to_csv(filename, index=False, encoding='utf-8-sig')
    print(f"✅ Data tersimpan di {filename}")
    
    # Return latest tweet ID
    latest_tweet_id = get_latest_tweet_id(combined_df) if not combined_df.empty else None
    
    return new_tweets_count, latest_tweet_id

def get_latest_tweet_id(df):
    """Extract tweet ID from the most recent tweet URL"""
    if df.empty:
        return None
    try:
        # Sort by scraped_at untuk mendapatkan yang terbaru
        latest_row = df.sort_values('scraped_at', ascending=False).iloc[0]
        tweet_url = latest_row['tweet_url']
        if '/status/' in tweet_url:
            tweet_id = tweet_url.split('/status/')[-1].split('/')[0]
            return tweet_id
    except:
        pass
    return None

# ======== FUNGSI SCRAPING OTOMATIS (MODIFIED) ========
last_tweet_id = None

def get_next_query():
    """Rotate through different query variations untuk diversitas"""
    queries = CONFIG["query_variations"]
    current_idx = CONFIG["current_query_index"]
    query = queries[current_idx]
    
    # Update index untuk query berikutnya
    CONFIG["current_query_index"] = (current_idx + 1) % len(queries)
    return query

# ======== MODIFIED AUTOMATED SCRAPING FUNCTION ========
def automated_scraping_improved():
    """
    Fungsi scraping otomatis yang menggunakan intelligent update
    """
    global last_tweet_id
    
    # Gunakan query yang berbeda setiap run
    current_query = get_next_query()
    
    print(f"\n🚀 [{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] Memulai scraping otomatis...")
    print(f"🔍 Query: '{current_query}'")
    
    if last_tweet_id:
        print(f"💾 Last tweet ID: {last_tweet_id}")
    
    # Scraping
    tweets, sna_relations = scrape_twitter_search(
        query=current_query,
        max_tweets=CONFIG["max_tweets"],
        use_login=CONFIG["use_login"],
        email_or_username=CONFIG["email_or_username"],
        password=CONFIG["password"],
        actual_username=CONFIG["actual_username"],
        since_id=last_tweet_id
    )
    
    if tweets:
        # Gunakan fungsi save yang sudah diimprove
        new_tweets_count, latest_tweet_id = save_tweets_to_csv_improved(tweets, CONFIG["csv_filename"])
        
        # Simpan data SNA relations
        new_relations_count = save_sna_relations(sna_relations, CONFIG["sna_filename"])
        
        # Update last_tweet_id untuk scraping berikutnya
        if latest_tweet_id:
            last_tweet_id = latest_tweet_id
            
        if new_tweets_count and new_tweets_count > 0:
            print(f"✅ [{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] Scraping selesai:")
            print(f"   📝 {new_tweets_count} tweet baru")
            print(f"   🔗 {new_relations_count} relasi SNA baru")
        else:
            print(f"⚠️ [{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] Scraping selesai, tidak ada data baru ditemukan")
        
        # Monitor engagement changes
        monitor_engagement_changes(CONFIG["csv_filename"])
        
    else:
        print(f"⚠️ [{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] Scraping selesai, tidak ada data ditemukan")
    
    print(f"⏰ Scraping berikutnya dalam {CONFIG['interval_minutes']} menit dengan query berikutnya...")

# ======== MAIN (MODIFIED) ========
# ======== MODIFIED MAIN FUNCTION ========
def main():
    print("=" * 70)
    print("🤖 TWITTER AUTO SCRAPER - FULL OTOMATIS V2 + SNA + HASHTAG")
    print("=" * 70)
    print(f"📋 Konfigurasi:")
    print(f"   • Query rotasi: {len(CONFIG['query_variations'])} variasi")
    for i, q in enumerate(CONFIG['query_variations'], 1):
        print(f"     {i}. '{q}'")
    print(f"   • Max tweets per run: {CONFIG['max_tweets']}")
    print(f"   • Interval: {CONFIG['interval_minutes']} menit")
    print(f"   • Output files:")
    print(f"     - Tweets: {CONFIG['csv_filename']}")
    print(f"     - SNA Relations: {CONFIG['sna_filename']}")
    print(f"   • Login: {'Ya' if CONFIG['use_login'] else 'Tidak'}")
    print("=" * 70)
    print("🔗 Fitur SNA (Social Network Analysis) + HASHTAG:")
    print("   • Mention: @user1 menyebut @user2")
    print("   • Reply: @user1 membalas @user2")
    print("   • Retweet: @user1 me-retweet @user2")
    print("   • Self-mention: @user1 menyebut dirinya sendiri")
    print("   • Hashtag_use: @user1 menggunakan #hashtag")
    print("=" * 70)
    
    # Jalankan scraping pertama kali
    print("🔥 Menjalankan scraping pertama kali...")
    automated_scraping_improved()
    
    # Jadwalkan scraping otomatis
    schedule.every(CONFIG["interval_minutes"]).minutes.do(automated_scraping_improved)
    
    print(f"\n⏰ Scheduler aktif! Press Ctrl+C untuk berhenti.")
    print(f"💡 Tip: Setiap run akan menggunakan query yang berbeda untuk diversitas data")
    print(f"🔗 Data SNA akan tersimpan dalam format: source | target | relation")
    print(f"🏷️ Hashtag relations akan menunjukkan: user | hashtag | hashtag_use")
    
    try:
        while True:
            schedule.run_pending()
            time.sleep(30)  # Check setiap 30 detik
    except KeyboardInterrupt:
        print("\n\n🛑 Scraper dihentikan oleh user.")
        print(f"📁 Data tweets tersimpan di: {CONFIG['csv_filename']}")
        print(f"📁 Data SNA relations tersimpan di: {CONFIG['sna_filename']}")
        
        # Tampilkan ringkasan data SNA jika file ada
        if os.path.exists(CONFIG['sna_filename']):
            try:
                sna_df = pd.read_csv(CONFIG['sna_filename'], encoding='utf-8-sig')
                print(f"\n📊 Ringkasan Data SNA:")
                print(f"   • Total relasi: {len(sna_df)}")
                
                relation_counts = sna_df['relation'].value_counts()
                for relation_type, count in relation_counts.items():
                    print(f"   • {relation_type}: {count} relasi")
                
                unique_users = set(sna_df['source'].tolist() + sna_df['target'].tolist())
                print(f"   • Unique nodes (users + hashtags): {len(unique_users)}")
                
                # Analisis hashtag network
                analyze_hashtag_network(CONFIG['sna_filename'])
                
            except Exception as e:
                print(f"   ⚠️ Tidak dapat membaca ringkasan SNA: {e}")

if __name__ == "__main__":
    main()