import pandas as pd
import re
from datetime import datetime
import requests

# ==============================
# SASTRAWI IMPORT & SETUP
# ==============================
try:
    from Sastrawi.Stemmer.StemmerFactory import StemmerFactory
    from Sastrawi.StopWordRemover.StopWordRemoverFactory import StopWordRemoverFactory
    
    # Initialize Sastrawi components
    stemmer_factory = StemmerFactory()
    stemmer = stemmer_factory.create_stemmer()
    
    stopword_factory = StopWordRemoverFactory()
    stopword_remover = stopword_factory.create_stop_word_remover()
    sastrawi_stopwords = stopword_factory.get_stop_words()
    
    SASTRAWI_AVAILABLE = True
    print("✅ Sastrawi successfully imported and initialized")
except ImportError:
    print("⚠️ Sastrawi not available. Install with: pip install Sastrawi")
    SASTRAWI_AVAILABLE = False
    stemmer = None
    stopword_remover = None
    sastrawi_stopwords = set()

# ==============================
# KONFIGURASI
# ==============================
CONFIG = {
    "text_preprocessing": {
        "enabled": True,
        "lowercase": True,
        "remove_urls": True,
        "remove_mentions": True,
        "remove_hashtags": False,
        "remove_extra_spaces": True,
        "remove_punctuation": True,
        "remove_numbers": False,
        "remove_stopwords": True,
        "min_word_length": 2,
        "remove_single_chars": True,
        "normalize_whitespace": True,
        "remove_duplicate_words": True,
        # Sastrawi specific options
        "use_sastrawi_stemming": True,
        "use_sastrawi_stopwords": True,
        "combine_stopwords": True,  # Gabungkan sastrawi + custom stopwords
        "custom_stopwords": [
            "rt","retweet","via","follow","followback","like","share",
            "wkwk","wkwkwk","haha","hehe","hihi","hoho",
            "tiktok","twitter","instagram","facebook","youtube",
            "yg", "nya", "dong", "sih", "nih", "deh", "lah", "kah",
            "banget", "bgt", "gak", "ga", "nggak", "engga"
        ]
    },
    "enable_logging": True
}

# ==============================
# HELPER FUNCTIONS
# ==============================
def log_activity(msg):
    if CONFIG["enable_logging"]:
        print(f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] {msg}")

def get_indonesian_stopwords():
    """Get Indonesian stopwords from multiple sources"""
    try:
        url = "https://raw.githubusercontent.com/rizqi-maulidi/UAS-Deep-Learning/main/kamusstopword.txt"
        resp = requests.get(url, timeout=10)
        resp.raise_for_status()
        online_stopwords = set(line.strip().lower() for line in resp.text.splitlines() if line.strip())
        log_activity(f"📥 Downloaded {len(online_stopwords)} online stopwords")
        return online_stopwords
    except Exception as e:
        log_activity(f"⚠️ Using default stopwords: {e}")
        return {"dan","yang","di","ke","dari","untuk","pada","dengan","sebagai","atau","juga","karena","ada","tidak","ini","itu","adalah","akan","atau","bisa","dapat","harus","jika","kalau","karena","ketika","maka","namun","oleh","sampai","sangat","satu","seperti","setiap","sudah","tanpa","telah","untuk","yang","yaitu"}

def get_combined_stopwords(config):
    """Combine all stopwords from different sources"""
    all_stopwords = set()
    
    # Add custom stopwords
    if config["custom_stopwords"]:
        all_stopwords.update(config["custom_stopwords"])
        log_activity(f"➕ Added {len(config['custom_stopwords'])} custom stopwords")
    
    # Add Sastrawi stopwords if available and enabled
    if config["use_sastrawi_stopwords"] and SASTRAWI_AVAILABLE:
        all_stopwords.update(sastrawi_stopwords)
        log_activity(f"➕ Added {len(sastrawi_stopwords)} Sastrawi stopwords")
    
    # Add online stopwords if combine_stopwords is True
    if config["combine_stopwords"]:
        online_stopwords = get_indonesian_stopwords()
        all_stopwords.update(online_stopwords)
        log_activity(f"➕ Added {len(online_stopwords)} online stopwords")
    
    log_activity(f"🔗 Total combined stopwords: {len(all_stopwords)}")
    return all_stopwords

def apply_sastrawi_stemming(text):
    """Apply Sastrawi stemming to text"""
    if not SASTRAWI_AVAILABLE or not text or pd.isna(text):
        return text
    
    try:
        # Stemming dengan Sastrawi
        stemmed = stemmer.stem(text)
        return stemmed
    except Exception as e:
        log_activity(f"⚠️ Stemming error: {e}")
        return text

def clean_text_advanced(text, config=None):
    """Advanced text preprocessing with Sastrawi integration"""
    if pd.isna(text) or text == '' or not isinstance(text, str):
        return ''

    if config is None:
        config = CONFIG["text_preprocessing"]

    if not config["enabled"]:
        return text

    # Step 1: Basic cleaning
    t = text.lower() if config["lowercase"] else text

    if config["remove_urls"]:
        t = re.sub(r'http\S+|www\.\S+', '', t)

    if config["remove_mentions"]:
        t = re.sub(r'@\w+', '', t)

    if config["remove_hashtags"]:
        t = re.sub(r'#\w+', '', t)

    if config["normalize_whitespace"]:
        t = re.sub(r'\s+', ' ', t).strip()

    if config["remove_punctuation"]:
        t = re.sub(r'[^\w\s#@]', ' ', t)

    if config["remove_numbers"]:
        t = re.sub(r'\d+', '', t)

    if config["remove_extra_spaces"]:
        t = re.sub(r'\s+', ' ', t).strip()

    if config["remove_single_chars"]:
        t = re.sub(r'\b\w\b', '', t)

    # Step 2: Word length filtering
    if config["min_word_length"] > 1:
        t = ' '.join([w for w in t.split() if len(w) >= config["min_word_length"]])

    # Step 3: Sastrawi Stemming (before stopword removal for better accuracy)
    if config["use_sastrawi_stemming"] and SASTRAWI_AVAILABLE:
        t = apply_sastrawi_stemming(t)

    # Step 4: Stopword removal with combined stopwords
    if config["remove_stopwords"]:
        combined_stopwords = get_combined_stopwords(config)
        words = t.split()
        filtered_words = [w for w in words if w.lower() not in combined_stopwords]
        t = ' '.join(filtered_words)

    # Step 5: Remove duplicate words
    if config["remove_duplicate_words"]:
        seen = set()
        unique = []
        for w in t.split():
            if w.lower() not in seen:  # Case-insensitive duplicate removal
                seen.add(w.lower())
                unique.append(w)
        t = ' '.join(unique)

    # Step 6: Final cleanup
    t = re.sub(r'\s+', ' ', t).strip()
    
    # Return cleaned text or mark as empty if too short
    return t if len(t.split()) >= 2 else '[cleaned_empty]'

def normalize_datetime_format(val):
    """Normalisasi format datetime menjadi YYYY-MM-DD HH:MM:SS"""
    if pd.isna(val) or val == '' or val == 'N/A':
        return datetime.now().strftime('%Y-%m-%d %H:%M:%S')

    if isinstance(val, (pd.Timestamp, datetime)):
        return val.strftime('%Y-%m-%d %H:%M:%S')

    parsed = pd.to_datetime(str(val).strip(), errors='coerce')
    if pd.isna(parsed):
        return datetime.now().strftime('%Y-%m-%d %H:%M:%S')

    return parsed.strftime('%Y-%m-%d %H:%M:%S')

def normalize_numeric_value(val):
    """Normalisasi nilai numerik dari format K, M, B, dll."""
    if pd.isna(val) or val == '' or val == 'N/A':
        return 0

    if isinstance(val, (int, float)):
        return int(val)

    s = str(val).strip().upper().replace(',', '').replace(' ', '')
    try:
        if 'K' in s:
            return int(float(s.replace('K', '')) * 1000)
        if 'M' in s:
            return int(float(s.replace('M', '')) * 1_000_000)
        if 'B' in s:
            return int(float(s.replace('B', '')) * 1_000_000_000)
        if 'JT' in s:
            return int(float(s.replace('JT', '')) * 1_000_000)
        if 'RB' in s:
            return int(float(s.replace('RB', '')) * 1000)
        return int(float(s.replace('.', ''))) if s.replace('.', '').isdigit() else int(float(s))
    except:
        return 0

def get_column(df, preferred, fallback_list):
    """Ambil kolom dengan prioritas, fallback kalau tidak ada"""
    if preferred in df.columns:
        return df[preferred]
    for fb in fallback_list:
        if fb in df.columns:
            return df[fb]
    return None

def preprocess_dataframe(df, text_column='content_text', normalize_metrics=True):
    """Preprocessing dataframe dengan advanced text cleaning dan normalisasi"""
    if df.empty:
        return df

    log_activity(f"🔄 Preprocessing {len(df)} records...")

    # Text preprocessing dengan Sastrawi
    if text_column in df.columns:
        df['content_original'] = df[text_column].copy()
        
        # Apply advanced text cleaning with Sastrawi
        log_activity("🧹 Applying advanced text preprocessing...")
        df[text_column] = df[text_column].apply(lambda x: clean_text_advanced(x))
        
        # Remove empty cleaned content
        initial_count = len(df)
        df = df[df[text_column] != '[cleaned_empty]'].copy()
        removed_count = initial_count - len(df)
        if removed_count > 0:
            log_activity(f"🗑️ Removed {removed_count} empty/invalid text records")

    # Datetime normalization
    datetime_cols = ['timestamp', 'scraped_at']
    for col in datetime_cols:
        if col in df.columns:
            df[col] = df[col].apply(normalize_datetime_format)

    # Numeric normalization
    if normalize_metrics:
        metric_cols = ['likes', 'shares', 'comments', 'views']
        for col in metric_cols:
            if col in df.columns:
                df[col] = df[col].apply(normalize_numeric_value)

    # Remove duplicates based on content
    if text_column in df.columns:
        initial_count = len(df)
        df = df.drop_duplicates(subset=[text_column], keep='last')
        removed_count = initial_count - len(df)
        if removed_count > 0:
            log_activity(f"🔗 Removed {removed_count} duplicate records")

    log_activity(f"✅ Preprocessing complete. {len(df)} records remaining.")
    return df

# ==============================
# PREPROCESSING STATISTICS
# ==============================
def show_preprocessing_stats(original_text, cleaned_text, sample_size=5):
    """Show preprocessing statistics and examples"""
    log_activity("📊 Preprocessing Statistics:")
    
    # Calculate basic stats
    original_words = [len(str(text).split()) for text in original_text if pd.notna(text)]
    cleaned_words = [len(str(text).split()) for text in cleaned_text if pd.notna(text) and text != '[cleaned_empty]']
    
    if original_words and cleaned_words:
        log_activity(f"   📝 Average words before: {sum(original_words)/len(original_words):.1f}")
        log_activity(f"   📝 Average words after: {sum(cleaned_words)/len(cleaned_words):.1f}")
        log_activity(f"   📉 Word reduction: {(1 - sum(cleaned_words)/sum(original_words))*100:.1f}%")
    
    # Show examples
    log_activity(f"\n🔍 Preprocessing Examples (showing {sample_size} samples):")
    valid_indices = [i for i, text in enumerate(cleaned_text) 
                    if pd.notna(text) and text != '[cleaned_empty]' and pd.notna(original_text.iloc[i])]
    
    for i, idx in enumerate(valid_indices[:sample_size]):
        log_activity(f"\n   Example {i+1}:")
        log_activity(f"   Before: {str(original_text.iloc[idx])[:100]}...")
        log_activity(f"   After:  {str(cleaned_text.iloc[idx])[:100]}...")

# ==============================
# 1. Load Data
# ==============================
log_activity("📁 Loading data files...")
log_activity(f"🔧 Sastrawi integration: {'Enabled' if SASTRAWI_AVAILABLE else 'Disabled'}")

files = {
    "twitter_sentimen": "/content/twitter_politik_indonesia_auto.csv",
    "twitter_sna": "/content/twitter_sna_relations.csv",
    "tiktok_sentimen": "/content/tiktok_politik_auto.csv",
    "tiktok_sna": "/content/tiktok_sna_relations.csv",
    "instagram_sentimen": "/content/instagram_data_cleaned.csv",
    "instagram_sna": "/content/instagram_sna_data.csv",
    "facebook_sentimen": "/content/facebook_politik_enhanced.csv",
    "facebook_sna": "/content/facebook_sna_relation.csv"
}

# ==============================
# 2. Twitter Processing
# ==============================
log_activity("🐦 Processing Twitter data...")

tw = pd.read_csv(files["twitter_sentimen"])
tw_sna = pd.read_csv(files["twitter_sna"])

# Join via tweet_url
tw_merged = tw_sna.merge(tw, on="tweet_url", how="left")

# Normalisasi ke format dashboard
twitter_sentimen = pd.DataFrame({
    "platform": "twitter",
    "author": tw["display_name"],
    "author_username": tw["username"],
    "content_text": tw["tweet_text"],
    "post_url": tw["tweet_url"],
    "timestamp": tw["timestamp"],
    "likes": tw["likes"],
    "shares": tw["retweets"],
    "comments": tw["replies"],
    "views": tw["views"],
    "hashtags": tw["hashtags"],
    "mentions": tw["mentions"],
    "scraped_at": tw["scraped_at"]
})

twitter_sna = pd.DataFrame({
    "platform": "twitter",
    "content_text": tw_merged["tweet_text"],
    "source": tw_merged["source"],
    "target": tw_merged["target"],
    "relation": tw_merged["relation"],
    "timestamp": tw_merged["timestamp_x"],
    "scraped_at": get_column(tw_merged, "scraped_at_x", ["scraped_at_y", "scraped_at"])
})

# Preprocessing Twitter data with Sastrawi
log_activity("🔤 Applying Sastrawi preprocessing to Twitter data...")
twitter_sentimen = preprocess_dataframe(twitter_sentimen)
twitter_sna = preprocess_dataframe(twitter_sna, normalize_metrics=False)

# ==============================
# 3. TikTok Processing
# ==============================
log_activity("📱 Processing TikTok data...")

tt = pd.read_csv(files["tiktok_sentimen"])
tt_sna = pd.read_csv(files["tiktok_sna"])

tt_merged = tt_sna.merge(tt, left_on="video_url", right_on="link", how="left")

tiktok_sentimen = pd.DataFrame({
    "platform": "tiktok",
    "author": tt["author"],
    "author_username": tt["author_username"],
    "content_text": tt["title"],
    "post_url": tt["link"],
    "timestamp": tt["timestamp"],
    "likes": tt["likes"],
    "shares": tt["shares"],
    "comments": tt["comments"],
    "views": tt["views"],
    "hashtags": tt["hashtags"],
    "mentions": tt["mentions_in_caption"],
    "scraped_at": tt["scraped_at"]
})

tiktok_sna = pd.DataFrame({
    "platform": "tiktok",
    "content_text": tt_merged["title"],
    "source": tt_merged["source"],
    "target": tt_merged["target"],
    "relation": tt_merged["relation"],
    "timestamp": tt_merged["timestamp_x"] if "timestamp_x" in tt_merged.columns else tt_merged["timestamp"],
    "scraped_at": get_column(tt_merged, "scraped_at_x", ["scraped_at_y", "scraped_at"])
})

# Preprocessing TikTok data with Sastrawi
log_activity("🔤 Applying Sastrawi preprocessing to TikTok data...")
tiktok_sentimen = preprocess_dataframe(tiktok_sentimen)
tiktok_sna = preprocess_dataframe(tiktok_sna, normalize_metrics=False)

# ==============================
# 4. Instagram Processing
# ==============================
log_activity("📷 Processing Instagram data...")

ig = pd.read_csv(files["instagram_sentimen"])
ig_sna = pd.read_csv(files["instagram_sna"])

# Samakan nama kolom scraped_at → extracted_at (biar konsisten)
if "scraped_at" in ig_sna.columns:
    ig_sna = ig_sna.rename(columns={"scraped_at": "extracted_at"})

ig_merged = ig_sna.merge(ig, left_on="post_url", right_on="url", how="left")

instagram_sentimen = pd.DataFrame({
    "platform": "instagram",
    "author": ig["owner_username"],
    "author_username": ig["owner_fullname"],
    "content_text": get_column(ig, "caption", ["text", "content"]),
    "post_url": ig["url"],
    "timestamp": get_column(ig, "formatted_date", ["timestamp"]),
    "likes": ig["likes_count"],
    "shares": ig["reshare_count"],
    "comments": ig["comments_count"],
    "views": ig["video_play_count"],
    "hashtags": ig["hashtags"],
    "mentions": ig["mentions"],
    "scraped_at": ig["scraped_at"]
})

instagram_sna = pd.DataFrame({
    "platform": "instagram",
    "content_text": get_column(ig_merged, "caption", ["caption_x","caption_y","text"]),
    "source": ig_merged["source"],
    "target": ig_merged["target"],
    "relation": ig_merged["relation"],
    "timestamp": ig_merged["timestamp_x"] if "timestamp_x" in ig_merged.columns else ig_merged["timestamp"],
    "scraped_at": get_column(ig_merged, "scraped_at_x", ["scraped_at", "extracted_at"])
})

# Preprocessing Instagram data with Sastrawi
log_activity("🔤 Applying Sastrawi preprocessing to Instagram data...")
instagram_sentimen = preprocess_dataframe(instagram_sentimen)
instagram_sna = preprocess_dataframe(instagram_sna, normalize_metrics=False)

# ==============================
# 5. Facebook Processing
# ==============================
log_activity("📘 Processing Facebook data...")

fb = pd.read_csv(files["facebook_sentimen"])
fb_sna = pd.read_csv(files["facebook_sna"])

fb_merged = fb_sna.merge(fb, left_on="post_url", right_on="facebookUrl", how="left")

fb_merged = fb_sna.merge(
    fb,
    left_on="post_url",
    right_on="facebookUrl" if "facebookUrl" in fb.columns else "url",
    how="left"
)

facebook_sentimen = pd.DataFrame({
    "platform": "facebook",
    "author": get_column(fb, "pageName", ["user"]),
    "author_username": get_column(fb, "user", ["pageName"]),
    "content_text": get_column(fb, "text", []),
    "post_url": get_column(fb, "facebookUrl", ["url", "topLevelUrl", "link"]),
    "timestamp": get_column(fb, "timestamp", ["time"]),
    "likes": get_column(fb, "likes", []),
    "shares": get_column(fb, "shares", []),
    "comments": get_column(fb, "comments", []),
    "views": get_column(fb, "viewsCount", []),
    "hashtags": get_column(fb, "hashtags", []),
    "mentions": get_column(fb, "mentions", []),
    "scraped_at": get_column(fb, "scraped_at", [])
})

facebook_sna = pd.DataFrame({
    "platform": "facebook",
    "content_text": get_column(fb_merged, "text", []),
    "source": fb_merged["source"],
    "target": fb_merged["target"],
    "relation": fb_merged["relation"],
    "timestamp": get_column(fb_merged, "timestamp_x", ["timestamp", "time"]),
    "scraped_at": get_column(fb_merged, "scraped_at_x", ["scraped_at_y","scraped_at"])
})

# Preprocessing Facebook data with Sastrawi
log_activity("🔤 Applying Sastrawi preprocessing to Facebook data...")
facebook_sentimen = preprocess_dataframe(facebook_sentimen)
facebook_sna = preprocess_dataframe(facebook_sna, normalize_metrics=False)

# ==============================
# 6. Gabung Semua Platform
# ==============================
log_activity("🔗 Combining all platforms...")

dashboardsentimen = pd.concat(
    [twitter_sentimen, tiktok_sentimen, instagram_sentimen, facebook_sentimen],
    ignore_index=True
)

# Final cleaning untuk data sentimen
dashboardsentimen = dashboardsentimen.dropna(subset=["content_text"])
dashboardsentimen = dashboardsentimen[dashboardsentimen["content_text"].str.strip() != ""]
dashboardsentimen = dashboardsentimen[dashboardsentimen["content_text"] != "[cleaned_empty]"]

dashboardsna = pd.concat(
    [twitter_sna, tiktok_sna, instagram_sna, facebook_sna],
    ignore_index=True
)

# Final cleaning untuk data SNA
dashboardsna = dashboardsna.dropna(subset=["source", "target", "relation"])
dashboardsna = dashboardsna[
    (dashboardsna["source"].str.strip() != "") &
    (dashboardsna["target"].str.strip() != "") &
    (dashboardsna["relation"].str.strip() != "")
]

# ==============================
# 7. Show Preprocessing Statistics
# ==============================
if len(dashboardsentimen) > 0 and 'content_original' in dashboardsentimen.columns:
    show_preprocessing_stats(
        dashboardsentimen['content_original'], 
        dashboardsentimen['content_text'], 
        sample_size=3
    )

# ==============================
# 8. Data Summary
# ==============================
log_activity("📊 Data Summary:")
log_activity(f"   Total Sentimen Records: {len(dashboardsentimen)}")
log_activity(f"   Total SNA Records: {len(dashboardsna)}")
log_activity(f"   Platform Distribution (Sentimen):")
for platform in dashboardsentimen['platform'].value_counts().items():
    log_activity(f"     - {platform[0]}: {platform[1]} records")

log_activity(f"\n🔧 Preprocessing Configuration:")
log_activity(f"   - Sastrawi Stemming: {'✅ Enabled' if CONFIG['text_preprocessing']['use_sastrawi_stemming'] and SASTRAWI_AVAILABLE else '❌ Disabled'}")
log_activity(f"   - Sastrawi Stopwords: {'✅ Enabled' if CONFIG['text_preprocessing']['use_sastrawi_stopwords'] and SASTRAWI_AVAILABLE else '❌ Disabled'}")
log_activity(f"   - Combined Stopwords: {'✅ Enabled' if CONFIG['text_preprocessing']['combine_stopwords'] else '❌ Disabled'}")

# ==============================
# 9. Simpan ke CSV
# ==============================
log_activity("💾 Saving processed files...")

dashboardsentimen.to_csv("/content/dashboardsentimen.csv", index=False)
dashboardsna.to_csv("/content/dashboardsna.csv", index=False)

# Save preprocessing report
if SASTRAWI_AVAILABLE:
    preprocessing_report = {
        'total_records_processed': len(dashboardsentimen),
        'sastrawi_enabled': True,
        'stemming_applied': CONFIG['text_preprocessing']['use_sastrawi_stemming'],
        'stopwords_combined': CONFIG['text_preprocessing']['combine_stopwords'],
        'platform_distribution': dict(dashboardsentimen['platform'].value_counts())
    }
    
    pd.DataFrame([preprocessing_report]).to_csv("/content/preprocessing_report.csv", index=False)
    log_activity("   📊 preprocessing_report.csv - Processing statistics")

log_activity("✅ Files successfully created:")
log_activity("   📄 dashboardsentimen.csv - Enhanced preprocessed sentiment data")
log_activity("   📄 dashboardsna.csv - Enhanced preprocessed SNA data")
log_activity("🎉 Enhanced processing with Sastrawi complete!")