import requests
import pandas as pd
import time
import re
import schedule
from datetime import datetime

# Konfigurasi API
DATASET_ID = "eOBtnoGJVkz24qlTr"
TOKEN = "apify_api_oSkEWFUgIXtbeZyqMcSQ8yIB5DzK4E19ji48"
BASE_URL = f"https://api.apify.com/v2/datasets/{DATASET_ID}/items"

# Parameter pagination
LIMIT = 1000

def extract_mentions(text):
    if pd.isna(text):
        return ""
    mentions = re.findall(r'@\w+', str(text))
    return ", ".join(mentions) if mentions else ""

def extract_hashtags(text):
    if pd.isna(text):
        return ""
    hashtags = re.findall(r'#\w+', str(text))
    return ", ".join(hashtags) if hashtags else ""

def run_job():
    print("🚀 Mulai ambil data:", datetime.now().strftime("%Y-%m-%d %H:%M:%S"))
    
    OFFSET = 0
    all_data = []
    
    while True:
        params = {
            "token": TOKEN,
            "limit": LIMIT,
            "offset": OFFSET
        }
        
        response = requests.get(BASE_URL, params=params)
        if response.status_code != 200:
            print("Gagal ambil data:", response.status_code)
            break
        
        data = response.json()
        if not data:
            break
        
        all_data.extend(data)
        OFFSET += LIMIT
        time.sleep(1)
    
    df = pd.DataFrame(all_data)

    # Tambahkan kolom mention & hashtag
    df["mentions"] = df["text"].apply(extract_mentions)
    df["hashtags"] = df["text"].apply(extract_hashtags)

    # Tambahkan kolom scraped_at
    scraped_at = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    df["scraped_at"] = scraped_at

    # Simpan file utama
    output_file_main = "facebook_politik_enhanced.csv"
    df.to_csv(output_file_main, index=False, encoding="utf-8-sig")
    print(f"✅ File utama disimpan: {output_file_main}")

    # Buat file SNA
    sna_records = []
    for _, row in df.iterrows():
        if row["mentions"]:
            for mention in row["mentions"].split(", "):
                sna_records.append({
                    "source": row.get("pageName", row.get("user", "")),
                    "text": row.get("text", ""),
                    "target": mention,
                    "relation": "mention",
                    "post_url": row.get("url", row.get("topLevelUrl", "")),
                    "timestamp": row.get("timestamp", ""),
                    "scraped_at": scraped_at
                })
    
    df_sna = pd.DataFrame(sna_records)
    output_file_sna = "facebook_sna_relation.csv"
    df_sna.to_csv(output_file_sna, index=False, encoding="utf-8-sig")
    print(f"✅ File SNA disimpan: {output_file_sna}")
    print("⏳ Job selesai.\n")

# --- Jalankan sekali langsung saat start ---
run_job()

# --- Scheduler setiap 2 jam ---
schedule.every(2).hours.do(run_job)

print("🔄 Scheduler aktif. Script akan jalan setiap 2 jam sekali.\n")

# Jalankan terus
while True:
    schedule.run_pending()
    time.sleep(60)  # cek setiap 1 menit
