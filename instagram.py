import pandas as pd
import re
import schedule
import time
import json
import requests
from datetime import datetime


class InstagramCSVCleaner:
    def __init__(self, dataset_id, token):
        """
        Initialize with Dataset ID and token
        """
        self.dataset_id = dataset_id
        self.token = token
        self.df = None
        self.cleaned_df = None
        self.scraped_at = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    
    def load_api(self):
        """Load data from Apify dataset"""
        try:
            api_url = f"https://api.apify.com/v2/datasets/{self.dataset_id}/items"
            print(f"🌐 Fetching data from dataset API: {api_url}")
            response = requests.get(api_url, params={"token": self.token})
            response.raise_for_status()
            data = response.json()

            self.df = pd.DataFrame(data)
            print(f"✅ Successfully fetched {self.df.shape[0]} rows")
            return True
        except Exception as e:
            print(f"❌ Error fetching API data: {e}")
            return False
    
    def extract_main_post_info(self):
        """Extract main post information"""
        main_columns = {
            'post_id': 'id',
            'caption': 'caption',
            'owner_username': 'ownerUsername', 
            'owner_fullname': 'ownerFullName',
            'owner_id': 'ownerId',
            'likes_count': 'likesCount',
            'comments_count': 'commentsCount',
            'reshare_count': 'reshareCount',
            'video_play_count': 'videoPlayCount',
            'timestamp': 'timestamp',
            'post_type': 'type',
            'product_type': 'productType',
            'is_sponsored': 'isSponsored',
            'short_code': 'shortCode',
            'url': 'url',
            'display_url': 'displayUrl',
            'video_url': 'videoUrl',
            'video_duration': 'videoDuration',
            'location_name': 'locationName',
            'width': 'dimensionsWidth',
            'height': 'dimensionsHeight'
        }
        
        main_data = {}
        for new_col, orig_col in main_columns.items():
            if orig_col in self.df.columns:
                main_data[new_col] = self.df[orig_col]
            else:
                main_data[new_col] = None
        
        return pd.DataFrame(main_data)
    
    def extract_hashtags(self):
        """Extract hashtags"""
        hashtags_list = []
        hashtag_columns = [col for col in self.df.columns if col.startswith('hashtags/')]
        
        for _, row in self.df.iterrows():
            tags = []
            for col in hashtag_columns:
                if pd.notna(row[col]) and str(row[col]).strip():
                    tags.append(str(row[col]).strip())

            caption = row.get('caption', '')
            if pd.notna(caption) and caption:
                found = re.findall(r'#(\w+)', str(caption))
                tags.extend(found)

            hashtags_list.append(json.dumps(list(set(tags))))
        
        return hashtags_list
    
    def extract_mentions(self):
        """Extract mentions"""
        mentions_list = []
        for _, row in self.df.iterrows():
            caption = row.get('caption', '')
            if pd.notna(caption) and caption:
                mentions = re.findall(r'@(\w+)', str(caption))
                mentions_list.append(json.dumps(list(set(mentions))))
            else:
                mentions_list.append(json.dumps([]))
        return mentions_list

    def clean_and_restructure(self):
        """Main cleaning function"""
        if self.df is None:
            print("❌ No data loaded. Run load_api() first.")
            return None
        
        print("🧹 Starting data cleaning...")
        cleaned_data = self.extract_main_post_info()
        cleaned_data['hashtags'] = self.extract_hashtags()
        cleaned_data['mentions'] = self.extract_mentions()
        
        if 'timestamp' in cleaned_data.columns:
            try:
                cleaned_data['formatted_date'] = pd.to_datetime(
                    cleaned_data['timestamp'], unit='s'
                ).dt.strftime('%Y-%m-%d %H:%M:%S')
            except:
                cleaned_data['formatted_date'] = cleaned_data['timestamp']
        
        cleaned_data['scraped_at'] = self.scraped_at
        self.cleaned_df = cleaned_data
        
        print("✅ Data cleaning completed!")
        return cleaned_data
    
    def create_sna_data(self):
        """Create SNA data"""
        if self.cleaned_df is None:
            print("❌ No cleaned data available.")
            return None
        
        sna_data = []
        for _, row in self.cleaned_df.iterrows():
            source = row.get('owner_username', '')
            caption = row.get('caption', '')
            post_url = row.get('url', '')
            timestamp = row.get('formatted_date', '')
            
            if pd.notna(caption) and caption and pd.notna(source) and source:
                caption_str = str(caption)
                
                mentions = re.findall(r'@(\w+)', caption_str)
                for mention in mentions:
                    if mention.lower() != source.lower():
                        sna_data.append({
                            'source': source,
                            'caption': caption_str[:500],
                            'target': mention,
                            'relation': 'mention',
                            'post_url': post_url,
                            'timestamp': timestamp,
                            'scraped_at': self.scraped_at
                        })
                
                hashtags = re.findall(r'#(\w+)', caption_str)
                for hashtag in hashtags:
                    sna_data.append({
                        'source': source,
                        'caption': caption_str[:500],
                        'target': f"#{hashtag}",
                        'relation': 'hashtag',
                        'post_url': post_url,
                        'timestamp': timestamp,
                        'scraped_at': self.scraped_at
                    })
                
                if not mentions and not hashtags:
                    sna_data.append({
                        'source': source,
                        'caption': caption_str[:500],
                        'target': 'PUBLIC',
                        'relation': 'post',
                        'post_url': post_url,
                        'timestamp': timestamp,
                        'scraped_at': self.scraped_at
                    })
        
        if sna_data:
            return pd.DataFrame(sna_data).drop_duplicates()
        else:
            return None
    
    def save_cleaned_data(self, output_file="instagram_data_cleaned.csv"):
        if self.cleaned_df is not None:
            self.cleaned_df.to_csv(output_file, index=False, encoding='utf-8')
            print(f"💾 Cleaned data saved: {output_file}")
            return output_file
    
    def save_sna_data(self, sna_df, output_file="instagram_sna_data.csv"):
        if sna_df is not None:
            sna_df.to_csv(output_file, index=False, encoding='utf-8')
            print(f"💾 SNA data saved: {output_file}")
            return output_file


# Main process
def main():
    DATASET_ID = "GVVBZaiBnMFDG2jmm"  # ganti dengan dataset ID kamu
    TOKEN = "apify_api_oSkEWFUgIXtbeZyqMcSQ8yIB5DzK4E19ji48"

    cleaner = InstagramCSVCleaner(DATASET_ID, TOKEN)
    
    if not cleaner.load_api():
        return
    
    cleaned_data = cleaner.clean_and_restructure()
    if cleaned_data is not None:
        cleaned_file = cleaner.save_cleaned_data()
        sna_data = cleaner.create_sna_data()
        sna_file = cleaner.save_sna_data(sna_data)


if __name__ == "__main__":
    main()
    schedule.every(1).hours.do(main)
    print("⏳ Scheduler aktif... CTRL+C untuk stop")
    while True:
        schedule.run_pending()
        time.sleep(30)