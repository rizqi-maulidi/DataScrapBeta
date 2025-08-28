import pandas as pd
import re
import schedule
import time
import json
from datetime import datetime

class InstagramCSVCleaner:
    def __init__(self, csv_file):
        """
        Initialize with CSV file path
        """
        self.csv_file = csv_file
        self.df = None
        self.cleaned_df = None
        self.scraped_at = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    
    def load_csv(self):
        """Load CSV file with proper handling"""
        try:
            print(f"📂 Loading CSV file: {self.csv_file}")
            
            # Try different encodings
            encodings = ['utf-8', 'latin1', 'cp1252', 'iso-8859-1']
            
            for encoding in encodings:
                try:
                    self.df = pd.read_csv(self.csv_file, encoding=encoding)
                    print(f"✅ Successfully loaded with {encoding} encoding")
                    print(f"📊 Shape: {self.df.shape[0]} rows, {self.df.shape[1]} columns")
                    break
                except UnicodeDecodeError:
                    continue
            
            if self.df is None:
                raise Exception("Could not load CSV with any encoding")
                
            return True
            
        except Exception as e:
            print(f"❌ Error loading CSV: {e}")
            return False
    
    def extract_main_post_info(self):
        """Extract main post information"""
        print("🔄 Extracting main post information...")
        
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
        """Extract hashtags from caption and hashtags/ columns, return JSON list"""
        print("🏷️ Extracting hashtags...")

        hashtags_list = []
        hashtag_columns = [col for col in self.df.columns if col.startswith('hashtags/')]
        
        for index, row in self.df.iterrows():
            tags = []

            # 1. Ambil dari kolom khusus hashtags/
            for col in hashtag_columns:
                if pd.notna(row[col]) and str(row[col]).strip():
                    tags.append(str(row[col]).strip())

            # 2. Ambil dari caption (regex #tag)
            caption = row.get('caption', '')
            if pd.notna(caption) and caption:
                found = re.findall(r'#(\w+)', str(caption))
                tags.extend(found)

            # Simpan dalam format JSON list
            hashtags_list.append(json.dumps(list(set(tags))))  # remove duplicate
        
        return hashtags_list
    
    def extract_mentions(self):
        """Extract mentions (@username) from caption, save as JSON list"""
        print("📌 Extracting mentions...")

        mentions_list = []
        for index, row in self.df.iterrows():
            caption = row.get('caption', '')
            if pd.notna(caption) and caption:
                mentions = re.findall(r'@(\w+)', str(caption))
                mentions_list.append(json.dumps(list(set(mentions))))  # unique
            else:
                mentions_list.append(json.dumps([]))
        
        return mentions_list

    def clean_and_restructure(self):
        """Main cleaning function"""
        if self.df is None:
            print("❌ No data loaded. Run load_csv() first.")
            return None
        
        print("🧹 Starting data cleaning and restructuring...")
        
        # Extract main post info
        cleaned_data = self.extract_main_post_info()
        
        # Add hashtags JSON
        cleaned_data['hashtags'] = self.extract_hashtags()

        # Add mentions JSON
        cleaned_data['mentions'] = self.extract_mentions()
        
        # Convert timestamp to readable format
        if 'timestamp' in cleaned_data.columns:
            try:
                cleaned_data['formatted_date'] = pd.to_datetime(
                    cleaned_data['timestamp'], unit='s'
                ).dt.strftime('%Y-%m-%d %H:%M:%S')
            except:
                cleaned_data['formatted_date'] = cleaned_data['timestamp']
        
        # Add scraped_at timestamp
        cleaned_data['scraped_at'] = self.scraped_at
        
        self.cleaned_df = cleaned_data
        
        print("✅ Data cleaning completed!")
        print(f"📊 Cleaned data shape: {cleaned_data.shape}")
        
        return cleaned_data
    
    def create_sna_data(self):
        """Create SNA (Social Network Analysis) format data"""
        if self.cleaned_df is None:
            print("❌ No cleaned data available. Run clean_and_restructure() first.")
            return None
        
        print("🔗 Creating SNA data...")
        
        sna_data = []
        
        for index, row in self.cleaned_df.iterrows():
            source = row.get('owner_username', '')
            caption = row.get('caption', '')
            post_url = row.get('url', '')
            timestamp = row.get('formatted_date', '')
            
            if pd.notna(caption) and caption and pd.notna(source) and source:
                caption_str = str(caption)
                
                # 1. Extract mentions (@username)
                mentions = re.findall(r'@(\w+)', caption_str)
                for mention in mentions:
                    if mention.lower() != source.lower():
                        sna_record = {
                            'source': source,
                            'caption': caption_str[:500],
                            'target': mention,
                            'relation': 'mention',
                            'post_url': post_url,
                            'timestamp': timestamp,
                            'scraped_at': self.scraped_at
                        }
                        sna_data.append(sna_record)
                
                # 2. Extract hashtags (#hashtag)
                hashtags = re.findall(r'#(\w+)', caption_str)
                for hashtag in hashtags:
                    sna_record = {
                        'source': source,
                        'caption': caption_str[:500],
                        'target': f"#{hashtag}",
                        'relation': 'hashtag',
                        'post_url': post_url,
                        'timestamp': timestamp,
                        'scraped_at': self.scraped_at
                    }
                    sna_data.append(sna_record)
                
                # 3. Extract entities
                entities = [
                    'DPR', 'MPR', 'KPK', 'Polri', 'TNI', 'Prabowo', 'Jokowi', 'Gibran', 
                    'Indonesia', 'Jakarta', 'Surabaya', 'Bandung', 'Medan',
                    'Gerindra', 'PDIP', 'PKS', 'Demokrat', 'Golkar', 'Nasdem', 'PKB'
                ]
                
                caption_upper = caption_str.upper()
                for entity in entities:
                    if entity.upper() in caption_upper and entity.lower() != source.lower():
                        entity_pattern = r'\b' + re.escape(entity) + r'\b'
                        if re.search(entity_pattern, caption_str, re.IGNORECASE):
                            sna_record = {
                                'source': source,
                                'caption': caption_str[:500],
                                'target': entity,
                                'relation': 'entity_mention',
                                'post_url': post_url,
                                'timestamp': timestamp,
                                'scraped_at': self.scraped_at
                            }
                            sna_data.append(sna_record)
                
                # 4. General post record
                if not mentions and not hashtags:
                    sna_record = {
                        'source': source,
                        'caption': caption_str[:500],
                        'target': 'PUBLIC',
                        'relation': 'post',
                        'post_url': post_url,
                        'timestamp': timestamp,
                        'scraped_at': self.scraped_at
                    }
                    sna_data.append(sna_record)
        
        if sna_data:
            sna_df = pd.DataFrame(sna_data)
            sna_df = sna_df.drop_duplicates(subset=['source', 'target', 'relation', 'post_url'])
            print(f"✅ SNA data created with {len(sna_df)} relationships")
            return sna_df
        else:
            print("⚠️ No relationships found for SNA")
            return None
    
    def save_cleaned_data(self, output_file=None):
        """Save cleaned data to CSV"""
        if self.cleaned_df is None:
            print("❌ No cleaned data available. Run clean_and_restructure() first.")
            return None
        
        if output_file is None:
            output_file = "instagram_data_cleaned.csv"
        
        try:
            self.cleaned_df.to_csv(output_file, index=False, encoding='utf-8')
            print(f"💾 Cleaned data saved to: {output_file}")
            return output_file
        except Exception as e:
            print(f"❌ Error saving file: {e}")
            return None
    
    def save_sna_data(self, sna_df, output_file=None):
        """Save SNA data to CSV"""
        if sna_df is None:
            print("❌ No SNA data available.")
            return None
        
        if output_file is None:
            output_file = "instagram_sna_data.csv"
        
        try:
            sna_df.to_csv(output_file, index=False, encoding='utf-8')
            print(f"💾 SNA data saved to: {output_file}")
            return output_file
        except Exception as e:
            print(f"❌ Error saving SNA file: {e}")
            return None
    
    def show_summary(self):
        """Show summary of the data"""
        if self.cleaned_df is None:
            print("❌ No cleaned data available.")
            return
        
        print("\n" + "="*50)
        print("📋 DATA SUMMARY")
        print("="*50)
        
        print(f"Total posts: {len(self.cleaned_df)}")
        print(f"Scraped at: {self.scraped_at}")
        
        if 'post_type' in self.cleaned_df.columns:
            print(f"\n📱 Post Types:")
            type_counts = self.cleaned_df['post_type'].value_counts()
            for post_type, count in type_counts.items():
                if pd.notna(post_type):
                    print(f"  - {post_type}: {count}")
        
        if 'owner_username' in self.cleaned_df.columns:
            print(f"\n👤 Top Users:")
            user_counts = self.cleaned_df['owner_username'].value_counts().head(5)
            for user, count in user_counts.items():
                if pd.notna(user):
                    print(f"  - @{user}: {count} posts")
        
        if 'formatted_date' in self.cleaned_df.columns:
            valid_dates = self.cleaned_df['formatted_date'].dropna()
            if len(valid_dates) > 0:
                print(f"\n📅 Date Range:")
                print(f"  - From: {valid_dates.min()}")
                print(f"  - To: {valid_dates.max()}")

# Main process
def main():
    csv_file = "apify_data.csv"
    
    print("🚀 Instagram CSV Cleaner with SNA Output")
    print("="*50)
    
    cleaner = InstagramCSVCleaner(csv_file)
    
    if not cleaner.load_csv():
        print("❌ Failed to load CSV file")
        return
    
    cleaned_data = cleaner.clean_and_restructure()
    
    if cleaned_data is not None:
        cleaned_file = cleaner.save_cleaned_data()
        sna_data = cleaner.create_sna_data()
        sna_file = cleaner.save_sna_data(sna_data)
        
        cleaner.show_summary()
        
        print(f"\n👀 Preview of cleaned data:")
        print(cleaned_data.head())
        
        if sna_data is not None:
            print(f"\n🔗 Preview of SNA data:")
            print(sna_data.head())
        
        print(f"\n📁 Files created:")
        print(f"  - Cleaned data: {cleaned_file}")
        if sna_file:
            print(f"  - SNA data: {sna_file}")

if __name__ == "__main__":
    # Jalankan sekali di awal
    main()
    
    # Jadwalkan auto-update tiap 1 jam
    schedule.every(1).hours.do(main)
    
    print("\n⏳ Scheduler aktif... menunggu jadwal berikutnya.")
    while True:
        schedule.run_pending()
        time.sleep(30)
