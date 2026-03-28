"""
Module: Meta Ads Daily Sync (v11.0)
Standard: Production Ready - 7 Days Rolling Window
Feature: Video Metrics & Granular Engagement Support
"""

import dlt
import os
import logging
import time
from datetime import datetime, timedelta
from facebook_business.api import FacebookAdsApi
from facebook_business.adobjects.adaccount import AdAccount

# Setup Logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

@dlt.resource(name="meta_insights", write_disposition="append")
def fetch_meta_daily(account_id, access_token, fields, start_date, end_date, breakdown=None):
    FacebookAdsApi.init(access_token=access_token)
    # Tự động thêm act_ nếu ní chưa có
    clean_acc_id = str(account_id).replace('act_', '')
    account = AdAccount(f'act_{clean_acc_id}')
    
    params = {
        'time_range': {'since': start_date, 'until': end_date},
        'level': 'ad',
        'time_increment': 1,
    }
    if breakdown:
        params['breakdowns'] = breakdown

    try:
        insights = account.get_insights(fields=fields, params=params)
        for entry in insights:
            data = dict(entry)
            data['account_id'] = clean_acc_id
            
            # Chuẩn hóa định dạng ngày cho BigQuery
            for key in data.keys():
                if 'date' in key and data[key]:
                    try: 
                        data[key] = datetime.strptime(data[key], '%Y-%m-%d').date()
                    except: 
                        continue
            yield data
    except Exception as e:
        logger.error(f"❌ Lỗi khi kéo data từ Acc {account_id}: {e}")

def run_daily_pipeline():
    # 1. Config Credentials (GCP)
    os.environ["DESTINATION__BIGQUERY__CREDENTIALS__PROJECT_ID"] = os.environ.get("GCP_PROJECT_ID")
    os.environ["DESTINATION__BIGQUERY__CREDENTIALS__CLIENT_EMAIL"] = os.environ.get("GCP_CLIENT_EMAIL")
    os.environ["DESTINATION__BIGQUERY__CREDENTIALS__PRIVATE_KEY"] = os.environ.get("GCP_PRIVATE_KEY", "").replace("\\n", "\n")
    os.environ["DESTINATION__BIGQUERY__LOCATION"] = "asia-southeast1" # Khu vực Singapore

    pipeline = dlt.pipeline(
        pipeline_name="meta_to_bigquery",
        destination="bigquery", 
        dataset_name="fb_ads_ahb1_report_v2"
    )
    
    token = os.environ.get("FB_ACCESS_TOKEN")
    acc_ids = [a.strip() for a in os.environ.get("FB_ACCOUNT_ID", "").split(",") if a.strip()]
    
    # 🌟 FIELDS UPDATE: Thêm actions để lôi Video & Engagement chi tiết
    fields = [
        "account_id", "campaign_id", "campaign_name", 
        "adset_id", "adset_name", "ad_id", "ad_name", 
        "date_start", "spend", "impressions", "clicks",
        "inline_post_engagement", # Engagement cũ (số tổng)
        "actions" # Chứa: video_view (3s), thruplay, post_reaction, comment, post (share)
    ]

    # 2. Window: 7 ngày gần nhất để đảm bảo data không bị sót
    end_date = datetime.now().strftime('%Y-%m-%d')
    start_date = (datetime.now() - timedelta(days=7)).strftime('%Y-%m-%d')
    
    logger.info(f"☀️ Daily Syncing từ {start_date} đến {end_date}...")

    for acc_id in acc_ids:
        # A. Sync Master Data (Bao gồm cả Video & Engagement)
        info_master = pipeline.run(
            fetch_meta_daily(acc_id, token, fields, start_date, end_date), 
            table_name="facebook_insights"
        )
        logger.info(f"📊 Master Data: {info_master}")
        
        # B. Sync Age/Gender (Giữ nguyên để báo cáo nhân khẩu học)
        pipeline.run(
            fetch_meta_daily(acc_id, token, fields, start_date, end_date, ['age', 'gender']), 
            table_name="insights_age_gender"
        )
        
        # C. Sync Region (Giữ nguyên để báo cáo địa lý)
        pipeline.run(
            fetch_meta_daily(acc_id, token, fields, start_date, end_date, ['region']), 
            table_name="insights_region"
        )
        
        logger.info(f"✅ Acc {acc_id} daily sync hoàn tất.")
        time.sleep(5) # Tránh bị Meta rate limit

if __name__ == "__main__":
    run_daily_pipeline()
