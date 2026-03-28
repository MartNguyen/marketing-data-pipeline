"""
Module: Meta Ads Daily Data Pipeline (v11.0)
Standard: Production Ready
Logic: Rolling 7-day window | Multi-table ingestion (Master, Age/Gender, Region)
"""

import dlt
import os
import logging
import time
from datetime import datetime, timedelta
from facebook_business.api import FacebookAdsApi
from facebook_business.adobjects.adaccount import AdAccount

# Cấu hình Logging chuyên nghiệp
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

@dlt.resource(name="facebook_insights", write_disposition="append")
def fetch_meta_daily(account_id, access_token, fields, start_date, end_date, breakdown=None):
    """Fetch dữ liệu từ Meta Ads API và chuẩn hóa định dạng cho BigQuery."""
    FacebookAdsApi.init(access_token=access_token)
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
            
            # Ép kiểu ISO Timestamp để tránh lỗi 400 tại BigQuery
            for key in data.keys():
                if 'date' in key and data[key]:
                    try:
                        dt_obj = datetime.strptime(data[key], '%Y-%m-%d')
                        data[key] = dt_obj.strftime('%Y-%m-%d %H:%M:%S')
                    except: continue
            
            # Đảm bảo mảng actions luôn tồn tại để tránh lỗi Schema bảng con
            if 'actions' not in data: data['actions'] = []
            yield data
    except Exception as e:
        logger.error(f"Account {account_id} | Range {start_date} - {end_date} | Error: {e}")

def run_meta_sync():
    """Khởi tạo và thực thi Pipeline nạp dữ liệu vào BigQuery."""
    # Thiết lập biến môi trường GCP (Singapore Region)
    os.environ["DESTINATION__BIGQUERY__CREDENTIALS__PROJECT_ID"] = os.environ.get("GCP_PROJECT_ID")
    os.environ["DESTINATION__BIGQUERY__CREDENTIALS__CLIENT_EMAIL"] = os.environ.get("GCP_CLIENT_EMAIL")
    os.environ["DESTINATION__BIGQUERY__CREDENTIALS__PRIVATE_KEY"] = os.environ.get("GCP_PRIVATE_KEY", "").replace("\\n", "\n")
    os.environ["DESTINATION__BIGQUERY__LOCATION"] = "asia-southeast1"

    pipeline = dlt.pipeline(
        pipeline_name="meta_daily_sync_v11",
        destination="bigquery",
        dataset_name="fb_ads_ahb1_report_v2"
    )
    
    token = os.environ.get("FB_ACCESS_TOKEN")
    acc_ids = [a.strip() for a in os.environ.get("FB_ACCOUNT_ID", "").split(",") if a.strip()]
    
    # Danh mục Fields (Master) & Fields (Breakdown)
    fields_master = ["account_id", "campaign_id", "campaign_name", "adset_id", "adset_name", "ad_id", "ad_name", "date_start", "spend", "impressions", "clicks", "inline_post_engagement", "actions"]
    fields_breakdown = ["account_id", "ad_id", "date_start", "spend", "impressions", "clicks"]

    # Xác định chu kỳ Rolling 7 ngày
    end_date = datetime.now().strftime('%Y-%m-%d')
    start_date = (datetime.now() - timedelta(days=7)).strftime('%Y-%m-%d')
    
    logger.info(f"🚀 Khởi động Daily Sync: {start_date} -> {end_date}")

    for acc_id in acc_ids:
        # 1. Nạp Master Data (Bảng chính + Actions)
        pipeline.run(fetch_meta_daily(acc_id, token, fields_master, start_date, end_date), table_name="facebook_insights")
        
        # 2. Nạp Age/Gender Breakdown
        pipeline.run(fetch_meta_daily(acc_id, token, fields_breakdown, start_date, end_date, ['age', 'gender']), table_name="insights_age_gender")
        
        # 3. Nạp Region Breakdown
        pipeline.run(fetch_meta_daily(acc_id, token, fields_breakdown, start_date, end_date, ['region']), table_name="insights_region")
        
        logger.info(f"✅ Đồng bộ hoàn tất Account: {acc_id}")
        time.sleep(5) # Tránh Rate Limit từ Meta API

if __name__ == "__main__":
    run_meta_sync()
