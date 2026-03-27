import dlt
import os
import time
import logging
from datetime import datetime, date
from dateutil.relativedelta import relativedelta
from facebook_business.api import FacebookAdsApi
from facebook_business.adobjects.adaccount import AdAccount

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

@dlt.resource(name="facebook_insights", write_disposition="append")
def fetch_meta_backfill(account_id, access_token, fields, start_date, end_date):
    FacebookAdsApi.init(access_token=access_token)
    clean_acc_id = str(account_id).replace('act_', '')
    account = AdAccount(f'act_{clean_acc_id}')
    
    params = {
        'time_range': {'since': start_date, 'until': end_date},
        'level': 'ad',
        'time_increment': 1,
    }

    try:
        insights = account.get_insights(fields=fields, params=params)
        for entry in insights:
            data = dict(entry)
            data['account_id'] = clean_acc_id
            for key in data.keys():
                if 'date' in key and data[key]:
                    try: 
                        # FIX: Giữ nguyên datetime để khớp với cột TIMESTAMP của BigQuery
                        data[key] = datetime.strptime(data[key], '%Y-%m-%d')
                    except: continue
            yield data
    except Exception as e:
        logger.error(f"❌ Lỗi Acc {account_id} ({start_date}): {e}")

def run_backfill():
    # 1. Credentials & Config
    os.environ["DESTINATION__BIGQUERY__CREDENTIALS__PROJECT_ID"] = os.environ.get("GCP_PROJECT_ID")
    os.environ["DESTINATION__BIGQUERY__CREDENTIALS__CLIENT_EMAIL"] = os.environ.get("GCP_CLIENT_EMAIL")
    os.environ["DESTINATION__BIGQUERY__CREDENTIALS__PRIVATE_KEY"] = os.environ.get("GCP_PRIVATE_KEY", "").replace("\\n", "\n")
    
    # Ép dlt trỏ đúng Singapore
    os.environ["DESTINATION__BIGQUERY__LOCATION"] = "asia-southeast1" 

    pipeline = dlt.pipeline(
        pipeline_name="meta_backfill_v2", 
        destination="bigquery", 
        dataset_name="fb_ads_ahb1_report_v2"
    )
    
    token = os.environ.get("FB_ACCESS_TOKEN")
    acc_ids = [a.strip() for a in os.environ.get("FB_ACCOUNT_ID", "").split(",") if a.strip()]
    fields = ["account_id", "campaign_id", "campaign_name", "adset_id", "adset_name", "ad_id", "ad_name", "date_start", "spend", "impressions", "clicks", "inline_post_engagement", "actions"]

    # 2. Vòng lặp chia tháng (Jan 2025 -> Mar 2026)
    start_point = date(2025, 1, 1)
    end_point = date(2026, 3, 20)
    
    current_start = start_point
    while current_start < end_point:
        current_end = current_start + relativedelta(months=1) - relativedelta(days=1)
        if current_end > end_point: current_end = end_point
            
        str_start = current_start.strftime('%Y-%m-%d')
        str_end = current_end.strftime('%Y-%m-%d')
        
        logger.info(f"🚀 [BACKFILL] Đang cày: {str_start} đến {str_end}")

        for acc_id in acc_ids:
            pipeline.run(fetch_meta_backfill(acc_id, token, fields, str_start, str_end), table_name="facebook_insights")
            time.sleep(2) 
        
        current_start += relativedelta(months=1)
        time.sleep(5)

if __name__ == "__main__":
    run_backfill()
