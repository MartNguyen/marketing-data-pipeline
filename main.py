"""
Module: AHB Meta Ads - Stealth Resume (v8.1)
Standard: Rate-Limit Proof Engineering
Strategy: 10-day windows + Long Sleep on 403
"""

import dlt
import os
import logging
import time
from datetime import datetime
from facebook_business.api import FacebookAdsApi
from facebook_business.adobjects.adaccount import AdAccount
from facebook_business.exceptions import FacebookRequestError

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

@dlt.resource(name="meta_insights", write_disposition="append")
def fetch_meta_breakdowns(account_id, access_token, fields, start_date, end_date, breakdown):
    FacebookAdsApi.init(access_token=access_token)
    account = AdAccount(f'act_{account_id}')
    params = {
        'time_range': {'since': start_date, 'until': end_date},
        'level': 'ad',
        'time_increment': 1,
        'breakdowns': breakdown
    }
    
    # Logic Retry ngay trong generator để không chết pipeline
    max_retries = 3
    for attempt in range(max_retries):
        try:
            insights = account.get_insights(fields=fields, params=params)
            for entry in insights:
                data = dict(entry)
                data['account_id'] = account_id 
                for key in data.keys():
                    if 'date' in key and data[key]:
                        try: data[key] = datetime.strptime(data[key], '%Y-%m-%d')
                        except: continue
                yield data
            break # Thành công thì thoát vòng lặp retry
        except FacebookRequestError as e:
            if e.api_error_code() == 4: # Rate limit
                wait_time = 300 * (attempt + 1) # Nghỉ 5 phút tăng dần
                logger.warning(f"⚠️ Meta bóp rồi! Nghỉ {wait_time}s rồi thử lại...")
                time.sleep(wait_time)
            else:
                raise e

def run_resumed_backfill():
    # Credentials (giữ nguyên)
    os.environ["DESTINATION__BIGQUERY__CREDENTIALS__PROJECT_ID"] = os.environ.get("GCP_PROJECT_ID")
    os.environ["DESTINATION__BIGQUERY__CREDENTIALS__CLIENT_EMAIL"] = os.environ.get("GCP_CLIENT_EMAIL")
    os.environ["DESTINATION__BIGQUERY__CREDENTIALS__PRIVATE_KEY"] = os.environ.get("GCP_PRIVATE_KEY", "").replace("\\n", "\n")
    os.environ["DESTINATION__BIGQUERY__LOCATION"] = "asia-southeast1"

    pipeline = dlt.pipeline(destination="bigquery", dataset_name="fb_ads_ahb1_report_v2")
    token = os.environ.get("FB_ACCESS_TOKEN")
    acc_ids = [a.strip() for a in os.environ.get("FB_ACCOUNT_ID", "").split(",") if a.strip()]
    fields = ["account_id", "campaign_id", "campaign_name", "adset_id", "adset_name", "ad_id", "ad_name", "date_start", "spend", "impressions", "clicks"]

    # CHỈ CHẠY TỪ THÁNG 5 TRỞ ĐI - Chia nhỏ 10 ngày để Meta không sốc
    RESUME_WINDOWS = [
        {"start": "2025-05-01", "end": "2025-05-15"}, {"start": "2025-05-16", "end": "2025-05-31"},
        {"start": "2025-06-01", "end": "2025-06-30"}, {"start": "2025-07-01", "end": "2025-07-31"},
        {"start": "2025-08-01", "end": "2025-08-31"}, {"start": "2025-09-01", "end": "2025-09-30"},
        {"start": "2025-10-01", "end": "2025-10-31"}, {"start": "2025-11-01", "end": "2025-11-30"},
        {"start": "2025-12-01", "end": "2025-12-31"}, {"start": "2026-01-01", "end": "2026-03-25"}
    ]

    for window in RESUME_WINDOWS:
        logger.info(f"🚀 Stealth Syncing: {window['start']} to {window['end']}")
        for acc_id in acc_ids:
            # Chạy tuần tự Age/Gender rồi tới Region
            pipeline.run(fetch_meta_breakdowns(acc_id, token, fields, window['start'], window['end'], ['age', 'gender']), table_name="insights_age_gender")
            pipeline.run(fetch_meta_breakdowns(acc_id, token, fields, window['start'], window['end'], ['region']), table_name="insights_region")
            time.sleep(15) # Nghỉ 15s giữa các Acc

if __name__ == "__main__":
    run_resumed_backfill()
