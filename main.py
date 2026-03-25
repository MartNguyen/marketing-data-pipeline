"""
Module: AHB Meta Ads Final Handover (v8.0)
Standard: Senior Tech Lead Production
Strategy: Anonymous Pipeline + Sequential Monthly Extraction
"""

import dlt
import os
import logging
import time
from datetime import datetime
from facebook_business.api import FacebookAdsApi
from facebook_business.adobjects.adaccount import AdAccount

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
    insights = account.get_insights(fields=fields, params=params)
    for entry in insights:
        data = dict(entry)
        data['account_id'] = account_id 
        for key in data.keys():
            if 'date' in key and data[key]:
                try: data[key] = datetime.strptime(data[key], '%Y-%m-%d')
                except: continue
        yield data

def run_master_final_backfill():
    # 1. ÉP MAPPING SECRETS (Bắt buộc để quay về mode cũ)
    os.environ["DESTINATION__BIGQUERY__CREDENTIALS__PROJECT_ID"] = os.environ.get("GCP_PROJECT_ID")
    os.environ["DESTINATION__BIGQUERY__CREDENTIALS__CLIENT_EMAIL"] = os.environ.get("GCP_CLIENT_EMAIL")
    os.environ["DESTINATION__BIGQUERY__CREDENTIALS__PRIVATE_KEY"] = os.environ.get("GCP_PRIVATE_KEY", "").replace("\\n", "\n")
    os.environ["DESTINATION__BIGQUERY__LOCATION"] = "asia-southeast1"

    # 2. KHÔNG ĐẶT TÊN PIPELINE (Để nó tự nhận diện Secrets ở trên)
    pipeline = dlt.pipeline(destination="bigquery", dataset_name="fb_ads_ahb1_report_v2")

    token = os.environ.get("FB_ACCESS_TOKEN")
    acc_ids = [a.strip() for a in os.environ.get("FB_ACCOUNT_ID", "").split(",") if a.strip()]
    
    fields = [
        "account_id", "campaign_id", "campaign_name", 
        "adset_id", "adset_name", "ad_id", "ad_name", 
        "date_start", "spend", "impressions", "clicks"
    ]

    # FULL LIST 15 THÁNG (Đã tối ưu để né 403)
    FULL_MONTHS = [
        {"start": "2025-01-01", "end": "2025-01-31"}, {"start": "2025-02-01", "end": "2025-02-28"},
        {"start": "2025-03-01", "end": "2025-03-31"}, {"start": "2025-04-01", "end": "2025-04-30"},
        {"start": "2025-05-01", "end": "2025-05-31"}, {"start": "2025-06-01", "end": "2025-06-30"},
        {"start": "2025-07-01", "end": "2025-07-31"}, {"start": "2025-08-01", "end": "2025-08-31"},
        {"start": "2025-09-01", "end": "2025-09-30"}, {"start": "2025-10-01", "end": "2025-10-31"},
        {"start": "2025-11-01", "end": "2025-11-30"}, {"start": "2025-12-01", "end": "2025-12-31"},
        {"start": "2026-01-01", "end": "2026-01-31"}, {"start": "2026-02-01", "end": "2026-02-28"},
        {"start": "2026-03-01", "end": "2026-03-25"}
    ]

    for month in FULL_MONTHS:
        logger.info(f"🚀 Master Syncing: {month['start']}...")
        for acc_id in acc_ids:
            # Age/Gender
            pipeline.run(fetch_meta_breakdowns(acc_id, token, fields, month['start'], month['end'], ['age', 'gender']), table_name="insights_age_gender")
            # Region
            pipeline.run(fetch_meta_breakdowns(acc_id, token, fields, month['start'], month['end'], ['region']), table_name="insights_region")
            
            logger.info(f"✅ Acc {acc_id} for {month['start']} LOADED. Sleeping 10s...")
            time.sleep(10)

if __name__ == "__main__":
    run_master_final_backfill()
