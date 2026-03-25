"""
Module: Meta Ads Backfill - Probing Run (Test 1 Month)
Strategy: Discrete Monthly - Age, Gender, Region
Fix: Rate Limit Recovery & Account ID Mapping
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

@dlt.resource(write_disposition="append")
def fetch_meta_breakdowns(account_id, access_token, fields, start_date, end_date, breakdown, table_name):
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
        data['account_id'] = account_id # ÉP ID ĐỂ KHÔNG BỊ NULL
        for key in data.keys():
            if 'date' in key and data[key]:
                try: data[key] = datetime.strptime(data[key], '%Y-%m-%d')
                except: continue
        yield data

def run_test_sync():
    os.environ["DESTINATION__BIGQUERY__LOCATION"] = "asia-southeast1"
    # Dùng v7 để xóa cache cũ
    pipeline = dlt.pipeline(pipeline_name="fb_ads_test_v7", destination="bigquery", dataset_name="fb_ads_ahb1_report_v2")

    token = os.environ.get("FB_ACCESS_TOKEN")
    acc_ids = [a.strip() for a in os.environ.get("FB_ACCOUNT_ID", "").split(",") if a.strip()]
    fields = ["account_id", "campaign_id", "campaign_name", "ad_id", "date_start", "spend", "impressions", "clicks"]

    # CHỈ TEST THÁNG 3/2026 ĐỂ THĂM DÒ
    month_test = {"start": "2026-03-01", "end": "2026-03-25"}

    logger.info(f"🚀 Probing Month: {month_test['start']}...")
    for acc_id in acc_ids:
        # Kéo cả 2 loại vào 2 bảng riêng
        pipeline.run(fetch_meta_breakdowns(acc_id, token, fields, month_test['start'], month_test['end'], ['age', 'gender'], "insights_age_gender"))
        pipeline.run(fetch_meta_breakdowns(acc_id, token, fields, month_test['start'], month_test['end'], ['region'], "insights_region"))
        
        logger.info(f"☕ Acc {acc_id} done. Waiting 5s...")
        time.sleep(5)

if __name__ == "__main__":
    run_test_sync()
