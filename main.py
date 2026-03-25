"""
Module: Meta Ads Backfill - Canary Run (v7.3)
Fix: Schema Mismatch by adding mandatory adset_id
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

def run_canary_test():
    os.environ["DESTINATION__BIGQUERY__LOCATION"] = "asia-southeast1"
    os.environ["DESTINATION__BIGQUERY__CREDENTIALS__PROJECT_ID"] = os.environ.get("GCP_PROJECT_ID")
    os.environ["DESTINATION__BIGQUERY__CREDENTIALS__CLIENT_EMAIL"] = os.environ.get("GCP_CLIENT_EMAIL")
    os.environ["DESTINATION__BIGQUERY__CREDENTIALS__PRIVATE_KEY"] = os.environ.get("GCP_PRIVATE_KEY", "").replace("\\n", "\n")

    pipeline = dlt.pipeline(destination="bigquery", dataset_name="fb_ads_ahb1_report_v2")

    token = os.environ.get("FB_ACCESS_TOKEN")
    acc_ids = [a.strip() for a in os.environ.get("FB_ACCOUNT_ID", "").split(",") if a.strip()]
    
    # FIX: Thêm adset_id và adset_name để khớp Schema và hỗ trợ MS làm Looker
    fields = [
        "account_id", 
        "campaign_id", "campaign_name", 
        "adset_id", "adset_name", # <-- Cốt lõi ở đây
        "ad_id", "ad_name", 
        "date_start", "spend", "impressions", "clicks"
    ]

    month_test = {"start": "2025-05-01", "end": "2025-05-31"}
    logger.info(f"🚀 Probing May 2025 (v7.3 - Fixed Fields)...")
    
    for acc_id in acc_ids:
        # Age/Gender
        res_ag = fetch_meta_breakdowns(acc_id, token, fields, month_test['start'], month_test['end'], ['age', 'gender'])
        res_ag.table_name = "insights_age_gender"
        pipeline.run(res_ag)
        
        # Region
        res_re = fetch_meta_breakdowns(acc_id, token, fields, month_test['start'], month_test['end'], ['region'])
        res_re.table_name = "insights_region"
        pipeline.run(res_re)
        
        logger.info(f"☕ Acc {acc_id} done. Waiting 5s...")
        time.sleep(5)

if __name__ == "__main__":
    run_canary_test()
