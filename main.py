"""
Module: Meta Ads Daily Sync (v10.0)
Standard: Production Ready - 7 Days Rolling Window
Logic: Incremental Append + SQL View Deduplication
"""

import dlt
import os
import logging
import time
from datetime import datetime, timedelta
from facebook_business.api import FacebookAdsApi
from facebook_business.adobjects.adaccount import AdAccount

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

@dlt.resource(name="meta_insights", write_disposition="append")
def fetch_meta_daily(account_id, access_token, fields, start_date, end_date, breakdown=None):
    FacebookAdsApi.init(access_token=access_token)
    account = AdAccount(f'act_{account_id}')
    params = {
        'time_range': {'since': start_date, 'until': end_date},
        'level': 'ad',
        'time_increment': 1,
    }
    if breakdown:
        params['breakdowns'] = breakdown

    insights = account.get_insights(fields=fields, params=params)
    for entry in insights:
        data = dict(entry)
        data['account_id'] = account_id
        for key in data.keys():
            if 'date' in key and data[key]:
                try: data[key] = datetime.strptime(data[key], '%Y-%m-%d')
                except: continue
        yield data

def run_daily_pipeline():
    # 1. Config Credentials (Generic Mode để nhận Secret cũ)
    os.environ["DESTINATION__BIGQUERY__CREDENTIALS__PROJECT_ID"] = os.environ.get("GCP_PROJECT_ID")
    os.environ["DESTINATION__BIGQUERY__CREDENTIALS__CLIENT_EMAIL"] = os.environ.get("GCP_CLIENT_EMAIL")
    os.environ["DESTINATION__BIGQUERY__CREDENTIALS__PRIVATE_KEY"] = os.environ.get("GCP_PRIVATE_KEY", "").replace("\\n", "\n")
    os.environ["DESTINATION__BIGQUERY__LOCATION"] = "asia-southeast1"

    pipeline = dlt.pipeline(destination="bigquery", dataset_name="fb_ads_ahb1_report_v2")
    
    token = os.environ.get("FB_ACCESS_TOKEN")
    acc_ids = [a.strip() for a in os.environ.get("FB_ACCOUNT_ID", "").split(",") if a.strip()]
    fields = ["account_id", "campaign_id", "campaign_name", "adset_id", "adset_name", "ad_id", "ad_name", "date_start", "spend", "impressions", "clicks"]

    # 2. Window: 7 ngày gần nhất
    end_date = datetime.now().strftime('%Y-%m-%d')
    start_date = (datetime.now() - timedelta(days=7)).strftime('%Y-%m-%d')
    
    logger.info(f"☀️ Daily Syncing from {start_date} to {end_date}...")

    for acc_id in acc_ids:
        # A. Sync Master Data
        pipeline.run(fetch_meta_daily(acc_id, token, fields, start_date, end_date), table_name="facebook_insights")
        
        # B. Sync Age/Gender
        pipeline.run(fetch_meta_daily(acc_id, token, fields, start_date, end_date, ['age', 'gender']), table_name="insights_age_gender")
        
        # C. Sync Region
        pipeline.run(fetch_meta_daily(acc_id, token, fields, start_date, end_date, ['region']), table_name="insights_region")
        
        logger.info(f"✅ Acc {acc_id} daily sync done.")
        time.sleep(5)

if __name__ == "__main__":
    run_daily_pipeline()
