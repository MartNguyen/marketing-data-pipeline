"""
Module: Meta Ads Backfill - Anonymous Probing (May 2025)
Standard: Reverting to Default Discovery Logic
Fix: Restoring Secrets recognition by removing pipeline_name
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
    # 1. ÉP MAPPING: Trả về đúng tên biến mà dlt mong đợi (Generic Mode)
    os.environ["DESTINATION__BIGQUERY__CREDENTIALS__PROJECT_ID"] = os.environ.get("GCP_PROJECT_ID")
    os.environ["DESTINATION__BIGQUERY__CREDENTIALS__CLIENT_EMAIL"] = os.environ.get("GCP_CLIENT_EMAIL")
    os.environ["DESTINATION__BIGQUERY__CREDENTIALS__PRIVATE_KEY"] = os.environ.get("GCP_PRIVATE_KEY", "").replace("\\n", "\n")
    os.environ["DESTINATION__BIGQUERY__LOCATION"] = "asia-southeast1"

    # 2. KHÔNG ĐẶT TÊN: Để pipeline tự nhận diện theo script name (giống như trước đây)
    pipeline = dlt.pipeline(
        destination="bigquery", 
        dataset_name="fb_ads_ahb1_report_v2"
    )

    token = os.environ.get("FB_ACCESS_TOKEN")
    acc_ids = [a.strip() for a in os.environ.get("FB_ACCOUNT_ID", "").split(",") if a.strip()]
    fields = ["account_id", "campaign_id", "campaign_name", "ad_id", "date_start", "spend", "impressions", "clicks"]

    # Chạy thử tháng 5/2025 để check credentials
    month_test = {"start": "2025-05-01", "end": "2025-05-31"}
    logger.info(f"🚀 Reverting to Default Pipeline - Testing May 2025...")

    for acc_id in acc_ids:
        # Test 1: Age/Gender
        res_ag = fetch_meta_breakdowns(acc_id, token, fields, month_test['start'], month_test['end'], ['age', 'gender'])
        res_ag.table_name = "insights_age_gender"
        pipeline.run(res_ag)
        
        # Test 2: Region
        res_re = fetch_meta_breakdowns(acc_id, token, fields, month_test['start'], month_test['end'], ['region'])
        res_re.table_name = "insights_region"
        pipeline.run(res_re)
        
        logger.info(f"☕ Acc {acc_id} finished. Waiting 5s...")
        time.sleep(5)

if __name__ == "__main__":
    run_canary_test()
