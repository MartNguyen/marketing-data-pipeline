"""
Module: Meta Ads Full Breakdown Backfill (Production)
Strategy: Discrete Monthly Chunking - Age, Gender, Region
Standard: Agency-Grade Handover Version
"""

import dlt
import os
import logging
from datetime import datetime
from facebook_business.api import FacebookAdsApi
from facebook_business.adobjects.adaccount import AdAccount

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def get_discrete_resource(account_id, access_token, fields, chunk, breakdown_type, table_name):
    """
    Tạo resource riêng cho từng loại breakdown để tối ưu hóa tốc độ Meta API.
    """
    @dlt.resource(name=table_name, write_disposition="append")
    def fetch_data():
        FacebookAdsApi.init(access_token=access_token)
        account = AdAccount(f'act_{account_id}')
        
        params = {
            'time_range': {'since': chunk['start'], 'until': chunk['end']},
            'level': 'ad',
            'time_increment': 1,
            'breakdowns': breakdown_type
        }
        
        insights = account.get_insights(fields=fields, params=params)
        for entry in insights:
            data = dict(entry)
            # ÉP KIỂU TIMESTAMP & FIX NULL ACCOUNT_ID
            data['account_id'] = account_id 
            for key in data.keys():
                if 'date' in key and data[key]:
                    try:
                        data[key] = datetime.strptime(data[key], '%Y-%m-%d')
                    except: continue
            yield data
    return fetch_data()

def run_full_breakdown_backfill():
    # 1. Credentials & Regional Configuration
    os.environ["DESTINATION__BIGQUERY__LOCATION"] = "asia-southeast1"
    os.environ["DESTINATION__BIGQUERY__CREDENTIALS__PROJECT_ID"] = os.environ.get("GCP_PROJECT_ID")
    os.environ["DESTINATION__BIGQUERY__CREDENTIALS__CLIENT_EMAIL"] = os.environ.get("GCP_CLIENT_EMAIL")
    os.environ["DESTINATION__BIGQUERY__CREDENTIALS__PRIVATE_KEY"] = os.environ.get("GCP_PRIVATE_KEY", "").replace("\\n", "\n")

    pipeline = dlt.pipeline(pipeline_name="fb_ads_full_v6", destination="bigquery", dataset_name="fb_ads_ahb1_report_v2")

    token = os.environ.get("FB_ACCESS_TOKEN")
    acc_ids = [a.strip() for a in os.environ.get("FB_ACCOUNT_ID", "").split(",") if a.strip()]
    fields = ["account_id", "campaign_id", "campaign_name", "ad_id", "date_start", "spend", "impressions", "clicks"]

    # CHUNKS: Chia nhỏ để né 3 tiếng
    MONTH_CHUNKS = [
        {"start": "2025-01-01", "end": "2025-06-30", "label": "H1_2025"},
        {"start": "2025-07-01", "end": "2025-12-31", "label": "H2_2025"},
        {"start": "2026-01-01", "end": "2026-03-25", "label": "YTD_2026"}
    ]

    for chunk in MONTH_CHUNKS:
        logger.info(f"🚀 Full Syncing {chunk['label']}...")
        all_resources = []
        for acc_id in acc_ids:
            # 1. Age & Gender Breakdown
            all_resources.append(get_discrete_resource(acc_id, token, fields, chunk, ['age', 'gender'], "insights_age_gender"))
            # 2. Region Breakdown
            all_resources.append(get_discrete_resource(acc_id, token, fields, chunk, ['region'], "insights_region"))

        try:
            info = pipeline.run(all_resources)
            logger.info(f"✅ Loaded {chunk['label']}: {info}")
        except Exception as e:
            logger.error(f"❌ Failed {chunk['label']}: {str(e)}")

if __name__ == "__main__":
    run_full_breakdown_backfill()
