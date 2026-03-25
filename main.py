"""
Module: Meta Ads Discrete Monthly Backfill
Strategy: Direct SDK Resource Wrapper (Non-redundant)
Standard: Agency-Grade Professional Workspace
"""

import dlt
import os
import logging
from facebook_business.api import FacebookAdsApi
from facebook_business.adobjects.adaccount import AdAccount

# Professional Logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

@dlt.resource(write_disposition="append")
def fetch_meta_discrete_chunks(account_id, access_token, fields, start_date, end_date):
    """
    Custom resource to fetch data for a specific, discrete time range.
    Bypasses high-level library limitations to ensure no data overlap.
    """
    FacebookAdsApi.init(access_token=access_token)
    account = AdAccount(f'act_{account_id}')
    
    params = {
        'time_range': {'since': start_date, 'until': end_date},
        'level': 'ad',
        'time_increment': 1, # Daily breakdown
    }
    
    # Requesting insights from Meta SDK
    insights = account.get_insights(fields=fields, params=params)
    for entry in insights:
        yield dict(entry)

def run_discrete_backfill():
    # Credentials Configuration
    os.environ["DESTINATION__BIGQUERY__CREDENTIALS__PROJECT_ID"] = os.environ.get("GCP_PROJECT_ID")
    os.environ["DESTINATION__BIGQUERY__CREDENTIALS__CLIENT_EMAIL"] = os.environ.get("GCP_CLIENT_EMAIL")
    os.environ["DESTINATION__BIGQUERY__CREDENTIALS__PRIVATE_KEY"] = os.environ.get("GCP_PRIVATE_KEY", "").replace("\\n", "\n")

    pipeline = dlt.pipeline(
        pipeline_name="fb_ads_discrete_backfill",
        destination="bigquery",
        dataset_name="fb_ads_ahb1_report_v2"
    )

    token = os.environ.get("FB_ACCESS_TOKEN")
    acc_ids = [a.strip() for a in os.environ.get("FB_ACCOUNT_ID", "").split(",") if a.strip()]
    
    # Standard Agency Reporting Fields
    reporting_fields = [
        "account_id", "campaign_id", "campaign_name", 
        "adset_id", "adset_name", "ad_id", "ad_name", 
        "date_start", "spend", "impressions", "clicks", 
        "reach", "frequency"
    ]

    # DISCRETE CHUNKS (No Overlap)
    MONTH_CHUNKS = [
        {"start": "2025-01-01", "end": "2025-03-31", "label": "Q1_2025"},
        {"start": "2025-04-01", "end": "2025-06-30", "label": "Q2_2025"},
        {"start": "2025-07-01", "end": "2025-09-30", "label": "Q3_2025"},
        {"start": "2025-10-01", "end": "2025-12-31", "label": "Q4_2025"},
        {"start": "2026-01-01", "end": "2026-03-25", "label": "YTD_2026"}
    ]

    for chunk in MONTH_CHUNKS:
        logger.info(f"🚀 Processing {chunk['label']} ({chunk['start']} to {chunk['end']})")
        
        resources = []
        for acc_id in acc_ids:
            # Creating a discrete resource for each account/chunk
            res = fetch_meta_discrete_chunks(
                account_id=acc_id, 
                access_token=token, 
                fields=reporting_fields, 
                start_date=chunk['start'], 
                end_date=chunk['end']
            )
            # Standardize table name for BigQuery
            res.table_name = "facebook_insights"
            resources.append(res)

        # Execution Phase
        try:
            info = pipeline.run(resources)
            logger.info(f"✅ Loaded {chunk['label']}: {info}")
        except Exception as e:
            logger.error(f"❌ Failed {chunk['label']}: {str(e)}")
            continue

if __name__ == "__main__":
    run_discrete_backfill()
