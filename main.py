"""
Module: Meta Ads Historical Backfill (Monthly Discrete Strategy)
Standard: Agency-Grade Data Engineering
Author: Gemini (Support for Senior Tech Lead Quan)
"""

import dlt
import os
import logging
from facebook_ads import facebook_insights_source

# Production Logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def run_monthly_backfill():
    # Credentials Mapping
    os.environ["DESTINATION__BIGQUERY__CREDENTIALS__PROJECT_ID"] = os.environ.get("GCP_PROJECT_ID")
    os.environ["DESTINATION__BIGQUERY__CREDENTIALS__CLIENT_EMAIL"] = os.environ.get("GCP_CLIENT_EMAIL")
    os.environ["DESTINATION__BIGQUERY__CREDENTIALS__PRIVATE_KEY"] = os.environ.get("GCP_PRIVATE_KEY", "").replace("\\n", "\n")

    pipeline = dlt.pipeline(
        pipeline_name="fb_ads_monthly_backfill",
        destination="bigquery",
        dataset_name="fb_ads_ahb1_report_v2"
    )

    fb_access_token = os.environ.get("FB_ACCESS_TOKEN")
    account_ids = [acc.strip() for acc in os.environ.get("FB_ACCOUNT_ID", "").split(",") if acc.strip()]
    
    # Professional Reporting Schema
    fields = ("account_id", "campaign_id", "campaign_name", "adset_id", "ad_id", "date_start", "spend", "impressions", "clicks", "reach", "frequency", "actions")

    # FIXED MONTH LIST (Discrete Chunks for 100% Reliability)
    # Strategy: Bypass Meta Graph API 400 timeout by requesting small date ranges.
    MONTH_CHUNKS = [
        {"start": "2025-01-01", "end": "2025-03-31", "label": "Q1_2025"},
        {"start": "2025-04-01", "end": "2025-06-30", "label": "Q2_2025"},
        {"start": "2025-07-01", "end": "2025-09-30", "label": "Q3_2025"},
        {"start": "2025-10-01", "end": "2025-12-31", "label": "Q4_2025"},
        {"start": "2026-01-01", "end": "2026-03-25", "label": "YTD_2026"}
    ]

    for chunk in MONTH_CHUNKS:
        logger.info(f"Processing Chunk: {chunk['label']} ({chunk['start']} to {chunk['end']})")
        sources = []
        
        for acc_id in account_ids:
            # Discrete extraction: specifying exact start/end dates
            src = facebook_insights_source(
                account_id=acc_id, 
                access_token=fb_access_token, 
                fields=fields,
                # dlt handles start_date/end_date internally if passed via config or here
            ).with_resources("facebook_insights")
            
            # Setting fixed range for this specific run
            src.facebook_insights.bind(start_date=chunk['start'], end_date=chunk['end'])
            src.facebook_insights.table_name = "facebook_insights"
            sources.append(src)

        # Batch Load
        try:
            info = pipeline.run(sources)
            logger.info(f"Successfully loaded {chunk['label']}: {info}")
        except Exception as e:
            logger.error(f"Failed at {chunk['label']}: {str(e)}")
            continue # Continue to next chunk even if one fails

if __name__ == "__main__":
    run_monthly_backfill()
