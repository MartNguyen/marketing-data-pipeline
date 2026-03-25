"""
Module: Meta Ads Production Backfill
Strategy: Monthly Discrete Extraction (Non-redundant)
Standard: Agency-Grade Workspace
"""

import dlt
import os
import logging
from facebook_ads import facebook_insights_source

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def run_monthly_discrete_backfill():
    # 1. Environment Configuration
    os.environ["DESTINATION__BIGQUERY__CREDENTIALS__PROJECT_ID"] = os.environ.get("GCP_PROJECT_ID")
    os.environ["DESTINATION__BIGQUERY__CREDENTIALS__CLIENT_EMAIL"] = os.environ.get("GCP_CLIENT_EMAIL")
    os.environ["DESTINATION__BIGQUERY__CREDENTIALS__PRIVATE_KEY"] = os.environ.get("GCP_PRIVATE_KEY", "").replace("\\n", "\n")

    pipeline = dlt.pipeline(
        pipeline_name="fb_ads_monthly_discrete",
        destination="bigquery",
        dataset_name="fb_ads_ahb1_report_v2"
    )

    fb_token = os.environ.get("FB_ACCESS_TOKEN")
    acc_ids = [a.strip() for a in os.environ.get("FB_ACCOUNT_ID", "").split(",") if a.strip()]
    
    fields = ("account_id", "campaign_id", "campaign_name", "adset_id", "ad_id", "date_start", "spend", "impressions", "clicks", "reach", "frequency", "actions")

    # 2. DISCRETE MONTH LIST (Không trùng lặp - Mỗi tháng là một block riêng)
    MONTH_CHUNKS = [
        {"start": "2025-01-01", "end": "2025-03-31", "label": "2025_Q1"},
        {"start": "2025-04-01", "end": "2025-06-30", "label": "2025_Q2"},
        {"start": "2025-07-01", "end": "2025-09-30", "label": "2025_Q3"},
        {"start": "2025-10-01", "end": "2025-12-31", "label": "2025_Q4"},
        {"start": "2026-01-01", "end": "2026-03-25", "label": "2026_YTD"}
    ]

    for chunk in MONTH_CHUNKS:
        logger.info(f"🚀 EXECUTING BLOCK: {chunk['label']} ({chunk['start']} to {chunk['end']})")
        sources = []
        
        for acc_id in acc_ids:
            # Truyền start_date trực tiếp vào source call thay vì dùng .bind()
            # Đây là cách pass params chuẩn để dlt không báo TypeError
            src = facebook_insights_source(
                account_id=acc_id, 
                access_token=fb_token, 
                fields=fields,
                start_date=chunk['start'], # Pass trực tiếp ở đây
                end_date=chunk['end']      # Pass trực tiếp ở đây
            )
            
            # Ép table name và lọc đúng resource chính
            src.facebook_insights.table_name = "facebook_insights"
            sources.append(src)

        try:
            load_info = pipeline.run(sources)
            logger.info(f"✅ Success: {chunk['label']} | Jobs: {load_info}")
        except Exception as e:
            logger.error(f"❌ Failed: {chunk['label']} | Error: {str(e)}")
            continue

if __name__ == "__main__":
    run_monthly_discrete_backfill()
