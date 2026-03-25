import dlt
import os
import sys
from facebook_ads import facebook_insights_source

def run_fb_safe_pipeline():
    # 1. Ánh xạ Credentials
    os.environ["DESTINATION__BIGQUERY__CREDENTIALS__PROJECT_ID"] = os.environ.get("GCP_PROJECT_ID")
    os.environ["DESTINATION__BIGQUERY__CREDENTIALS__CLIENT_EMAIL"] = os.environ.get("GCP_CLIENT_EMAIL")
    os.environ["DESTINATION__BIGQUERY__CREDENTIALS__PRIVATE_KEY"] = os.environ.get("GCP_PRIVATE_KEY", "").replace("\\n", "\n")

    pipeline = dlt.pipeline(
        pipeline_name="fb_ads_safe_sync", 
        destination="bigquery", 
        dataset_name="fb_ads_ahb1_report_v2"
    )

    fb_access_token = os.environ.get("FB_ACCESS_TOKEN")
    all_account_ids = [x.strip() for x in os.environ.get("FB_ACCOUNT_ID", "").split(",") if x.strip()]

    # Đảm bảo có Campaign Name để không lỗi View SQL
    standard_fields = (
        "account_id", "campaign_id", "campaign_name", 
        "adset_id", "ad_id", "date_start", "spend", 
        "impressions", "clicks", "reach", "frequency", "actions"
    )

    all_sources = []
    for acc_id in all_account_ids:
        # TEST AN TOÀN: Chỉ lấy 30 ngày để FIX SCHEMA và lấy data mới nhất
        days = 30 
        print(f"📡 Safe Sync Account: {acc_id} | Days: {days}")

        # Chỉ kéo bảng chính và bảng Age/Gender để giảm tải cho API
        all_sources.append(facebook_insights_source(
            account_id=acc_id, access_token=fb_access_token, initial_load_past_days=days, fields=standard_fields
        ).with_resources("facebook_insights"))
        
        all_sources.append(facebook_insights_source(
            account_id=acc_id, access_token=fb_access_token, initial_load_past_days=days, fields=standard_fields, breakdowns="ads_insights_age_and_gender"
        ).with_resources("facebook_insights").with_name("insights_age_gender"))

    print("🚀 Đang chạy Safe Sync để cập nhật Schema...")
    info = pipeline.run(all_sources)
    print(info)

if __name__ == "__main__":
    run_fb_safe_pipeline()
