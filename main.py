import dlt
import os
import sys
from facebook_ads import facebook_insights_source

def run_fb_safe_pipeline():
    # 1. ÁNH XẠ CREDENTIALS (Bắt buộc để dlt không báo thiếu field)
    os.environ["DESTINATION__BIGQUERY__CREDENTIALS__PROJECT_ID"] = os.environ.get("GCP_PROJECT_ID")
    os.environ["DESTINATION__BIGQUERY__CREDENTIALS__CLIENT_EMAIL"] = os.environ.get("GCP_CLIENT_EMAIL")
    os.environ["DESTINATION__BIGQUERY__CREDENTIALS__PRIVATE_KEY"] = os.environ.get("GCP_PRIVATE_KEY", "").replace("\\n", "\n")
    os.environ["DESTINATION__BIGQUERY__LOCATION"] = "asia-southeast1"

    # 2. KHỞI TẠO PIPELINE (Trỏ thẳng vào dataset con cưng v2)
    pipeline = dlt.pipeline(
        pipeline_name="fb_ads_safe_sync", 
        destination="bigquery", 
        dataset_name="fb_ads_ahb1_report_v2"
    )

    # 3. THÔNG SỐ CẤU HÌNH
    fb_access_token = os.environ.get("FB_ACCESS_TOKEN")
    all_account_ids = [x.strip() for x in os.environ.get("FB_ACCOUNT_ID", "").split(",") if x.strip()]

    # Phải có đủ NAME fields để ní tách Naming Convention
    standard_fields = (
        "account_id", "campaign_id", "campaign_name", 
        "adset_id", "adset_name", "ad_id", "ad_name", 
        "date_start", "spend", "impressions", "clicks", 
        "reach", "frequency", "actions"
    )

    all_sources = []
    for acc_id in all_account_ids:
        # TEST 30 NGÀY TRƯỚC: Để dlt cập nhật schema campaign_name nhanh chóng
        days = 30 
        print(f"📡 Safe Sync Account: {acc_id} | Days: {days}")

        # --- CÁCH GÁN TÊN TRỰC TIẾP (Dứt điểm lỗi with_name) ---

        # Luồng 1: Master Insights
        src_master = facebook_insights_source(
            account_id=acc_id, access_token=fb_access_token, 
            initial_load_past_days=days, fields=standard_fields
        )
        src_master.facebook_insights.table_name = "facebook_insights"
        all_sources.append(src_master)

        # Luồng 2: Age & Gender Breakdown
        src_ag = facebook_insights_source(
            account_id=acc_id, access_token=fb_access_token, 
            initial_load_past_days=days, fields=standard_fields, 
            breakdowns="ads_insights_age_and_gender"
        )
        src_ag.facebook_insights.table_name = "insights_age_gender"
        all_sources.append(src_ag)

        # Luồng 3: Region Breakdown (Location)
        src_reg = facebook_insights_source(
            account_id=acc_id, access_token=fb_access_token, 
            initial_load_past_days=days, fields=standard_fields, 
            breakdowns="ads_insights_region"
        )
        src_reg.facebook_insights.table_name = "insights_region"
        all_sources.append(src_reg)

    # 4. KHAI HỎA
    print("🚀 Đang chạy Safe Sync (30 days) để cập nhật Schema...")
    info = pipeline.run(all_sources)
    print(info)

if __name__ == "__main__":
    run_fb_safe_pipeline()
