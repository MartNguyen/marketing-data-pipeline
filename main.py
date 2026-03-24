import dlt
import os
import sys

try:
    from facebook_ads import facebook_insights_source
except ImportError:
    print("Lỗi: Không tìm thấy thư viện facebook_ads.")
    sys.exit(1)

def run_fb_production():
    # 1. ÁNH XẠ CREDENTIALS (Fix lỗi ConfigFieldMissing)
    # Chúng ta phải map trực tiếp các biến từ GitHub Secrets vào đúng tên dlt cần
    os.environ["DESTINATION__BIGQUERY__CREDENTIALS__PROJECT_ID"] = os.environ.get("GCP_PROJECT_ID")
    os.environ["DESTINATION__BIGQUERY__CREDENTIALS__CLIENT_EMAIL"] = os.environ.get("GCP_CLIENT_EMAIL")
    os.environ["DESTINATION__BIGQUERY__CREDENTIALS__PRIVATE_KEY"] = os.environ.get("GCP_PRIVATE_KEY", "").replace("\\n", "\n")
    os.environ["DESTINATION__BIGQUERY__LOCATION"] = "asia-southeast1"

    # 2. Khởi tạo Pipeline
    pipeline = dlt.pipeline(
        pipeline_name="fb_ads_prod_full", 
        destination="bigquery", 
        dataset_name="fb_ads_ahb1_report_v2"
    )

    # 3. Chìa khóa Facebook & Danh sách tài khoản
    fb_access_token = os.environ.get("FB_ACCESS_TOKEN")
    old_account_id = "587898528769829" 
    all_account_ids = [x.strip() for x in os.environ.get("FB_ACCOUNT_ID", "").split(",") if x.strip()]

    if not fb_access_token:
        print("Lỗi: Thiếu FB_ACCESS_TOKEN.")
        sys.exit(1)

    # Bộ fields chuẩn theo template Media
    standard_fields = ("campaign_id", "adset_id", "ad_id", "date_start", "spend", "impressions", "clicks", "reach", "frequency", "actions")

    all_sources = []
    for acc_id in all_account_ids:
        # Phân luồng: Cũ (2025 - 450 ngày), Mới (2026 - 90 ngày)
        days = 450 if acc_id == old_account_id else 90
        
        # Nguồn 1: Master Insights
        m_src = facebook_insights_source(account_id=acc_id, access_token=fb_access_token, initial_load_past_days=days, fields=standard_fields)
        m_src.facebook_insights.table_name = "facebook_insights"
        all_sources.append(m_src)

        # Nguồn 2: Age & Gender Breakdown
        ag_src = facebook_insights_source(account_id=acc_id, access_token=fb_access_token, initial_load_past_days=days, fields=standard_fields, breakdowns="ads_insights_age_and_gender")
        ag_src.facebook_insights.table_name = "insights_age_gender"
        all_sources.append(ag_src)

        # Nguồn 3: Region Breakdown (Location)
        r_src = facebook_insights_source(account_id=acc_id, access_token=fb_access_token, initial_load_past_days=days, fields=standard_fields, breakdowns="ads_insights_region")
        r_src.facebook_insights.table_name = "insights_region"
        all_sources.append(r_src)

    print(f"--- ĐANG NẠP DỮ LIỆU FULL CHO {len(all_account_ids)} TÀI KHOẢN ---")
    info = pipeline.run(all_sources)
    print(info)

if __name__ == "__main__":
    run_fb_production()
