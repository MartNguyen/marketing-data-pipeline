import dlt
import os
import sys

try:
    from facebook_ads import facebook_insights_source
except ImportError:
    print("Lỗi: Không tìm thấy thư viện facebook_ads. Hãy kiểm tra requirements.txt")
    sys.exit(1)

def run_fb_production():
    # 1. Credentials
    os.environ["DESTINATION__BIGQUERY__CREDENTIALS__PRIVATE_KEY"] = os.environ.get("GCP_PRIVATE_KEY", "").replace("\\n", "\n")
    
    pipeline = dlt.pipeline(
        pipeline_name="fb_ads_prod_full", 
        destination="bigquery", 
        dataset_name="fb_ads_ahb1_report_v2"
    )

    # 2. Chìa khóa và Danh sách tài khoản
    fb_access_token = os.environ.get("FB_ACCESS_TOKEN")
    old_account_id = "587898528769829" 
    all_account_ids = [x.strip() for x in os.environ.get("FB_ACCOUNT_ID", "").split(",") if x.strip()]

    if not fb_access_token:
        print("Lỗi: Thiếu FB_ACCESS_TOKEN trong environment variables.")
        sys.exit(1)

    # Bộ fields chuẩn
    standard_fields = ("campaign_id", "adset_id", "ad_id", "date_start", "spend", "impressions", "clicks", "reach", "frequency", "actions")

    all_sources = []
    for acc_id in all_account_ids:
        # Cũ: 450 ngày (2025). Mới: 90 ngày (2026)
        days = 450 if acc_id == old_account_id else 90
        
        # --- FIX: Đã truyền access_token trực tiếp vào hàm ---
        
        # Nguồn 1: Master
        all_sources.append(facebook_insights_source(
            account_id=acc_id, access_token=fb_access_token, initial_load_past_days=days, fields=standard_fields
        ).with_resources("facebook_insights"))

        # Nguồn 2: Age & Gender
        all_sources.append(facebook_insights_source(
            account_id=acc_id, access_token=fb_access_token, initial_load_past_days=days, fields=standard_fields, breakdowns="ads_insights_age_and_gender"
        ).with_resources("facebook_insights").with_name("insights_age_gender"))

        # Nguồn 3: Region
        all_sources.append(facebook_insights_source(
            account_id=acc_id, access_token=fb_access_token, initial_load_past_days=days, fields=standard_fields, breakdowns="ads_insights_region"
        ).with_resources("facebook_insights").with_name("insights_region"))

    print(f"--- ĐANG NẠP DỮ LIỆU FULL CHO {len(all_account_ids)} TÀI KHOẢN ---")
    info = pipeline.run(all_sources)
    print(info)

if __name__ == "__main__":
    run_fb_production()
