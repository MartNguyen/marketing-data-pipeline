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
        days = 450 if acc_id == old_account_id else 90
        
        # 1. Luồng Master
        master_source = facebook_insights_source(account_id=acc_id, access_token=fb_access_token, initial_load_past_days=days, fields=standard_fields)
        master_source.facebook_insights.table_name = "facebook_insights"
        all_sources.append(master_source)

        # 2. Luồng Age & Gender
        age_gender_source = facebook_insights_source(account_id=acc_id, access_token=fb_access_token, initial_load_past_days=days, fields=standard_fields, breakdowns="ads_insights_age_and_gender")
        age_gender_source.facebook_insights.table_name = "insights_age_gender"
        all_sources.append(age_gender_source)

        # 3. Luồng Region
        region_source = facebook_insights_source(account_id=acc_id, access_token=fb_access_token, initial_load_past_days=days, fields=standard_fields, breakdowns="ads_insights_region")
        region_source.facebook_insights.table_name = "insights_region"
        all_sources.append(region_source)

    print(f"--- ĐANG NẠP DỮ LIỆU FULL CHO {len(all_account_ids)} TÀI KHOẢN ---")
    info = pipeline.run(all_sources)
    print(info)

if __name__ == "__main__":
    run_fb_production()
