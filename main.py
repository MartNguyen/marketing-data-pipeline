import dlt
import os
from facebook_ads import facebook_insights_source

def run_fb_production():
    # 1. Credentials
    os.environ["DESTINATION__BIGQUERY__CREDENTIALS__PRIVATE_KEY"] = os.environ.get("GCP_PRIVATE_KEY", "").replace("\\n", "\n")
    
    pipeline = dlt.pipeline(
        pipeline_name="fb_ads_prod_full", 
        destination="bigquery", 
        dataset_name="fb_ads_ahb1_report_v2"
    )

    # 2. Phân loại tài khoản
    # Cốt Quân điền ID tài khoản cũ vào đây
    old_account_id = "587898528769829" 
    all_account_ids = [x.strip() for x in os.environ.get("FB_ACCOUNT_ID", "").split(",") if x.strip()]

    # Bộ fields chuẩn để khớp template Media (có 'actions' để lấy View/Interaction)
    standard_fields = ("campaign_id", "adset_id", "ad_id", "date_start", "spend", "impressions", "clicks", "reach", "frequency", "actions")

    all_sources = []
    for acc_id in all_account_ids:
        # Cũ: lấy trọn 2025 (450 ngày). Mới: lấy 2026 tiệm tiến (90 ngày)
        days = 450 if acc_id == old_account_id else 90
        
        # Tạo 3 nguồn data cho 3 bảng báo cáo chính
        all_sources.append(facebook_insights_source(account_id=acc_id, initial_load_past_days=days, fields=standard_fields).with_resources("facebook_insights"))
        all_sources.append(facebook_insights_source(account_id=acc_id, initial_load_past_days=days, fields=standard_fields, breakdowns="ads_insights_age_and_gender").with_resources("facebook_insights").with_name("insights_age_gender"))
        all_sources.append(facebook_insights_source(account_id=acc_id, initial_load_past_days=days, fields=standard_fields, breakdowns="ads_insights_region").with_resources("facebook_insights").with_name("insights_region"))

    print(f"--- ĐANG NẠP DỮ LIỆU FULL CHO {len(all_account_ids)} TÀI KHOẢN ---")
    print(pipeline.run(all_sources))

if __name__ == "__main__":
    run_fb_production()
