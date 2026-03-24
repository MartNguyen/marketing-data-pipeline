import dlt
import os
import sys
from facebook_ads import facebook_insights_source

def run_fb_final_pipeline():
    # 1. THIẾT LẬP MÔI TRƯỜNG (Map credentials chuẩn xác)
    os.environ["DESTINATION__BIGQUERY__CREDENTIALS__PROJECT_ID"] = os.environ.get("GCP_PROJECT_ID")
    os.environ["DESTINATION__BIGQUERY__CREDENTIALS__CLIENT_EMAIL"] = os.environ.get("GCP_CLIENT_EMAIL")
    os.environ["DESTINATION__BIGQUERY__CREDENTIALS__PRIVATE_KEY"] = os.environ.get("GCP_PRIVATE_KEY", "").replace("\\n", "\n")
    os.environ["DESTINATION__BIGQUERY__LOCATION"] = "asia-southeast1"

    pipeline = dlt.pipeline(
        pipeline_name="fb_ads_master_final", 
        destination="bigquery", 
        dataset_name="fb_ads_ahb1_report_v2"
    )

    # 2. CONFIG PARAMS
    fb_access_token = os.environ.get("FB_ACCESS_TOKEN")
    old_account_id = "587898528769829" # Tài khoản ní đã tiêu 1.6 tỷ
    all_account_ids = [x.strip() for x in os.environ.get("FB_ACCOUNT_ID", "").split(",") if x.strip()]

    # BỔ SUNG ĐỦ NAME FIELDS ĐỂ TÁCH NAMING CONVENTION
    standard_fields = (
        "account_id", "campaign_id", "campaign_name", 
        "adset_id", "adset_name", "ad_id", "ad_name", 
        "date_start", "spend", "impressions", "clicks", 
        "reach", "frequency", "actions"
    )

    all_sources = []
    for acc_id in all_account_ids:
        days = 450 if acc_id == old_account_id else 90
        print(f"📡 Processing Account: {acc_id} | Days: {days}")

        # Định nghĩa 3 luồng: Master, Age/Gender, Region
        configs = [
            {"name": "facebook_insights", "breakdown": None},
            {"name": "insights_age_gender", "breakdown": "ads_insights_age_and_gender"},
            {"name": "insights_region", "breakdown": "ads_insights_region"}
        ]

        for cfg in configs:
            source = facebook_insights_source(
                account_id=acc_id, 
                access_token=fb_access_token, 
                initial_load_past_days=days, 
                fields=standard_fields,
                breakdowns=cfg["breakdown"]
            )
            source.facebook_insights.table_name = cfg["name"]
            all_sources.append(source)

    # 3. KHAI HỎA
    print("🚀 Bắt đầu nạp dữ liệu chỉnh chu (Lưu ý: Có thể mất vài tiếng để Meta nhả Name)...")
    info = pipeline.run(all_sources)
    print(info)

if __name__ == "__main__":
    run_fb_final_pipeline()
