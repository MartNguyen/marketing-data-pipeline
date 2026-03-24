import dlt
import os
from facebook_ads import facebook_insights_source

def run_smoke_test():
    # 1. Credentials
    os.environ["DESTINATION__BIGQUERY__CREDENTIALS__PROJECT_ID"] = os.environ.get("GCP_PROJECT_ID")
    os.environ["DESTINATION__BIGQUERY__CREDENTIALS__CLIENT_EMAIL"] = os.environ.get("GCP_CLIENT_EMAIL")
    os.environ["DESTINATION__BIGQUERY__CREDENTIALS__PRIVATE_KEY"] = os.environ.get("GCP_PRIVATE_KEY", "").replace("\\n", "\n")
    os.environ["DESTINATION__BIGQUERY__LOCATION"] = "asia-southeast1"

    # 2. Pipeline Test
    pipeline = dlt.pipeline(
        pipeline_name="media_smoke_test_v2",
        destination="bigquery",
        dataset_name="fb_ads_media_test" 
    )

    # 3. Bộ Metrics chuẩn cho team Media
    standard_fields = (
        "campaign_id", "adset_id", "ad_id", "date_start", "spend", "impressions", "clicks",
        "reach", "frequency", "video_3sec_watched_actions", "video_thruplay_actions", "actions"
    )

    # 4. KHỞI TẠO RIÊNG BIỆT ĐỂ TRÁNH LỖI CÚ PHÁP
    fb_token = os.environ.get("FB_ACCESS_TOKEN")
    acc_id = '587898528769829'

    # Nguồn 1: Master Insights
    master_source = facebook_insights_source(
        account_id=acc_id, access_token=fb_token,
        initial_load_past_days=1, level="ad", fields=standard_fields,
        breakdowns="ads_insights"
    )

    # Nguồn 2: Age & Gender (Dùng đúng Key mình đã Peek được trước đó)
    age_gender_source = facebook_insights_source(
        account_id=acc_id, access_token=fb_token,
        initial_load_past_days=1, level="ad", fields=standard_fields,
        breakdowns="ads_insights_age_and_gender"
    )

    print("Bắt đầu Smoke Test (Bản sửa cú pháp)...")
    # Chạy song song cả 2 nguồn
    info = pipeline.run([master_source, age_gender_source])
    print(info)

if __name__ == "__main__":
    run_smoke_test()
