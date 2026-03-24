import dlt
import os
from facebook_ads import facebook_insights_source

def run_smoke_test():
    # 1. Credentials (Dùng lại Secret cũ của ní)
    os.environ["DESTINATION__BIGQUERY__CREDENTIALS__PROJECT_ID"] = os.environ.get("GCP_PROJECT_ID")
    os.environ["DESTINATION__BIGQUERY__CREDENTIALS__CLIENT_EMAIL"] = os.environ.get("GCP_CLIENT_EMAIL")
    os.environ["DESTINATION__BIGQUERY__CREDENTIALS__PRIVATE_KEY"] = os.environ.get("GCP_PRIVATE_KEY", "").replace("\\n", "\n")
    os.environ["DESTINATION__BIGQUERY__LOCATION"] = "asia-southeast1"

    # 2. Pipeline Test (Đổ vào dataset 'fb_ads_media_test' để check số)
    pipeline = dlt.pipeline(
        pipeline_name="media_smoke_test",
        destination="bigquery",
        dataset_name="fb_ads_media_test" 
    )

    # 3. Bộ Metrics "Full Đồ Chơi" cho team Media
    # Bao gồm: Reach, Freq, Video 3s/15s và Interaction (actions)
    standard_fields = (
        "campaign_id", "ad_id", "date_start", "spend", "impressions", "clicks",
        "reach", "frequency", "video_3sec_watched_actions", "video_thruplay_actions", "actions"
    )

    # 4. Chỉ kéo 1 ngày của 1 tài khoản (587898528769829)
    source = facebook_insights_source(
        account_id='587898528769829',
        access_token=os.environ.get("FB_ACCESS_TOKEN"),
        initial_load_past_days=1, # Ép lấy 1 ngày cho cực nhanh
        level="ad",
        fields=standard_fields
    )

    # 5. Khai hỏa đồng thời cả Master và Breakdowns
    # Để kiểm tra xem Age/Gender có bị lỗi khi kéo Reach/Freq không
    print("Bắt đầu Smoke Test...")
    info = pipeline.run([
        source.with_resources("facebook_insights"),
        source.with_resources("facebook_insights").with_breakdowns("age_and_gender")
    ])
    print(info)

if __name__ == "__main__":
    run_smoke_test()
