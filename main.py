import dlt
import os
import sys
from facebook_ads import facebook_insights_source

def run_test():
    # 1. NẠP ĐỒ NGHỀ (Lấy từ GitHub Secrets)
    bq_project_id = os.environ.get("GCP_PROJECT_ID")
    bq_client_email = os.environ.get("GCP_CLIENT_EMAIL")
    bq_private_key = os.environ.get("GCP_PRIVATE_KEY", "").replace("\\n", "\n")
    fb_token = os.environ.get("FB_ACCESS_TOKEN")

    if not bq_private_key or not fb_token:
        print("LỖI: Thiếu Secret rồi ní ơi, check lại GitHub nhé!")
        return

    # 2. THIẾT LẬP MÔI TRƯỜNG CHO DLT
    os.environ["DESTINATION__BIGQUERY__CREDENTIALS__PROJECT_ID"] = bq_project_id
    os.environ["DESTINATION__BIGQUERY__CREDENTIALS__CLIENT_EMAIL"] = bq_client_email
    os.environ["DESTINATION__BIGQUERY__CREDENTIALS__PRIVATE_KEY"] = bq_private_key
    os.environ["DESTINATION__BIGQUERY__LOCATION"] = "asia-southeast1"

    # 3. CẤU HÌNH PIPELINE TEST
    pipeline = dlt.pipeline(
        pipeline_name="test_video_v3",
        destination="bigquery",
        dataset_name="fb_ads_test_video" 
    )

    # Lấy 1 tài khoản test và 1 ngày duy nhất
    test_acc_id = '587898528769829' 
    source = facebook_insights_source(
        account_id=test_acc_id,
        access_token=fb_token,
        initial_load_past_days=1,
        level="ad",
        fields=(
            "campaign_id", "adset_id", "ad_id", "date_start", 
            "spend", "impressions", "clicks", "account_id",
            "video_play_actions", "video_p3s_actions" 
        )
    )

    # KHAI HỎA
    info = pipeline.run(source.with_resources("facebook_insights"))
    print(info)

if __name__ == "__main__":
    run_test()
