import dlt
import os
import sys
from facebook_ads import facebook_insights_source

def run_pre_flight():
    # 1. Credentials (Cực kỳ cẩn thận phần này cho ní)
    os.environ["DESTINATION__BIGQUERY__CREDENTIALS__PROJECT_ID"] = os.environ.get("GCP_PROJECT_ID")
    os.environ["DESTINATION__BIGQUERY__CREDENTIALS__CLIENT_EMAIL"] = os.environ.get("GCP_CLIENT_EMAIL")
    os.environ["DESTINATION__BIGQUERY__CREDENTIALS__PRIVATE_KEY"] = os.environ.get("GCP_PRIVATE_KEY", "").replace("\\n", "\n")
    os.environ["DESTINATION__BIGQUERY__LOCATION"] = "asia-southeast1"

    pipeline = dlt.pipeline(
        pipeline_name="pre_flight_v6_final",
        destination="bigquery",
        dataset_name="fb_ads_media_test" 
    )

    # 2. CHỈ DÙNG BỘ FIELDS AN TOÀN
    # 'actions' sẽ bao gồm cả Interaction, Video 3s, 15s... Meta sẽ tự nhả trong này.
    standard_fields = (
        "campaign_id", "adset_id", "ad_id", "date_start", 
        "spend", "impressions", "clicks", "reach", "frequency", 
        "actions" 
    )

    fb_token = os.environ.get("FB_ACCESS_TOKEN")
    acc_id = '587898528769829'

    # 3. Khởi tạo source (Dùng đúng cách gọi đã thành công v2)
    master_source = facebook_insights_source(
        account_id=acc_id, access_token=fb_token,
        initial_load_past_days=1, level="ad", fields=standard_fields,
        breakdowns="ads_insights"
    )

    print("Bắt đầu Pre-Flight Check (Vắt sạch lỗi 400)...")
    info = pipeline.run(master_source)
    print(info)

if __name__ == "__main__":
    run_pre_flight()
