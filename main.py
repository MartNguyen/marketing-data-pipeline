import dlt
import os
import sys

# Import thư viện từ folder facebook_ads (bắt buộc phải có folder này cùng cấp)
try:
    from facebook_ads import facebook_ads_source, facebook_insights_source
except ImportError:
    print("LỖI: Không tìm thấy folder 'facebook_ads'. Hãy đảm bảo bạn đã copy folder này từ Colab về cùng chỗ với file main.py")
    sys.exit(1)

def load_facebook_data():
    # 1. Lấy thông tin bảo mật từ Biến môi trường (GitHub Secrets sẽ điền vào đây)
    # Nếu chạy local, bạn phải tự set các biến này
    bq_project_id = os.environ.get("GCP_PROJECT_ID", "ahb-dltxgg-bigquery")
    bq_client_email = os.environ.get("GCP_CLIENT_EMAIL", "dlt-bigquery-pusher@ahb-dltxgg-bigquery.iam.gserviceaccount.com")
    
    # Quan trọng: Key lấy từ môi trường để bảo mật
    bq_private_key = os.environ.get("GCP_PRIVATE_KEY") 
    fb_access_token = os.environ.get("FB_ACCESS_TOKEN")
    fb_account_id = os.environ.get("FB_ACCOUNT_ID", "587898528769829")

    # Kiểm tra xem có thiếu Key không
    if not bq_private_key or not fb_access_token:
        print("LỖI: Thiếu GCP_PRIVATE_KEY hoặc FB_ACCESS_TOKEN trong biến môi trường.")
        return

    # 2. Thiết lập Credentials cho BigQuery (dlt tự động đọc các biến này)
    os.environ["DESTINATION__BIGQUERY__CREDENTIALS__PROJECT_ID"] = bq_project_id
    os.environ["DESTINATION__BIGQUERY__CREDENTIALS__CLIENT_EMAIL"] = bq_client_email
    os.environ["DESTINATION__BIGQUERY__CREDENTIALS__PRIVATE_KEY"] = bq_private_key
    os.environ["DESTINATION__BIGQUERY__LOCATION"] = "asia-southeast1"

    # 3. Tạo Pipeline
    pipeline = dlt.pipeline(
        pipeline_name="fb_ads_to_bq",
        destination="bigquery",
        dataset_name="fb_ads_ahb1_report"
    )

    # 4. Cấu hình nguồn 1: Objects (Cấu trúc TK)
    fb_source = facebook_ads_source(
        account_id=fb_account_id,
        access_token=fb_access_token
    ).with_resources(
        "campaigns",
        "ad_sets",
        "ads",
        "ad_creatives"
    )

    # 5. Cấu hình nguồn 2: Insights (Số liệu)
    insights_source = facebook_insights_source(
        account_id=fb_account_id,
        access_token=fb_access_token,
        initial_load_past_days=30, # Để 30 ngày cho nhẹ, load lần đầu có thể tăng lên
        time_increment_days=1,
        level="ad",
        fields=(
            "campaign_id", "adset_id", "ad_id", 
            "date_start", "date_stop", 
            "spend", "impressions", "clicks", "cpc", "cpm"
        )
    )

    # 6. Chạy Pipeline
    print("--- Đang tải dữ liệu từ Facebook về BigQuery ---")
    load_info = pipeline.run([fb_source, insights_source])
    print(load_info)
    print("--- Hoàn thành ---")

if __name__ == "__main__":
    load_facebook_data()