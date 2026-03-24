import dlt
import os
import sys

try:
    from facebook_ads import facebook_insights_source
except ImportError:
    sys.exit(1)

def load_facebook_data():
    # 1. THIẾT LẬP MÔI TRƯỜNG & CHÌA KHÓA
    os.environ["DESTINATION__BIGQUERY__CREDENTIALS__PROJECT_ID"] = os.environ.get("GCP_PROJECT_ID")
    os.environ["DESTINATION__BIGQUERY__CREDENTIALS__CLIENT_EMAIL"] = os.environ.get("GCP_CLIENT_EMAIL")
    os.environ["DESTINATION__BIGQUERY__CREDENTIALS__PRIVATE_KEY"] = os.environ.get("GCP_PRIVATE_KEY", "").replace("\\n", "\n")
    os.environ["DESTINATION__BIGQUERY__LOCATION"] = "asia-southeast1"

    fb_access_token = os.environ.get("FB_ACCESS_TOKEN")
    # Lấy danh sách tài khoản từ GitHub Secrets, tách bằng dấu phẩy
    fb_account_ids = [x.strip() for x in os.environ.get("FB_ACCOUNT_ID", "").split(",") if x.strip()]

    # 2. KHỞI TẠO PIPELINE (Trỏ thẳng vào Dataset Master)
    pipeline = dlt.pipeline(
        pipeline_name="fb_ads_production_pipeline",
        destination="bigquery",
        dataset_name="fb_ads_ahb1_report_v2"
    )

    # 3. BỘ CHỈ SỐ CHUẨN (Reach/Freq giữ làm placeholder, data thực ăn từ Actions)
    standard_fields = (
        "campaign_id", "adset_id", "ad_id", "date_start", 
        "spend", "impressions", "clicks", 
        "reach", "frequency", "actions"
    )

    all_sources = []
    for acc_id in fb_account_ids:
        # Khởi tạo nguồn cho từng tài khoản, lùi lại 30 ngày để cập nhật số liệu trễ
        source = facebook_insights_source(
            account_id=acc_id, 
            access_token=fb_access_token,
            initial_load_past_days=30, 
            level="ad", 
            fields=standard_fields,
            breakdowns="ads_insights"
        )
        # Ép tất cả vào chung một bảng mẹ
        for res in source.resources.values(): 
            res.table_name = "facebook_insights"
        
        all_sources.append(source)

    # 4. KHAI HỎA NẠP DỮ LIỆU
    if all_sources:
        print(f"Đang tiến hành nạp dữ liệu cho {len(fb_account_ids)} tài khoản Meta...")
        load_info = pipeline.run(all_sources)
        print(load_info)
    else:
        print("Không tìm thấy Account ID nào để chạy!")

if __name__ == "__main__":
    load_facebook_data()
