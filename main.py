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
    if bq_private_key:
        # Thay thế ký tự \n bằng dấu xuống dòng thực sự
        bq_private_key = bq_private_key.replace("\\n", "\n")
    fb_access_token = os.environ.get("FB_ACCESS_TOKEN")
    fb_account_ids_str = os.environ.get("FB_ACCOUNT_ID", "")
    fb_account_ids = [x.strip() for x in fb_account_ids_str.split(",") if x.strip()]

    # Kiểm tra xem có thiếu Key không
    if not bq_private_key or not fb_access_token or not fb_account_ids:
        print("LỖI: Thiếu Key, Token hoặc Account ID.")
        print(f"Debug info: Key={'OK' if bq_private_key else 'Missing'}, Token={'OK' if fb_access_token else 'Missing'}, IDs={fb_account_ids}")
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
        dataset_name="fb_ads_ahb1_report_v2"
    )
    
    # Lệnh này sẽ xóa bỏ các gói dữ liệu bị kẹt từ lần chạy lỗi trước
    print("Đang kiểm tra và xóa pending packages...")
    pipeline.drop_pending_packages()
    
    # 4. Cấu hình nguồn 1: Objects (Cấu trúc TK)
    all_sources = []
    
    print(f"Tìm thấy {len(fb_account_ids)} tài khoản cần chạy: {fb_account_ids}")

    # acc_id là biến chạy trong vòng lặp
    for acc_id in fb_account_ids:
        print(f"--- Đang cấu hình cho Account ID: {acc_id} ---")
        
        # Source 1: Objects
        # SỬA LỖI Ở ĐÂY: Dùng acc_id (biến vòng lặp) thay vì fb_account_id cũ
        obj_source = facebook_ads_source(
            account_id=acc_id, 
            access_token=fb_access_token
        ).with_resources("campaigns", "ad_sets", "ads", "ad_creatives")

        obj_source.resources["campaigns"].apply_hints(write_disposition="merge")
        obj_source.resources["ad_sets"].apply_hints(write_disposition="merge")
        obj_source.resources["ads"].apply_hints(write_disposition="merge")
        obj_source.resources["ad_creatives"].apply_hints(write_disposition="merge")
        
        # Source 2: Insights
        ins_source = facebook_insights_source(
            account_id=acc_id,
            access_token=fb_access_token,
            initial_load_past_days=30, 
            time_increment_days=1,
            level="ad",
            fields=(
                "campaign_id", 
                "adset_id", 
                "ad_id", 
                "date_start",  # Ngày bắt đầu (Dùng để plot timeline)
                "date_stop",   # Ngày kết thúc
                "spend", 
                "impressions", 
                "clicks", 
                "cpc", 
                "cpm", 
                "account_id",
                "inline_post_engagement",
                "actions",                        # Chứa video_view (3s views)
                "video_play_actions",             # Lượt phát (Views trên UI mới)
                "video_thruplay_watched_actions", # ThruPlay (Xem >15s)
                "video_p50_watched_actions"
            )
        )
        all_sources.append(obj_source)
        all_sources.append(ins_source)

    # 6. Chạy Pipeline
    if all_sources:
        print("--- Bắt đầu tải dữ liệu ---")
        load_info = pipeline.run(all_sources)
        print(load_info)
        print("--- Hoàn thành ---")
    else:
        print("Không có source nào được tạo.")

if __name__ == "__main__":
    load_facebook_data()
