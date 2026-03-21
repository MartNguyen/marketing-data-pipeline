import dlt
import os
import sys

# Import thư viện từ folder facebook_ads
try:
    from facebook_ads import facebook_ads_source, facebook_insights_source
except ImportError:
    print("LỖI: Không tìm thấy folder 'facebook_ads'.")
    sys.exit(1)

def load_facebook_data():
    # 1. LẤY THÔNG TIN BẢO MẬT (GitHub Secrets)
    bq_project_id = os.environ.get("GCP_PROJECT_ID", "ahb-dltxgg-bigquery")
    bq_client_email = os.environ.get("GCP_CLIENT_EMAIL", "dlt-bigquery-pusher@ahb-dltxgg-bigquery.iam.gserviceaccount.com")
    bq_private_key = os.environ.get("GCP_PRIVATE_KEY")
    if bq_private_key:
        bq_private_key = bq_private_key.replace("\\n", "\n")
        
    fb_access_token = os.environ.get("FB_ACCESS_TOKEN")
    fb_account_ids_str = os.environ.get("FB_ACCOUNT_ID", "")
    fb_account_ids = [x.strip() for x in fb_account_ids_str.split(",") if x.strip()]

    # 2. THIẾT LẬP CREDENTIALS (Singapore Region)
    os.environ["DESTINATION__BIGQUERY__CREDENTIALS__PROJECT_ID"] = bq_project_id
    os.environ["DESTINATION__BIGQUERY__CREDENTIALS__CLIENT_EMAIL"] = bq_client_email
    os.environ["DESTINATION__BIGQUERY__CREDENTIALS__PRIVATE_KEY"] = bq_private_key
    os.environ["DESTINATION__BIGQUERY__LOCATION"] = "asia-southeast1"

    # 3. KHỞI TẠO PIPELINE
    # Đổi tên pipeline v4 để đảm bảo dlt xóa sạch các cấu hình lỗi trước đó
    pipeline = dlt.pipeline(
        pipeline_name="fb_ads_master_pipeline_v4",
        destination="bigquery",
        dataset_name="fb_ads_ahb1_report_v2"
    )

    all_sources = []
    print(f"Bắt đầu xử lý {len(fb_account_ids)} tài khoản: {fb_account_ids}")

    for acc_id in fb_account_ids:
        print(f"--- Đang cấu hình cho Account ID: {acc_id} ---")
        
        # SOURCE 1: OBJECTS (Campaigns, Ad Sets, Ads, Creatives)
        obj_source = facebook_ads_source(account_id=acc_id, access_token=fb_access_token) \
            .with_resources("campaigns", "ad_sets", "ads", "ad_creatives")
        for res in obj_source.resources.values():
            res.apply_hints(write_disposition="merge")
        all_sources.append(obj_source)

        # SOURCE 2: MASTER INSIGHTS (Bảng tổng quát không breakdown)
        master_ins = facebook_insights_source(
            account_id=acc_id, access_token=fb_access_token,
            initial_load_past_days=30, time_increment_days=1,
            level="ad", fields=("campaign_id", "adset_id", "ad_id", "date_start", "spend", "impressions", "clicks", "account_id")
        )
        for res in master_ins.resources.values():
            res.table_name = "facebook_insights"
        all_sources.append(master_ins)

        # SOURCE 3: BREAKDOWN AGE (Bảng thực tế theo nhóm tuổi)
        age_ins = facebook_insights_source(
            account_id=acc_id, access_token=fb_access_token,
            initial_load_past_days=30, breakdowns="age", # Sửa từ tuple sang string để tránh KeyError
            level="ad", fields=("ad_id", "date_start", "spend", "impressions", "clicks")
        )
        for res in age_ins.resources.values():
            res.table_name = "insights_age"
        all_sources.append(age_ins)

        # SOURCE 4: BREAKDOWN GENDER (Bảng thực tế theo giới tính)
        gender_ins = facebook_insights_source(
            account_id=acc_id, access_token=fb_access_token,
            initial_load_past_days=30, breakdowns="gender", # Sửa sang string đơn lẻ
            level="ad", fields=("ad_id", "date_start", "spend", "impressions", "clicks")
        )
        for res in gender_ins.resources.values():
            res.table_name = "insights_gender"
        all_sources.append(gender_ins)

        # SOURCE 5: BREAKDOWN REGION (Bảng thực tế theo tỉnh thành)
        region_ins = facebook_insights_source(
            account_id=acc_id, access_token=fb_access_token,
            initial_load_past_days=30, breakdowns="region",
            level="ad", fields=("ad_id", "date_start", "spend", "impressions", "clicks")
        )
        for res in region_ins.resources.values():
            res.table_name = "insights_region"
        all_sources.append(region_ins)

    # 4. CHẠY PIPELINE
    if all_sources:
        load_info = pipeline.run(all_sources)
        print(load_info)

if __name__ == "__main__":
    load_facebook_data()
