import dlt
import os
import sys

try:
    from facebook_ads import facebook_ads_source, facebook_insights_source
except ImportError:
    sys.exit(1)

def load_facebook_data():
    # 1. LẤY THÔNG TIN (GitHub Secrets)
    bq_project_id = os.environ.get("GCP_PROJECT_ID", "ahb-dltxgg-bigquery")
    bq_client_email = os.environ.get("GCP_CLIENT_EMAIL", "dlt-bigquery-pusher@ahb-dltxgg-bigquery.iam.gserviceaccount.com")
    bq_private_key = os.environ.get("GCP_PRIVATE_KEY", "").replace("\\n", "\n")
    fb_access_token = os.environ.get("FB_ACCESS_TOKEN")
    fb_account_ids = [x.strip() for x in os.environ.get("FB_ACCOUNT_ID", "").split(",") if x.strip()]

    # 2. CREDENTIALS
    os.environ["DESTINATION__BIGQUERY__CREDENTIALS__PROJECT_ID"] = bq_project_id
    os.environ["DESTINATION__BIGQUERY__CREDENTIALS__CLIENT_EMAIL"] = bq_client_email
    os.environ["DESTINATION__BIGQUERY__CREDENTIALS__PRIVATE_KEY"] = bq_private_key
    os.environ["DESTINATION__BIGQUERY__LOCATION"] = "asia-southeast1"

    # 3. PIPELINE
    pipeline = dlt.pipeline(
        pipeline_name="fb_ads_master_pipeline_final_v2", # Tăng version để reset schema cũ bị lỗi
        destination="bigquery",
        dataset_name="fb_ads_ahb1_report_v2"
    )

    all_sources = []
    # Khai báo bộ Fields chuẩn cho mọi bảng Insights để tránh lỗi "Missing fields"
    # Phải có đủ ID để sau này JOIN được với nhau
    standard_fields = ("campaign_id", "adset_id", "ad_id", "date_start", "spend", "impressions", "clicks", "account_id")

    for acc_id in fb_account_ids:
        # Source 1: Objects
        obj_source = facebook_ads_source(account_id=acc_id, access_token=fb_access_token) \
            .with_resources("campaigns", "ad_sets", "ads", "ad_creatives")
        for res in obj_source.resources.values():
            res.apply_hints(write_disposition="merge")
        all_sources.append(obj_source)

        # Source 2: Master
        master_ins = facebook_insights_source(
            account_id=acc_id, access_token=fb_access_token,
            initial_load_past_days=30, breakdowns="ads_insights",
            level="ad", fields=standard_fields
        )
        for res in master_ins.resources.values(): res.table_name = "facebook_insights"
        all_sources.append(master_ins)

        # Source 3: Age & Gender
        age_gender_ins = facebook_insights_source(
            account_id=acc_id, access_token=fb_access_token,
            initial_load_past_days=30, breakdowns="ads_insights_age_and_gender",
            level="ad", fields=standard_fields
        )
        for res in age_gender_ins.resources.values(): res.table_name = "insights_age_gender"
        all_sources.append(age_gender_ins)

        # Source 4: Region
        region_ins = facebook_insights_source(
            account_id=acc_id, access_token=fb_access_token,
            initial_load_past_days=30, breakdowns="ads_insights_region",
            level="ad", fields=standard_fields
        )
        for res in region_ins.resources.values(): res.table_name = "insights_region"
        all_sources.append(region_ins)

    if all_sources:
        print(pipeline.run(all_sources))

if __name__ == "__main__":
    load_facebook_data()
