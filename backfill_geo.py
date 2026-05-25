import dlt
import os
import logging
import time
from datetime import date, datetime, timedelta
from facebook_business.api import FacebookAdsApi
from facebook_business.adobjects.adaccount import AdAccount
from facebook_business.exceptions import FacebookRequestError

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def fetch_meta_geo_chunk(account_id, access_token, start_date, end_date):
    FacebookAdsApi.init(access_token=access_token)
    acc = AdAccount(f'act_{str(account_id).replace("act_", "")}')
    
    fields = [
        "account_id", "campaign_id", "campaign_name", "objective",
        "adset_id", "adset_name", "ad_id", "ad_name",
        "date_start", "spend", "impressions", "clicks", "reach", "frequency",
        "inline_post_engagement", "actions"
    ]
    
    params = {
        'time_range': {'since': start_date, 'until': end_date},
        'level': 'ad',
        'time_increment': 1,
        'breakdowns': ['region']
    }

    try:
        insights = acc.get_insights(fields=fields, params=params)
        for entry in insights:
            raw = dict(entry)
            row = {
                'fb_account_id': raw.get('account_id'),
                'fb_campaign_name': raw.get('campaign_name'),
                'fb_objective': raw.get('objective'),
                'fb_adset_name': raw.get('adset_name'),
                'fb_ad_id': raw.get('ad_id'),
                'fb_ad_name': raw.get('ad_name'),
                'date': raw.get('date_start'),
                'region': raw.get('region', 'All'),
                
                # Bồi thêm 5 trường bắt buộc để khớp hoàn toàn Schema Lock cũ của BigQuery
                'age': 'All',
                'gender': 'All',
                'impression_device': 'All',
                'platform_position': 'All',
                'publisher_platform': 'All',
                'device_platform': 'All',

                'fb_spend': round(float(raw.get('spend', 0)), 2),
                'fb_impressions': int(raw.get('impressions', 0)),
                'fb_clicks': int(raw.get('clicks', 0)),
                'fb_reach': int(raw.get('reach', 0)),
                'fb_frequency': float(raw.get('frequency', 0)),
                'fb_eng_total': int(raw.get('inline_post_engagement', 0)),
                'fb_interaction': 0, 'fb_comment': 0, 'fb_share': 0, 'fb_save': 0,
                'fb_video_3s': 0, 'fb_purchase': 0, 'fb_lead': 0
            }

            if 'actions' in raw:
                for act in raw['actions']:
                    val = int(act.get('value', 0))
                    a_type = act.get('action_type')
                    if a_type == 'post_reaction': row['fb_interaction'] = val
                    elif a_type == 'comment': row['fb_comment'] = val
                    elif a_type == 'post': row['fb_share'] = val
                    elif a_type == 'onsite_conversion.post_save': row['fb_save'] = val
                    elif a_type == 'video_view': row['fb_video_3s'] = val
                    elif a_type == 'purchase': row['fb_purchase'] = val
                    elif a_type == 'lead': row['fb_lead'] = val
            yield row
    except FacebookRequestError as e:
        if e.api_error_code() == 4 or e.api_error_subcode() == 1504022:
            logger.warning(f"🚨 Dính giới hạn Rate Limit! Khóa mạch nghỉ 180s...")
            time.sleep(180)
            insights = acc.get_insights(fields=fields, params=params)
            # Vòng lặp dự phòng tương tự...
        else:
            raise e

def run_geo_backfill():
    os.environ["DESTINATION__BIGQUERY__LOCATION"] = "asia-southeast1"
    os.environ["DESTINATION__BIGQUERY__CREDENTIALS__PROJECT_ID"] = os.environ.get("GCP_PROJECT_ID")
    os.environ["DESTINATION__BIGQUERY__CREDENTIALS__CLIENT_EMAIL"] = os.environ.get("GCP_CLIENT_EMAIL")
    os.environ["DESTINATION__BIGQUERY__CREDENTIALS__PRIVATE_KEY"] = os.environ.get("GCP_PRIVATE_KEY", "").replace("\\n", "\n")

    pipeline = dlt.pipeline(pipeline_name="meta_geo_v2", destination="bigquery", dataset_name="fb_ads_master_v4")
    token = os.environ.get("FB_ACCESS_TOKEN")
    
    accounts = ["874972305237436", "779857487799415"]
    
    start_date = datetime.strptime("2026-01-01", "%Y-%m-%d")
    end_date = datetime.now()

    for acc_id in accounts:
        logger.info(f"🚀 Khởi động luồng kéo Địa lý băm nhỏ cho Account: {acc_id}")
        current_start = start_date
        
        while current_start < end_date:
            current_end = current_start + timedelta(days=7)
            if current_end > end_date:
                current_end = end_date
                
            s_str = current_start.strftime('%Y-%m-%d')
            e_str = current_end.strftime('%Y-%m-%d')
            
            logger.info(f"⏳ Đang bốc lốc dữ liệu tuần: {s_str} -> {e_str}")
            try:
                pipeline.run(
                    fetch_meta_geo_chunk(acc_id, token, s_str, e_str), 
                    table_name="fact_fb_geographic", 
                    primary_key=["date", "fb_ad_id", "region", "age", "gender"],
                    write_disposition="merge"
                )
                time.sleep(5)
            except Exception as e:
                logger.error(f"❌ Lỗi tại block {s_str}: {e}. Chờ 60s chuyển block kế tiếp.")
                time.sleep(60)
                
            current_start = current_end + timedelta(days=1)

if __name__ == "__main__":
    run_geo_backfill()
