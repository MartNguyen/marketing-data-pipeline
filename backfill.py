import dlt
import os
import logging
import time
from datetime import date
from facebook_business.api import FacebookAdsApi
from facebook_business.adobjects.adaccount import AdAccount
from facebook_business.exceptions import FacebookRequestError

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(message)s')
logger = logging.getLogger(__name__)

def fetch_meta_fast_backfill(account_id, access_token, start_date, end_date):
    FacebookAdsApi.init(access_token=access_token)
    acc = AdAccount(f'act_{str(account_id).replace("act_", "")}')
    
    fields = [
        "account_id", "ad_id", "date_start", 
        "inline_post_engagement", "actions"
    ]
    
    params = {
        'time_range': {'since': start_date, 'until': end_date},
        'level': 'ad',
        'time_increment': 1,
        'breakdowns': [] 
    }

    try:
        insights = acc.get_insights(fields=fields, params=params)
        for entry in insights:
            raw = dict(entry)
            row = {
                'fb_account_id': raw.get('account_id'),
                'fb_ad_id': raw.get('ad_id'),
                'date': raw.get('date_start'),
                
                'fb_eng_total': int(raw.get('inline_post_engagement', 0)), 
                'fb_interaction': 0, 'fb_comment': 0, 'fb_share': 0, 'fb_save': 0,
                'fb_video_2s': 0, 'fb_video_3s': 0, 'fb_thruplay': 0
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
                    elif a_type == 'video_2_sec_continuous_video_view': row['fb_video_2s'] = val
                    elif a_type in ['thruplay', 'video_thruplay_watched_actions']: row['fb_thruplay'] = val

            yield row
            
    except FacebookRequestError as e:
        logger.error(f"FB API Error on {account_id}: {e}")
        raise e

def run_fast_backfill():
    # Setup BigQuery Env
    os.environ["DESTINATION__BIGQUERY__LOCATION"] = "asia-southeast1"
    os.environ["DESTINATION__BIGQUERY__CREDENTIALS__PROJECT_ID"] = os.environ.get("GCP_PROJECT_ID")
    os.environ["DESTINATION__BIGQUERY__CREDENTIALS__CLIENT_EMAIL"] = os.environ.get("GCP_CLIENT_EMAIL")
    # Thay thế \n thành newline chuẩn cho Private Key
    private_key = os.environ.get("GCP_PRIVATE_KEY", "")
    if "\\n" in private_key:
        private_key = private_key.replace("\\n", "\n")
    os.environ["DESTINATION__BIGQUERY__CREDENTIALS__PRIVATE_KEY"] = private_key

    token = os.environ.get("FB_ACCESS_TOKEN")
    if not token:
        logger.error("LỖI CHẾT NGƯỜI: Không tìm thấy FB_ACCESS_TOKEN trong biến môi trường!")
        return

    pipeline = dlt.pipeline(pipeline_name="meta_backfill", destination="bigquery", dataset_name="fb_ads_master_v4")
    
    ids = ["874972305237436", "779857487799415"] 
    s_str, e_str = '2026-01-01', date.today().strftime('%Y-%m-%d')
    
    logger.info(f"Bat dau Backfill từ {s_str} den {e_str}...")
    
    for acc_id in ids:
        logger.info(f"-> Đang xử lý Account: {acc_id}")
        try:
            info = pipeline.run(
                fetch_meta_fast_backfill(acc_id, token, s_str, e_str), 
                table_name="temp_fact_fb_backfill",
                write_disposition="append" 
            )
            logger.info(f"✅ Xong Account {acc_id}. Load info:\n{info}")
        except Exception as e:
            logger.error(f"❌ Lỗi khi chạy pipeline cho account {acc_id}: {str(e)}")

if __name__ == "__main__":
    run_fast_backfill()
