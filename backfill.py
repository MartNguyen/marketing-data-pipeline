import dlt
import os
import logging
from datetime import date
from concurrent.futures import ThreadPoolExecutor
from facebook_business.api import FacebookAdsApi
from facebook_business.adobjects.adaccount import AdAccount
from facebook_business.exceptions import FacebookRequestError

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(message)s')
logger = logging.getLogger(__name__)

# [FIX] Cập nhật lại logic bóc tách VÀ tính tổng như ní yêu cầu
def fetch_meta_fast_backfill(account_id, access_token, start_date, end_date):
    FacebookAdsApi.init(access_token=access_token)
    acc = AdAccount(f'act_{str(account_id).replace("act_", "")}')
    
    fields = [
        "account_id", "ad_id", "date_start", 
        "inline_post_engagement", "actions" # Chỉ lấy những cột cần thiết cho việc Backfill
    ]
    
    params = {
        'time_range': {'since': start_date, 'until': end_date},
        'level': 'ad',
        'time_increment': 1, # Vẫn phải để daily để map đúng ngày trong BQ
        'breakdowns': []     # Bỏ qua breakdown để chạy cực nhanh
    }

    try:
        insights = acc.get_insights(fields=fields, params=params)
        for entry in insights:
            raw = dict(entry)
            row = {
                'fb_account_id': raw.get('account_id'),
                'fb_ad_id': raw.get('ad_id'),
                'date': raw.get('date_start'),
                
                # Cột Gốc để đối soát
                'fb_eng_total': int(raw.get('inline_post_engagement', 0)), 
                
                # Khởi tạo mặc định
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
            
    except Exception as e:
        logger.error(f"Error on {account_id}: {e}")

def run_fast_backfill():
    os.environ["DESTINATION__BIGQUERY__LOCATION"] = "asia-southeast1"
    # (Nhớ set thêm GCP_PROJECT_ID, Client Email, Private Key như script cũ)
    
    # [QUAN TRỌNG] Trỏ vào đúng Dataset và đổi tên Table để chứa data sửa sai
    pipeline = dlt.pipeline(pipeline_name="meta_backfill", destination="bigquery", dataset_name="fb_ads_master_v4")
    token = os.environ.get("FB_ACCESS_TOKEN")

    # Chỉ chạy Backfill cho Account đang chạy năm 2026 và CHỈ lấy 2 tháng gần nhất để siêu nhanh
    ids = ["874972305237436", "779857487799415"] 
    s_str, e_str = '2026-01-01', date.today().strftime('%Y-%m-%d')
    
    logger.info(f"Bat dau Backfill siêu tốc từ {s_str} den {e_str}...")
    
    with ThreadPoolExecutor(max_workers=2) as executor:
        for acc_id in ids:
            executor.submit(
                pipeline.run, 
                fetch_meta_fast_backfill(acc_id, token, s_str, e_str), 
                table_name="temp_fact_fb_backfill", # Bắn vào bảng tạm
                write_disposition="replace"
            )
            
    logger.info("Done Backfill! Gio chay Update SQL tren BQ.")

if __name__ == "__main__":
    run_fast_backfill()
