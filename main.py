"""
Meta Ads Pipeline v15.4 - Final 2026 Standard
Logic: Parallel Execution | Dynamic Timeline | Interaction & Retention Deep-Dive
Standard: Merge Strategy | Location: asia-southeast1
"""

import dlt
import os
import logging
import time
from datetime import date
from concurrent.futures import ThreadPoolExecutor
from dateutil.relativedelta import relativedelta
from facebook_business.api import FacebookAdsApi
from facebook_business.adobjects.adaccount import AdAccount
from facebook_business.exceptions import FacebookRequestError

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

@dlt.resource(write_disposition="merge")
def fetch_meta_ultimate(account_id, access_token, start_date, end_date, breakdown=None):
    FacebookAdsApi.init(access_token=access_token)
    acc = AdAccount(f'act_{str(account_id).replace("act_", "")}')
    
    fields = [
        "account_id", "campaign_id", "campaign_name", "objective",
        "adset_id", "adset_name", "ad_id", "ad_name", 
        "date_start", "spend", "impressions", "clicks", "reach", "frequency",
        "inline_post_engagement", "actions", "video_avg_time_watched_actions", "video_play_actions"
    ]
    
    params = {
        'time_range': {'since': start_date, 'until': end_date},
        'level': 'ad',
        'time_increment': 1,
        'breakdowns': breakdown if breakdown else []
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
                # Standard Dimensions
                'age': raw.get('age', 'All'),
                'gender': raw.get('gender', 'All'),
                'region': raw.get('region', 'All'),
                'publisher_platform': raw.get('publisher_platform', 'All'),
                'platform_position': raw.get('platform_position', 'All'),
                'impression_device': raw.get('impression_device', 'All'),
                'device_platform': raw.get('device_platform', 'All'),
                # Basic Metrics
                'fb_spend': round(float(raw.get('spend', 0)), 2),
                'fb_impressions': int(raw.get('impressions', 0)),
                'fb_clicks': int(raw.get('clicks', 0)),
                'fb_reach': int(raw.get('reach', 0)),
                'fb_frequency': float(raw.get('frequency', 0)),
                'fb_eng_total': int(raw.get('inline_post_engagement', 0)),
                # Interaction & Video Metrics (Initiate)
                'fb_interaction': 0, 'fb_comment': 0, 'fb_share': 0, 'fb_save': 0,
                'fb_video_2s': 0, 'fb_video_3s': 0, 'fb_thruplay': 0,
                'fb_video_avg_time': 0, 'fb_video_plays': 0,
                'fb_purchase': 0, 'fb_lead': 0
            }

            # Map Retention Metrics (Avg Time & Plays)
            if 'video_avg_time_watched_actions' in raw:
                for v_avg in raw['video_avg_time_watched_actions']:
                    row['fb_video_avg_time'] = int(v_avg.get('value', 0))
            if 'video_play_actions' in raw:
                for v_play in raw['video_play_actions']:
                    row['fb_video_plays'] = int(v_play.get('value', 0))

            # Map Actions (Interactions & Video Views)
            if 'actions' in raw:
                for act in raw['actions']:
                    val = int(act.get('value', 0))
                    a_type = act.get('action_type')
                    # Engagement bóc tách
                    if a_type == 'post_reaction': row['fb_interaction'] = val
                    elif a_type == 'comment': row['fb_comment'] = val
                    elif a_type == 'post': row['fb_share'] = val
                    elif a_type == 'onsite_conversion.post_save': row['fb_save'] = val
                    # Video View depth
                    elif a_type == 'video_view': row['fb_video_3s'] = val
                    elif a_type == 'video_2_sec_continuous_video_view': row['fb_video_2s'] = val
                    elif a_type in ['thruplay', 'video_thruplay_watched_actions']: row['fb_thruplay'] = val
                    # Lower Funnel
                    elif a_type == 'purchase': row['fb_purchase'] = val
                    elif a_type == 'lead': row['fb_lead'] = val
            yield row
    except FacebookRequestError as e:
        if e.api_error_code() == 4:
            logger.warning(f"🚨 Rate Limit for {account_id}! Sleeping 120s...")
            time.sleep(120)
        raise e

def sync_account_worker(acc_id, token, s_str, e_str, pipeline):
    """Hàm chạy 7 luồng dữ liệu song song cho 1 account"""
    base_pk = ["date", "fb_ad_id"]
    try:
        logger.info(f"⏳ Syncing Account: {acc_id} | {s_str} -> {e_str}")
        pipeline.run(fetch_meta_ultimate(acc_id, token, s_str, e_str), table_name="fact_fb_performance", primary_key=base_pk)
        pipeline.run(fetch_meta_ultimate(acc_id, token, s_str, e_str, ['age', 'gender']), table_name="fact_fb_demographic", primary_key=base_pk + ["age", "gender"])
        pipeline.run(fetch_meta_ultimate(acc_id, token, s_str, e_str, ['publisher_platform']), table_name="fact_fb_platform", primary_key=base_pk + ["publisher_platform"])
        pipeline.run(fetch_meta_ultimate(acc_id, token, s_str, e_str, ['region']), table_name="fact_fb_geographic", primary_key=base_pk + ["region"])
        pipeline.run(fetch_meta_ultimate(acc_id, token, s_str, e_str, ['publisher_platform', 'platform_position']), table_name="fact_fb_placement_detail", primary_key=base_pk + ["publisher_platform", "platform_position"])
        pipeline.run(fetch_meta_ultimate(acc_id, token, s_str, e_str, ['impression_device']), table_name="fact_fb_device_detail", primary_key=base_pk + ["impression_device"])
        pipeline.run(fetch_meta_ultimate(acc_id, token, s_str, e_str, ['device_platform']), table_name="fact_fb_device_platform", primary_key=base_pk + ["device_platform"])
    except Exception as e:
        logger.error(f"❌ Failed worker for {acc_id}: {e}")

def run_pipeline():
    # Setup Credentials cho BigQuery asia-southeast1
    os.environ["DESTINATION__BIGQUERY__LOCATION"] = "asia-southeast1"
    os.environ["DESTINATION__BIGQUERY__CREDENTIALS__PROJECT_ID"] = os.environ.get("GCP_PROJECT_ID")
    os.environ["DESTINATION__BIGQUERY__CREDENTIALS__CLIENT_EMAIL"] = os.environ.get("GCP_CLIENT_EMAIL")
    os.environ["DESTINATION__BIGQUERY__CREDENTIALS__PRIVATE_KEY"] = os.environ.get("GCP_PRIVATE_KEY", "").replace("\\n", "\n")

    pipeline = dlt.pipeline(pipeline_name="meta_ultimate_v15_4", destination="bigquery", dataset_name="fb_ads_master_v4")
    token = os.environ.get("FB_ACCESS_TOKEN")

    # Cấu hình nhóm Account theo Timeline
    account_groups = {
        "2025": ["587898528769829"], 
        "2026": ["874972305237436", "779857487799415"] 
    }

    today = date.today()

    for year_str, ids in account_groups.items():
        curr_start = date(int(year_str), 1, 1)
        while curr_start <= today:
            curr_end = curr_start + relativedelta(months=1) - relativedelta(days=1)
            if curr_end > today: curr_end = today
            s_str, e_str = curr_start.strftime('%Y-%m-%d'), curr_end.strftime('%Y-%m-%d')
            
            # Thực thi song song (Max 3 workers để an toàn cho Token)
            with ThreadPoolExecutor(max_workers=3) as executor:
                for acc_id in ids:
                    executor.submit(sync_account_worker, acc_id, token, s_str, e_str, pipeline)
            
            curr_start += relativedelta(months=1)

if __name__ == "__main__":
    run_pipeline()
