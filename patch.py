"""
Meta Ads Pipeline v15.2 - Patch & Repair
Focus: Fix Missing Months | Rate Limit Handling | Merge Strategy
Dataset: fb_ads_master_v4 | Location: asia-southeast1
"""

import dlt
import os
import logging
import time
from datetime import date
from facebook_business.api import FacebookAdsApi
from facebook_business.adobjects.adaccount import AdAccount
from facebook_business.exceptions import FacebookRequestError

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

@dlt.resource(
    write_disposition="merge", 
    primary_key=["date", "fb_ad_id", "age", "gender", "region", "publisher_platform", "platform_position", "impression_device", "device_platform"]
)
def fetch_meta_ultimate(account_id, access_token, start_date, end_date, breakdown=None):
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
                'age': raw.get('age', 'All'),
                'gender': raw.get('gender', 'All'),
                'region': raw.get('region', 'All'),
                'publisher_platform': raw.get('publisher_platform', 'All'),
                'platform_position': raw.get('platform_position', 'All'),
                'impression_device': raw.get('impression_device', 'All'),
                'device_platform': raw.get('device_platform', 'All'),
                'fb_spend': round(float(raw.get('spend', 0)), 2),
                'fb_impressions': int(raw.get('impressions', 0)),
                'fb_clicks': int(raw.get('clicks', 0)),
                'fb_reach': int(raw.get('reach', 0)),
                'fb_frequency': float(raw.get('frequency', 0)),
                'fb_eng_total': int(raw.get('inline_post_engagement', 0)),
                'fb_video_3s': 0, 'fb_thruplay': 0, 'fb_eng_granular': 0, 
                'fb_purchase': 0, 'fb_lead': 0, 'fb_registration': 0
            }
            if 'actions' in raw:
                for act in raw['actions']:
                    val = int(act.get('value', 0))
                    a_type = act.get('action_type')
                    if a_type == 'video_view': row['fb_video_3s'] = val
                    elif a_type in ['thruplay', 'video_thruplay_watched_actions']: row['fb_thruplay'] = val
                    elif a_type in ['post_reaction', 'comment', 'post']: row['fb_eng_granular'] += val
                    elif a_type == 'purchase': row['fb_purchase'] = val
                    elif a_type == 'lead': row['fb_lead'] = val
                    elif a_type == 'complete_registration': row['fb_registration'] = val
            yield row
    except FacebookRequestError as e:
        if e.api_error_code() == 4: # Rate limit
            logger.warning("🚨 Meta Rate Limit Hit! Sleeping for 120s...")
            time.sleep(120)
            # Re-raise to let dlt or the loop handle retry if needed
        raise e

def run_patch():
    os.environ["DESTINATION__BIGQUERY__LOCATION"] = "asia-southeast1"
    os.environ["DESTINATION__BIGQUERY__CREDENTIALS__PROJECT_ID"] = os.environ.get("GCP_PROJECT_ID")
    os.environ["DESTINATION__BIGQUERY__CREDENTIALS__CLIENT_EMAIL"] = os.environ.get("GCP_CLIENT_EMAIL")
    os.environ["DESTINATION__BIGQUERY__CREDENTIALS__PRIVATE_KEY"] = os.environ.get("GCP_PRIVATE_KEY", "").replace("\\n", "\n")

    pipeline = dlt.pipeline(
        pipeline_name="meta_v15_patch_fix", 
        destination="bigquery", 
        dataset_name="fb_ads_master_v4"
    )
    
    token = os.environ.get("FB_ACCESS_TOKEN")

    # --- DANH SÁCH CÁC THÁNG BỊ THIẾU CẦN VÁ ---
    # Cấu trúc: (account_id, start_date, end_date)
    missing_tasks = [
        ("587898528769829", "2025-03-01", "2025-03-31"),
        ("587898528769829", "2025-06-01", "2025-06-30"),
        ("587898528769829", "2025-10-01", "2025-10-31"),
        ("874972305237436", "2025-05-01", "2025-05-31")
    ]

    tables = [
        ("fact_fb_performance", None),
        ("fact_fb_demographic", ['age', 'gender']),
        ("fact_fb_platform", ['publisher_platform']),
        ("fact_fb_geographic", ['region']),
        ("fact_fb_placement_detail", ['publisher_platform', 'platform_position']),
        ("fact_fb_device_detail", ['impression_device']),
        ("fact_fb_device_platform", ['device_platform'])
    ]

    for acc_id, s_str, e_str in missing_tasks:
        logger.info(f"🛠 Patching {acc_id} | {s_str} to {e_str}")
        for table_name, breakdown in tables:
            try:
                logger.info(f"  --> Loading {table_name}")
                pipeline.run(
                    fetch_meta_ultimate(acc_id, token, s_str, e_str, breakdown), 
                    table_name=table_name
                )
                time.sleep(8) # Nghỉ 8s giữa mỗi table để tránh bị vịn
            except Exception as e:
                logger.error(f"❌ Failed table {table_name}: {e}")
                time.sleep(30) # Lỗi thì nghỉ lâu xíu rồi qua table khác

if __name__ == "__main__":
    run_patch()
