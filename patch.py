"""
Meta Ads Pipeline v15.1
Changes: Added Stream 7 (Device Platform) | Explicit Objective Mapping
Target: Dataset v4 | Location: asia-southeast1
"""

import dlt
import os
import logging
import time
from datetime import date
from dateutil.relativedelta import relativedelta
from facebook_business.api import FacebookAdsApi
from facebook_business.adobjects.adaccount import AdAccount

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
        "account_id", "campaign_id", "campaign_name", "objective", # Objective nằm đây
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
                'fb_objective': raw.get('objective'), # Đây là "Ad Format" theo ý ní
                'fb_adset_name': raw.get('adset_name'),
                'fb_ad_id': raw.get('ad_id'),
                'fb_ad_name': raw.get('ad_name'),
                'date': raw.get('date_start'),
                # Breakdowns
                'age': raw.get('age', 'All'),
                'gender': raw.get('gender', 'All'),
                'region': raw.get('region', 'All'),
                'publisher_platform': raw.get('publisher_platform', 'All'),
                'platform_position': raw.get('platform_position', 'All'),
                'impression_device': raw.get('impression_device', 'All'),
                'device_platform': raw.get('device_platform', 'All'), # Luồng 7
                # Metrics
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
    except Exception as e:
        logger.error(f"Error for Acc {account_id}: {e}")

def run_backfill():
    os.environ["DESTINATION__BIGQUERY__LOCATION"] = "asia-southeast1"
    os.environ["DESTINATION__BIGQUERY__CREDENTIALS__PROJECT_ID"] = os.environ.get("GCP_PROJECT_ID")
    os.environ["DESTINATION__BIGQUERY__CREDENTIALS__CLIENT_EMAIL"] = os.environ.get("GCP_CLIENT_EMAIL")
    
    p_key = os.environ.get("GCP_PRIVATE_KEY", "")
    if "\\n" in p_key:
        p_key = p_key.replace("\\n", "\n")
    os.environ["DESTINATION__BIGQUERY__CREDENTIALS__PRIVATE_KEY"] = p_key

    pipeline = dlt.pipeline(
        pipeline_name="meta_v15_1_backfill", 
        destination="bigquery", 
        dataset_name="fb_ads_master_v4"
    )
    
    acc_ids = [a.strip() for a in os.environ.get("FB_ACCOUNT_ID", "").split(",") if a.strip()]
    token = os.environ.get("FB_ACCESS_TOKEN")
    curr_start = date(2025, 1, 1) # Ní có thể chỉnh lại thành date(2026, 4, 1) nếu chỉ muốn chạy bù tháng này
    end_point = date.today()

    while curr_start <= end_point:
        curr_end = curr_start + relativedelta(months=1) - relativedelta(days=1)
        if curr_end > end_point: curr_end = end_point
        s_str, e_str = curr_start.strftime('%Y-%m-%d'), curr_end.strftime('%Y-%m-%d')
        
        for acc_id in acc_ids:
            logger.info(f"🚀 Processing {acc_id} | {s_str} to {e_str}")
            # Luồng 1-4: Standard
            pipeline.run(fetch_meta_ultimate(acc_id, token, s_str, e_str), table_name="fact_fb_performance")
            pipeline.run(fetch_meta_ultimate(acc_id, token, s_str, e_str, ['age', 'gender']), table_name="fact_fb_demographic")
            pipeline.run(fetch_meta_ultimate(acc_id, token, s_str, e_str, ['publisher_platform']), table_name="fact_fb_platform")
            pipeline.run(fetch_meta_ultimate(acc_id, token, s_str, e_str, ['region']), table_name="fact_fb_geographic")
            
            # Luồng 5-6: Placements & Devices
            pipeline.run(fetch_meta_ultimate(acc_id, token, s_str, e_str, ['publisher_platform', 'platform_position']), table_name="fact_fb_placement_detail")
            pipeline.run(fetch_meta_ultimate(acc_id, token, s_str, e_str, ['impression_device']), table_name="fact_fb_device_detail")
            
            # LUỒNG 7: DEVICE PLATFORM (NEW)
            pipeline.run(fetch_meta_ultimate(acc_id, token, s_str, e_str, ['device_platform']), table_name="fact_fb_device_platform")
            
            time.sleep(10) 
        
        curr_start += relativedelta(months=1)

if __name__ == "__main__":
    run_backfill()
