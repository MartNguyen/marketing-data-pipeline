"""
Meta Ads Master Pipeline (v12.0)
Standard: Production Ready | Star Schema | Automation Friendly
Logic: Rolling 7-day window | Explicit Mapping | fb_ Prefix
"""

import dlt
import os
import logging
import time
from datetime import datetime, timedelta
from facebook_business.api import FacebookAdsApi
from facebook_business.adobjects.adaccount import AdAccount

# Logging setup
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

@dlt.resource(write_disposition="append")
def fetch_meta_master(account_id, access_token, start_date, end_date, breakdown=None):
    """Extract, flatten, and normalize Meta insights."""
    FacebookAdsApi.init(access_token=access_token)
    clean_acc_id = str(account_id).replace('act_', '')
    account = AdAccount(f'act_{clean_acc_id}')
    
    fields = [
        "account_id", "campaign_id", "campaign_name", "adset_id", "adset_name", 
        "ad_id", "ad_name", "date_start", "spend", "impressions", "clicks", 
        "inline_post_engagement", "actions"
    ]
    
    params = {
        'time_range': {'since': start_date, 'until': end_date},
        'level': 'ad',
        'time_increment': 1,
        'breakdowns': breakdown if breakdown else []
    }

    try:
        insights = account.get_insights(fields=fields, params=params)
        for entry in insights:
            raw = dict(entry)
            
            # Explicit mapping & fb_ prefixing
            row = {
                'fb_account_id': clean_acc_id,
                'fb_campaign_id': raw.get('campaign_id'),
                'fb_campaign_name': raw.get('campaign_name'),
                'fb_adset_id': raw.get('adset_id'),
                'fb_adset_name': raw.get('adset_name'),
                'fb_ad_id': raw.get('ad_id'),
                'fb_ad_name': raw.get('ad_name'),
                'date': raw.get('date_start'),
                'age': raw.get('age', 'All'),
                'gender': raw.get('gender', 'All'),
                'region': raw.get('region', 'All'),
                'fb_spend': round(float(raw.get('spend', 0)), 2),
                'fb_impressions': int(raw.get('impressions', 0)),
                'fb_clicks': int(raw.get('clicks', 0)),
                'fb_engagement_total': int(raw.get('inline_post_engagement', 0))
            }

            # Flatten actions into metrics
            row.update({'fb_video_3s': 0, 'fb_thruplay_15s': 0, 'fb_engagement_granular': 0})
            if 'actions' in raw:
                for act in raw['actions']:
                    val = int(act.get('value', 0))
                    a_type = act.get('action_type')
                    if a_type == 'video_view': row['fb_video_3s'] = val
                    elif a_type in ['thruplay', 'video_thruplay_watched_actions']: row['fb_thruplay_15s'] = val
                    elif a_type in ['post_reaction', 'comment', 'post']: row['fb_engagement_granular'] += val
            
            yield row
    except Exception as e:
        logger.error(f"Acc {account_id} Error: {e}")

def run_sync():
    """Execute pipeline: Master, Demographic, and Geographic layers."""
    os.environ["DESTINATION__BIGQUERY__LOCATION"] = "asia-southeast1"
    
    pipeline = dlt.pipeline(
        pipeline_name="meta_master_v12",
        destination="bigquery",
        dataset_name="fb_ads_ahb_master_v3"
    )

    token = os.environ.get("FB_ACCESS_TOKEN")
    acc_ids = [a.strip() for a in os.environ.get("FB_ACCOUNT_ID", "").split(",") if a.strip()]
    
    end_date = datetime.now().strftime('%Y-%m-%d')
    start_date = (datetime.now() - timedelta(days=7)).strftime('%Y-%m-%d')

    for acc_id in acc_ids:
        # 1. Master Data
        pipeline.run(fetch_meta_master(acc_id, token, start_date, end_date), 
                     table_name="fact_fb_performance")
        
        # 2. Age/Gender Breakdown
        pipeline.run(fetch_meta_master(acc_id, token, start_date, end_date, ['age', 'gender']), 
                     table_name="fact_fb_demographic")
        
        # 3. Region Breakdown
        pipeline.run(fetch_meta_master(acc_id, token, start_date, end_date, ['region']), 
                     table_name="fact_fb_geographic")
        
        logger.info(f"✅ Sync complete: {acc_id}")
        time.sleep(5)

if __name__ == "__main__":
    run_sync()
