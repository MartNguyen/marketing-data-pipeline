"""
Meta Ads Pipeline v15.2 - Schema Resilient
Fix: Dynamic Primary Keys to avoid BigQuery "Required Field" Error
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

# BỎ primary_key ở đây để tránh áp đặt lên tất cả các bảng
@dlt.resource(write_disposition="merge")
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
                'fb_eng_total': int(raw.get('inline_post_engagement', 0))
            }
            # ... (Phần actions giữ nguyên)
            yield row
    except Exception as e:
        logger.error(f"Error for Acc {account_id}: {e}")

def run_backfill():
    # ĐỔI TÊN PIPELINE ĐỂ XOÁ CACHED STATE (CỰC KỲ QUAN TRỌNG)
    pipeline = dlt.pipeline(
        pipeline_name="meta_v15_final_fix_v2", 
        destination="bigquery", 
        dataset_name="fb_ads_master_v4"
    )
    
    acc_ids = [a.strip() for a in os.environ.get("FB_ACCOUNT_ID", "").split(",") if a.strip()]
    token = os.environ.get("FB_ACCESS_TOKEN")
    curr_start = date(2025, 1, 1)
    end_point = date.today()

    # Define Primary Keys cho từng loại bảng
    base_pk = ["date", "fb_ad_id"]

    while curr_start <= end_point:
        s_str = curr_start.strftime('%Y-%m-%d')
        e_str = (curr_start + relativedelta(months=1) - relativedelta(days=1)).strftime('%Y-%m-%d')
        
        for acc_id in acc_ids:
            # Luồng 1: Performance (Chỉ cần Date + AdID)
            pipeline.run(fetch_meta_ultimate(acc_id, token, s_str, e_str), 
                         table_name="fact_fb_performance", primary_key=base_pk)
            
            # Luồng 7: Device Platform (Cần thêm Device Platform vào PK)
            pipeline.run(fetch_meta_ultimate(acc_id, token, s_str, e_str, ['device_platform']), 
                         table_name="fact_fb_device_platform", primary_key=base_pk + ["device_platform"])
            
            # ... (Các luồng khác tương tự, ní tự thêm primary_key tương ứng vào pipeline.run)
            
            time.sleep(10)
        curr_start += relativedelta(months=1)

if __name__ == "__main__":
    run_backfill()
