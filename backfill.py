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

def fetch_meta_ultimate(account_id, access_token, start_date, end_date, breakdown=None):
    FacebookAdsApi.init(access_token=access_token)
    acc = AdAccount(f'act_{str(account_id).replace("act_", "")}')
    
    fields = [
        "account_id", "campaign_id", "campaign_name", "objective",
        "adset_id", "adset_name", "ad_id", "ad_name", 
        "date_start", "spend", "impressions", "clicks", "reach", "frequency",
        "inline_post_engagement", "actions", "video_avg_time_watched_actions", "video_play_actions"
    ]
    
    has_breakdown = breakdown is not None and len(breakdown) > 0
    
    params = {
        'time_range': {'since': start_date, 'until': end_date},
        'level': 'ad',
        'time_increment': 1,
        'breakdowns': breakdown if has_breakdown else []
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
                'fb_spend': round(float(raw.get('spend', 0)), 2),
                'fb_impressions': int(raw.get('impressions', 0)),
                'fb_clicks': int(raw.get('clicks', 0)),
                'fb_reach': int(raw.get('reach', 0)),
                'fb_frequency': float(raw.get('frequency', 0)),
                'fb_eng_total': int(raw.get('inline_post_engagement', 0)),
                'fb_interaction': 0, 'fb_comment': 0, 'fb_share': 0, 'fb_save': 0,
                'fb_video_2s': 0, 'fb_video_3s': 0, 'fb_thruplay': 0,
                'fb_video_avg_time': 0, 'fb_video_plays': 0,
                'fb_purchase': 0, 'fb_lead': 0,
                # Đắp cứng giá trị 'All' để khớp khít cấu trúc REQUIRED cũ của BigQuery, bảo vệ an toàn data
                'age': 'All', 'gender': 'All', 'region': 'All', 
                'publisher_platform': 'All', 'platform_position': 'All', 
                'impression_device': 'All', 'device_platform': 'All'
            }

            if has_breakdown:
                for b_field in breakdown:
                    row[b_field] = raw.get(b_field, 'Unknown')

            if 'video_avg_time_watched_actions' in raw:
                for v_avg in raw['video_avg_time_watched_actions']:
                    row['fb_video_avg_time'] = int(v_avg.get('value', 0))
            if 'video_play_actions' in raw:
                for v_play in raw['video_play_actions']:
                    row['fb_video_plays'] = int(v_play.get('value', 0))

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
                    elif a_type == 'purchase': row['fb_purchase'] = val
                    elif a_type == 'lead': row['fb_lead'] = val
            yield row
    except FacebookRequestError as e:
        if e.api_error_code() == 4:
            logger.warning(f"🚨 Rate Limit detected! Sleeping 120s...")
            time.sleep(120)
        raise e

def sync_account_worker(acc_id, token, s_str, e_str, pipeline):
    base_pk = ["date", "fb_ad_id"]
    logger.info(f"⏳ Syncing Account: {acc_id} | Timeline: {s_str} -> {e_str}")
    
    # Ép cấu hình primary_key bảng phụ khớp để tránh dlt ghi lộn xộn
    pipeline.run(fetch_meta_ultimate(acc_id, token, s_str, e_str), table_name="fact_fb_performance", write_disposition="merge", primary_key=base_pk)
    pipeline.run(fetch_meta_ultimate(acc_id, token, s_str, e_str, ['age', 'gender']), table_name="fact_fb_demographic", write_disposition="merge", primary_key=base_pk + ["age", "gender"])
    pipeline.run(fetch_meta_ultimate(acc_id, token, s_str, e_str, ['publisher_platform']), table_name="fact_fb_platform", write_disposition="merge", primary_key=base_pk + ["publisher_platform"])
    pipeline.run(fetch_meta_ultimate(acc_id, token, s_str, e_str, ['region']), table_name="fact_fb_geographic", write_disposition="merge", primary_key=base_pk + ["region"])
    pipeline.run(fetch_meta_ultimate(acc_id, token, s_str, e_str, ['publisher_platform', 'platform_position']), table_name="fact_fb_placement_detail", write_disposition="merge", primary_key=base_pk + ["publisher_platform", "platform_position"])
    pipeline.run(fetch_meta_ultimate(acc_id, token, s_str, e_str, ['impression_device']), table_name="fact_fb_device_detail", write_disposition="merge", primary_key=base_pk + ["impression_device"])
    pipeline.run(fetch_meta_ultimate(acc_id, token, s_str, e_str, ['device_platform']), table_name="fact_fb_device_platform", write_disposition="merge", primary_key=base_pk + ["device_platform"])

def run_pipeline():
    os.environ["DESTINATION__BIGQUERY__LOCATION"] = "asia-southeast1"
    os.environ["DESTINATION__BIGQUERY__CREDENTIALS__PROJECT_ID"] = os.environ.get("GCP_PROJECT_ID")
    os.environ["DESTINATION__BIGQUERY__CREDENTIALS__CLIENT_EMAIL"] = os.environ.get("GCP_CLIENT_EMAIL")
    os.environ["DESTINATION__BIGQUERY__CREDENTIALS__PRIVATE_KEY"] = os.environ.get("GCP_PRIVATE_KEY", "").replace("\\n", "\n")

    # Giữ nguyên tên pipeline để nó khớp checkpoint cũ, nạp nối tiếp/merge an toàn dữ liệu
    pipeline = dlt.pipeline(
        pipeline_name="meta_ultimate_v15_4_refresh",
        destination="bigquery", 
        dataset_name="fb_ads_master_v4"
    )
    token = os.environ.get("FB_ACCESS_TOKEN")

    account_groups = {
        "2026": {"ids": ["874972305237436", "779857487799415"], "start": "2026-01-01", "end": date.today().strftime('%Y-%m-%d')}
    }

    for group_year, config in account_groups.items():
        s_str, e_str = config["start"], config["end"]
        for acc_id in config["ids"]:
            try:
                sync_account_worker(acc_id, token, s_str, e_str, pipeline)
            except Exception as e:
                logger.error(f"❌ Gãy pipeline tại Account {acc_id}: {e}")
                continue

if __name__ == "__main__":
    run_pipeline()
