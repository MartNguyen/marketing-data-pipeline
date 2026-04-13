import dlt
import os
import logging
from facebook_business.api import FacebookAdsApi
from facebook_business.adobjects.adaccount import AdAccount
from datetime import datetime, timedelta

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

@dlt.resource(write_disposition="replace")
def fetch_meta_parallel_breakdown(account_id, access_token, start_date, end_date):
    FacebookAdsApi.init(access_token=access_token)
    account = AdAccount(f'act_{account_id}')
    
    fields = [
        'ad_id', 'ad_name', 'adset_id', 'adset_name', 'campaign_id', 
        'date_start', 'spend', 'impressions', 'clicks', 'actions'
    ]
    
    # CHIẾN THUẬT: Chia để trị - Lấy từng trục một để né lỗi 400
    breakdown_types = ['age', 'gender', 'region']

    for b_type in breakdown_types:
        logger.info(f"🔍 Đang kéo trục: {b_type} cho Acc {account_id}")
        params = {
            'time_range': {'since': start_date, 'until': end_date},
            'level': 'ad',
            'time_increment': 1,
            'breakdowns': [b_type] # CHỈ lấy 1 trục mỗi lần gọi
        }

        try:
            insights = account.get_insights(fields=fields, params=params)
            for entry in insights:
                data = dict(entry)
                data['breakdown_dimension'] = b_type # Đánh dấu để phân biệt trong BigQuery
                
                # Bóc tách Metrics
                data['video_views_3s'] = 0.0
                data['thruplay_15s'] = 0.0
                if 'actions' in data:
                    for act in data['actions']:
                        val = float(act.get('value', 0))
                        if act['action_type'] == 'video_view': data['video_views_3s'] = val
                        elif act['action_type'] in ['video_thruplay_watched_actions', 'thruplay']: data['thruplay_15s'] = val
                yield data
        except Exception as e:
            logger.error(f"❌ Thất bại tại trục {b_type} của Acc {account_id}: {e}")

def run_deep_sync():
    # Credentials Mapping cho dlt (Giữ nguyên từ .yml của ní)
    os.environ["DESTINATION__BIGQUERY__CREDENTIALS__PROJECT_ID"] = os.environ.get("GCP_PROJECT_ID")
    os.environ["DESTINATION__BIGQUERY__CREDENTIALS__CLIENT_EMAIL"] = os.environ.get("GCP_CLIENT_EMAIL")
    os.environ["DESTINATION__BIGQUERY__CREDENTIALS__PRIVATE_KEY"] = os.environ.get("GCP_PRIVATE_KEY", "").replace("\\n", "\n")
    os.environ["DESTINATION__BIGQUERY__LOCATION"] = "asia-southeast1"

    pipeline = dlt.pipeline(
        pipeline_name="meta_parallel_breakdown",
        destination="bigquery",
        dataset_name="fb_ads_ahb1_report_v2"
    )
    
    token = os.environ.get("FB_ACCESS_TOKEN")
    acc_ids = [a.strip() for a in os.environ.get("FB_ACCOUNT_ID", "").split(",") if a.strip()]
    
    end_date = datetime.now().strftime('%Y-%m-%d')
    start_date = (datetime.now() - timedelta(days=7)).strftime('%Y-%m-%d')

    for acc_id in acc_ids:
        pipeline.run(
            fetch_meta_parallel_breakdown(acc_id, token, start_date, end_date),
            table_name="fb_ads_creative_breakdown_raw_metrics"
        )

if __name__ == "__main__":
    run_deep_sync()
