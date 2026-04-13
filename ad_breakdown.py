import dlt
import os
import logging
from datetime import datetime, timedelta
from facebook_business.api import FacebookAdsApi
from facebook_business.adobjects.adaccount import AdAccount

# Setup Diagnostic Logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

@dlt.resource(write_disposition="replace")
def fetch_meta_deep_breakdown(account_id, access_token, start_date, end_date):
    """
    Fetch raw data at Ad Level with 3-way breakdown (Age, Gender, Region).
    Includes Video (3s, 15s) and Engagement metrics.
    """
    FacebookAdsApi.init(access_token=access_token)
    account = AdAccount(f'act_{account_id}')
    
    # Granularity: Ad + Ad Set + Campaign
    fields = [
        'ad_id', 'ad_name', 'adset_id', 'adset_name', 'campaign_id', 
        'date_start', 'spend', 'impressions', 'clicks', 'actions'
    ]
    
    params = {
        'time_range': {'since': start_date, 'until': end_date},
        'level': 'ad',
        'time_increment': 1,
        'breakdowns': ['age', 'gender', 'region']
    }

    try:
        insights = account.get_insights(fields=fields, params=params)
        for entry in insights:
            data = dict(entry)
            
            # Khởi tạo mặc định để tránh lỗi Schema tại BigQuery
            data['video_views_3s'] = 0.0
            data['video_thruplay_15s'] = 0.0
            data['post_engagement_total'] = 0.0
            
            if 'actions' in data:
                for act in data['actions']:
                    a_type = act.get('action_type')
                    val = float(act.get('value', 0))
                    
                    if a_type == 'video_view':
                        data['video_views_3s'] = val
                    elif a_type in ['video_thruplay_watched_actions', 'thruplay']:
                        data['video_thruplay_15s'] = val
                    elif a_type == 'post_engagement':
                        data['post_engagement_total'] = val
            
            yield data
            
    except Exception as e:
        logger.error(f"❌ API Error | Acc: {account_id} | Breakdown Matrix: {e}")

def run_deep_sync():
    """
    Pipeline Runner: Mapping GitHub Secrets to DLT Destination Config
    """
    # Mapping biến từ file .yml sang chuẩn DLT cho BigQuery
    os.environ["DESTINATION__BIGQUERY__CREDENTIALS__PROJECT_ID"] = os.environ.get("GCP_PROJECT_ID")
    os.environ["DESTINATION__BIGQUERY__CREDENTIALS__CLIENT_EMAIL"] = os.environ.get("GCP_CLIENT_EMAIL")
    os.environ["DESTINATION__BIGQUERY__CREDENTIALS__PRIVATE_KEY"] = os.environ.get("GCP_PRIVATE_KEY", "").replace("\\n", "\n")
    os.environ["DESTINATION__BIGQUERY__LOCATION"] = "asia-southeast1"

    pipeline = dlt.pipeline(
        pipeline_name="meta_deep_breakdown_v2",
        destination="bigquery",
        dataset_name="fb_ads_ahb1_report_v2" # Lưu chung dataset với Main để dễ JOIN
    )
    
    token = os.environ.get("FB_ACCESS_TOKEN")
    # Tách chuỗi Account IDs từ Secret
    acc_ids = [a.strip() for a in os.environ.get("FB_ACCOUNT_ID", "").split(",") if a.strip()]
    
    # Xác định chu kỳ nạp (7 ngày gần nhất để MS monitor trend)
    end_date = datetime.now().strftime('%Y-%m-%d')
    start_date = (datetime.now() - timedelta(days=7)).strftime('%Y-%m-%d')

    logger.info(f"🚀 Khởi động Deep Sync: {start_date} -> {end_date}")

    for acc_id in acc_ids:
        pipeline.run(
            fetch_meta_deep_breakdown(acc_id, token, start_date, end_date),
            table_name="fb_ads_creative_breakdown_raw_metrics"
        )
        logger.info(f"✅ Success: Account {acc_id}")

if __name__ == "__main__":
    run_deep_sync()
