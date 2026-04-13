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
    
    fields = ['ad_id', 'ad_name', 'adset_id', 'adset_name', 'campaign_id', 'date_start', 'spend', 'impressions', 'clicks', 'actions']
    breakdown_types = ['age', 'gender', 'region']

    for b_type in breakdown_types:
        params = {
            'time_range': {'since': start_date, 'until': end_date},
            'level': 'ad',
            'time_increment': 1,
            'breakdowns': [b_type]
        }
        # Nếu lỗi ở một trục, vẫn cố gắng chạy các trục còn lại
        try:
            insights = account.get_insights(fields=fields, params=params)
            for entry in insights:
                data = dict(entry)
                data['breakdown_dimension'] = b_type
                data['video_views_3s'] = 0.0
                data['thruplay_15s'] = 0.0
                if 'actions' in data:
                    for act in data['actions']:
                        val = float(act.get('value', 0))
                        if act['action_type'] == 'video_view': data['video_views_3s'] = val
                        elif act['action_type'] in ['thruplay', 'video_thruplay_watched_actions']: data['thruplay_15s'] = val
                yield data
        except Exception as e:
            # Ném lỗi lên để hàm run_deep_sync xử lý skip acc
            raise e

def run_deep_sync():
    # Credentials Mapping cho dlt
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
        # --- SCENARIO: SKIP IF NO PERMISSION ---
        try:
            logger.info(f"🚀 Processing Account: {acc_id}")
            pipeline.run(
                fetch_meta_parallel_breakdown(acc_id, token, start_date, end_date),
                table_name="fb_ads_creative_breakdown_raw_metrics"
            )
            logger.info(f"✅ Success: Data loaded for {acc_id}")
        except Exception as e:
            # Nếu gặp lỗi 403 (Quyền) hoặc 400 (Lỗi logic/Account chết)
            if "403" in str(e) or "ads_management" in str(e) or "400" in str(e):
                logger.warning(f"⚠️ SKIP ACCOUNT {acc_id}: Thiếu quyền hoặc tài khoản lỗi. Chi tiết: {str(e)[:100]}...")
                continue # Nhảy sang account tiếp theo, không làm sập pipeline
            else:
                # Nếu là lỗi hệ thống khác (GCP, Network), mới cho dừng để check
                logger.error(f"❌ CRITICAL ERROR for {acc_id}: {e}")
                raise e

if __name__ == "__main__":
    run_deep_sync()
