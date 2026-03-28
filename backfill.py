"""
Module: Meta Ads Backfill Pipeline (v6.2 - Ultimate Tiệm Tiến)
Standard: Production Ready - Monthly Gap Strategy (Open-ended)
Feature: Fan-out Prevention, Auto-Parse Video & Engagement Metrics, Historical Isolation
"""

import dlt
import os
import logging
import time
from datetime import datetime, date
from dateutil.relativedelta import relativedelta
from facebook_business.api import FacebookAdsApi
from facebook_business.adobjects.adaccount import AdAccount

# 1. Setup Diagnostic Logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

@dlt.resource(write_disposition="append")
def fetch_meta_data(account_id, access_token, fields, start_date, end_date, breakdown=None):
    """Hàm fetch data cốt lõi - Tự bóc tách JSON Actions thành Cột phẳng"""
    FacebookAdsApi.init(access_token=access_token)
    clean_acc_id = str(account_id).replace('act_', '')
    account = AdAccount(f'act_{clean_acc_id}')
    
    params = {
        'time_range': {'since': start_date, 'until': end_date},
        'level': 'ad',
        'time_increment': 1,
    }
    if breakdown:
        params['breakdowns'] = breakdown

    try:
        insights = account.get_insights(fields=fields, params=params)
        for entry in insights:
            data = dict(entry)
            data['account_id'] = clean_acc_id
            
            # --- XỬ LÝ DATE: Ép về chuẩn BQ DATE ---
            if 'date_start' in data and data['date_start']:
                try:
                    data['date_start'] = datetime.strptime(data['date_start'], '%Y-%m-%d').date()
                except Exception as e:
                    logger.warning(f"Lỗi parse date: {data['date_start']} - {e}")

            # --- XỬ LÝ METRICS: Bóc tách Actions (Chỉ áp dụng cho bảng Master) ---
            if not breakdown and 'actions' in data:
                view_3s = 0
                thruplay = 0
                custom_eng = 0
                
                # Quét mảng actions để cộng dồn đúng metric
                for act in data['actions']:
                    act_type = act.get('action_type', '')
                    val = float(act.get('value', 0))
                    
                    if act_type == 'video_view': 
                        view_3s += val
                    elif act_type in ['video_thruplay_watched_actions', 'thruplay']:
                        thruplay += val
                    elif act_type in ['post_reaction', 'comment', 'post', 'post_engagement']:
                        custom_eng += val
                        
                # Tạo cột mới phẳng lỳ cho Looker Studio dễ đọc
                data['custom_video_view_3s'] = view_3s
                data['custom_video_thruplay'] = thruplay
                data['custom_total_engagement'] = custom_eng

            yield data
            
    except Exception as e:
        logger.error(f"❌ Error Acc {account_id} | Breakdown {breakdown} | {start_date}: {e}")

def run_backfill_campaign():
    # 2. Config Hộp Đen (GCP Credentials & Location)
    os.environ["DESTINATION__BIGQUERY__CREDENTIALS__PROJECT_ID"] = os.environ.get("GCP_PROJECT_ID")
    os.environ["DESTINATION__BIGQUERY__CREDENTIALS__CLIENT_EMAIL"] = os.environ.get("GCP_CLIENT_EMAIL")
    os.environ["DESTINATION__BIGQUERY__CREDENTIALS__PRIVATE_KEY"] = os.environ.get("GCP_PRIVATE_KEY", "").replace("\\n", "\n")
    os.environ["DESTINATION__BIGQUERY__LOCATION"] = "asia-southeast1" 

    pipeline = dlt.pipeline(
        pipeline_name="meta_backfill_v6_ultimate", 
        destination="bigquery", 
        dataset_name="fb_ads_ahb1_report_v2"
    )
    
    token = os.environ.get("FB_ACCESS_TOKEN")
    acc_ids = [a.strip() for a in os.environ.get("FB_ACCOUNT_ID", "").split(",") if a.strip()]
    
    # 3. Định nghĩa Fields chuẩn
    # LƯU Ý: Đã nhét 'reach' vào Master. Tuyệt đối KHÔNG SUM(reach) trên Looker Studio.
    fields_master = [
        "account_id", "campaign_id", "campaign_name", "adset_id", "adset_name", 
        "ad_id", "ad_name", "date_start", "spend", "impressions", "clicks", "reach",
        "inline_post_engagement", "actions"
    ]
    
    # Bảng Breakdown bỏ 'reach' và 'actions' để tránh rác data và sai số nhân khẩu học
    fields_breakdown = [
        "account_id", "ad_id", "date_start", "spend", "impressions", "clicks"
    ]

    # 4. Vòng lặp Chia Tháng (Monthly Gap) - Tự động quét đến hiện tại
    start_point = date(2025, 1, 1)
    end_point = date.today() # Chốt sổ tại ngày bấm chạy Script

    current_start = start_point

    while current_start <= end_point:
        current_end = current_start + relativedelta(months=1) - relativedelta(days=1)
        if current_end > end_point: 
            current_end = end_point
            
        str_start = current_start.strftime('%Y-%m-%d')
        str_end = current_end.strftime('%Y-%m-%d')
        
        logger.info(f"🚀 --- Đang cày Block Tháng: {str_start} đến {str_end} ---")

        for acc_id in acc_ids:
            # Luồng 1: Master Data (Bảng Historical)
            pipeline.run(
                fetch_meta_data(acc_id, token, fields_master, str_start, str_end)(), 
                table_name="facebook_insights_backfill_historical",
            )
            
            # Luồng 2: Age & Gender (Bảng Historical)
            pipeline.run(
                fetch_meta_data(acc_id, token, fields_breakdown, str_start, str_end, ['age', 'gender'])(), 
                table_name="insights_age_gender_backfill_historical",
            )
            
            # Luồng 3: Region (Bảng Historical)
            pipeline.run(
                fetch_meta_data(acc_id, token, fields_breakdown, str_start, str_end, ['region'])(), 
                table_name="insights_region_backfill_historical",
            )
            
            logger.info(f"✅ Xong Acc {acc_id} cho tháng {str_start}")
            time.sleep(2) # Nghỉ hồi API cho Account
        
        current_start += relativedelta(months=1)
        time.sleep(5) # Nghỉ hồi máu cho Node/Runner

if __name__ == "__main__":
    run_backfill_campaign()
