@dlt.resource(write_disposition="replace") # Thay thế toàn bộ để lấy data mới nhất
def fetch_meta_deep_breakdown(account_id, access_token, start_date, end_date):
    FacebookAdsApi.init(access_token=access_token)
    account = AdAccount(f'act_{account_id}')
    
    fields = [
        'ad_id', 'ad_name', 'campaign_id', 'date_start',
        'spend', 'impressions', 'clicks', 'actions'
    ]
    
    params = {
        'time_range': {'since': start_date, 'until': end_date},
        'level': 'ad',
        'time_increment': 1,
        'breakdowns': ['age', 'gender', 'region'] # Đánh thẳng vào 3 trục
    }

    try:
        insights = account.get_insights(fields=fields, params=params)
        for entry in insights:
            data = dict(entry)
            
            # --- LẤY SỐ THỰC TỪ ACTIONS ---
            # Mặc định gán bằng 0 thay vì Null để dễ làm toán trên Looker
            data['video_views_3s'] = 0.0
            data['post_engagement'] = 0.0
            
            if 'actions' in data:
                for action in data['actions']:
                    a_type = action.get('action_type')
                    val = float(action.get('value', 0))
                    
                    if a_type == 'video_view':
                        data['video_views_3s'] = val
                    elif a_type in ['post_engagement', 'post_reaction', 'comment']:
                        data['post_engagement'] += val # Cộng dồn engagement thực
            
            yield data
    except Exception as e:
        logger.error(f"❌ API Error: {e}")

# Chạy pipeline đổ vào bảng: fb_ads_creative_breakdown_raw_metrics
