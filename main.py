import dlt
import os
from facebook_ads import facebook_insights_source

# 1. Dùng đúng ID tài khoản test
test_acc_id = '587898528769829' 

# 2. Cấu hình Pipeline ảo
pipeline = dlt.pipeline(
    pipeline_name="test_video_fields_v2",
    destination="bigquery",
    dataset_name="fb_ads_test_video" 
)

# 3. Lấy 1 ngày duy nhất với các field Video
source = facebook_insights_source(
    account_id=test_acc_id,
    access_token=os.environ.get("FB_ACCESS_TOKEN"),
    initial_load_past_days=1,
    level="ad",
    fields=(
        "campaign_id", "adset_id", "ad_id", "date_start", 
        "spend", "impressions", "clicks", "account_id",
        "video_play_actions", "video_p3s_actions" 
    )
)

# SỬA LỖI: Đổi 'va_insights' thành 'facebook_insights' theo báo lỗi của dlt
info = pipeline.run(source.with_resources("facebook_insights"))
print(info)
