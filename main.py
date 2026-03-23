import dlt
import os
from facebook_ads import facebook_insights_source

# 1. Chỉ lấy đúng 1 ID tài khoản mà ní biết chắc là CÓ CHẠY VIDEO
# Ví dụ: '587898528769829'
test_acc_id = '587898528769829' 

# 2. Cấu hình Pipeline ảo (Đổ vào dataset test để không làm rác dataset thật)
pipeline = dlt.pipeline(
    pipeline_name="test_video_fields",
    destination="bigquery",
    dataset_name="fb_ads_test_video" 
)

# 3. Chỉ lấy số liệu của 1 ngày duy nhất (Hôm qua)
source = facebook_insights_source(
    account_id=test_acc_id,
    access_token=os.environ.get("FB_ACCESS_TOKEN"),
    initial_load_past_days=1, # Ép lấy 1 ngày cho nhanh
    level="ad",
    fields=(
        "campaign_id", "adset_id", "ad_id", "date_start", 
        "spend", "impressions", "clicks", "account_id",
        "video_play_actions", "video_p3s_actions" # Hai "mầm mống" gây lỗi
    )
)

# Chỉ chạy tài nguyên Master Insights để test
info = pipeline.run(source.with_resources("va_insights"))
print(info)
