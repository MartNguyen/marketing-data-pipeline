import dlt
import os
import time
from datetime import date
from main import fetch_meta_ultimate # Đảm bảo file chính của ní tên là main.py

def run_patch():
    # Setup Credentials
    os.environ["DESTINATION__BIGQUERY__CREDENTIALS__PROJECT_ID"] = os.environ.get("GCP_PROJECT_ID")
    os.environ["DESTINATION__BIGQUERY__CREDENTIALS__CLIENT_EMAIL"] = os.environ.get("GCP_CLIENT_EMAIL")
    os.environ["DESTINATION__BIGQUERY__CREDENTIALS__PRIVATE_KEY"] = os.environ.get("GCP_PRIVATE_KEY", "").replace("\\n", "\n")
    
    pipeline = dlt.pipeline(
        pipeline_name="meta_patch_v14_1", 
        destination="bigquery", 
        dataset_name="fb_ads_ahb_master_v3"
    )

    token = os.environ.get("FB_ACCESS_TOKEN")
    acc_id = "587898528769829"
    
    # Chỉ định đích danh 2 tháng cần vá
    blocks = [
        ('2025-07-01', '2025-07-31'),
        ('2025-10-01', '2025-10-31')
    ]

    for s_str, e_str in blocks:
        print(f"🛠 Đang vá lỗ hổng: {s_str} đến {e_str}")
        
        # Luồng 1: Master
        pipeline.run(fetch_meta_ultimate(acc_id, token, s_str, e_str), table_name="fact_fb_performance")
        # Luồng 2: Demographic
        pipeline.run(fetch_meta_ultimate(acc_id, token, s_str, e_str, ['age', 'gender']), table_name="fact_fb_demographic")
        # Luồng 3: Platform
        pipeline.run(fetch_meta_ultimate(acc_id, token, s_str, e_str, ['publisher_platform']), table_name="fact_fb_platform")
        # Luồng 4: Geographic
        pipeline.run(fetch_meta_ultimate(acc_id, token, s_str, e_str, ['region']), table_name="fact_fb_geographic")
        
        print(f"✅ Đã vá xong block {s_str}")
        time.sleep(10) # Nghỉ ngơi lâu hơn để tránh dính 403 lần nữa

if __name__ == "__main__":
    run_patch()
