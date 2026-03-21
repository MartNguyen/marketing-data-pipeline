import os
import sys

# Thử import để soi ruột thư viện
try:
    from facebook_ads import INSIGHTS_BREAKDOWNS_OPTIONS
    print("--- DANH SÁCH BREAKDOWN HỢP LỆ TRÊN MÁY NÍ ---")
    # In ra tất cả các khóa (keys) mà thư viện này chấp nhận
    for key in INSIGHTS_BREAKDOWNS_OPTIONS.keys():
        print(f"Key: {key} -> Support: {INSIGHTS_BREAKDOWNS_OPTIONS[key]['breakdowns']}")
    print("---------------------------------------------")
except Exception as e:
    print(f"LỖI khi soi thư viện: {e}")

# Dừng chương trình tại đây để ní đọc Log
sys.exit(0)
