# Production validation — 2026-09-08

Bổ sung cho `test-evidence.md` (chỉ có test offline). Đây là kết quả chạy THẬT trên
Meta API + BigQuery, thực hiện sau khi merge PR #1 vào `main`.

## Kết quả

| Test | account_ids | target_tables | Khoảng ngày | Kết quả |
|---|---|---|---|---|
| Xác nhận điểm gãy cũ | 779857487799415 | performance | 08-14→08-21 | 0 dòng — gap thật |
| Mở rộng tìm resume | 779857487799415 | performance | 08-14→09-07 | Có data 09-04→09-07 — **fix work thật, account resume sau ~3 tuần nghỉ** |
| Xác nhận account tĩnh | 874972305237436 | performance | 06-01→06-14 | Data chỉ tới 06-05, sau đó 0 dòng |
| Mở rộng account tĩnh | 874972305237436 | performance | 06-14→09-08 | 0 dòng toàn bộ — **account ngừng hẳn từ 06-06, không phải bug** |
| Catch-up | 779857487799415 | demographic | 06-12→09-07 | ✅ success, 3834→6773 dòng |
| Catch-up | 779857487799415 | platform | 06-12→09-07 | ✅ success, tới 09-07 |
| Catch-up | 779857487799415 | geographic | 06-12→09-07 | ✅ success (chạy lâu hơn hẳn, ~9 phút, có thể do backoff), tới 09-07, 8001 dòng |
| Catch-up | 779857487799415 | placement_detail | 06-12→09-07 | ✅ success (~15+ phút, bảng 2 chiều breakdown nặng nhất), tới 09-07, 6597 dòng |

## Quyết định: KHÔNG catch-up device_detail / device_platform

Người dùng chủ động quyết định không cần 2 bảng này ở thời điểm hiện tại. Vẫn đứng ở
`2026-06-11` cho account `779857487799415`. Muốn làm sau thì dùng đúng lệnh:

```
target_tables=device_detail&backfill_start=2026-06-12&backfill_end=2026-09-07&account_ids=779857487799415
target_tables=device_platform&backfill_start=2026-06-12&backfill_end=2026-09-07&account_ids=779857487799415
```

## Phát hiện thêm ngoài phạm vi ban đầu

- Có account thứ 3 `587898528769829` trong `fact_fb_demographic`, đứng ở `2025-12-31`.
  Account cũ, đã bị bỏ khỏi `account_groups` trong code từ trước — không phải gap.
- `ahb_mart_unified.daily_ads_summary` là BigQuery Scheduled Query (6h sáng VN), không
  nằm trong repo nào — raw data mới sẽ tự chảy vào mart này, không cần thao tác thêm.

## Chưa verify

- Cron tự động (schedule, không phải workflow_dispatch) chạy code mới lần đầu vào
  00:15 UTC đêm 08→09/09/2026 — merge xong sau giờ cron chạy hôm 08/09 nên chưa có lần
  quan sát nào qua lịch tự động thật.
