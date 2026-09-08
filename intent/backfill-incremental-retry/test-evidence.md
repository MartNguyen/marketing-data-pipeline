# Test evidence — Stage 4

Ngày chạy: 2026-09-08 · Nhánh: `feat/backfill-incremental-retry`

## Cách chạy lại

```bash
python -m venv venv && ./venv/bin/pip install facebook-business dlt pytest
./venv/bin/python -m pytest tests/test_backfill.py -v
```

Test chạy **hoàn toàn offline**: giả `AdAccount` và `dlt.pipeline`, monkeypatch
`time.sleep` nên không gọi Meta, không đụng BigQuery, không tốn quota của app
đang ở `development_access`.

## Kết quả

```
.............................................                            [100%]
45 passed in 0.75s
```

## Test nào chứng minh yêu cầu nào

| Yêu cầu | Test |
|---|---|
| 1. Bỏ full re-sync 2026-01-01, chuyển rolling | `test_rolling_default_is_seven_days_inclusive`, `test_rolling_respects_lookback_days`, `test_backfill_start_switches_to_catch_up` |
| 1b. Chia chunk, không hụt/không trùng ngày | `test_iter_chunks_covers_range_exactly` (kiểm tra từng ngày một), `test_iter_chunks_single_day`, `test_iter_chunks_zero_chunk_days_does_not_hang` |
| 2. Retry + exponential backoff cho lỗi transient | `test_transient_error_is_retried_then_succeeds`, `test_backoff_is_exponential`, `test_gives_up_after_max_retries_and_raises` |
| 2b. Đúng lỗi code:1 subcode:99 của run 34177997452 | `test_classify_fb_error[1-99-transient]` |
| 2c. Rate limit nghỉ dài hơn, có trần | `test_rate_limit_backoff_is_much_longer_than_transient`, `test_rate_limit_backoff_is_capped` |
| 2d. Lỗi token/tham số không retry vô ích | `test_fatal_error_raises_immediately_without_sleeping` |
| 2e. Retry không làm nhân bản dòng | `test_no_partial_yield_before_retries_finish` |
| 3. Hết "success giả" | `test_failure_makes_the_job_exit_nonzero`, `test_clean_run_does_not_exit`, `test_broken_webhook_does_not_mask_the_real_failure` |
| 3b. Một bảng hỏng không giết cả run | `test_one_broken_table_does_not_stop_the_rest` (đủ 7 bảng vẫn chạy) |
| Không đổi ngầm primary key / schema | `test_table_specs_match_legacy_primary_keys`, `test_every_breakdown_field_is_part_of_its_primary_key` |
| Sửa bất nhất cột (spec §6) | `test_new_action_columns_default_to_zero_not_null`, `test_action_mapping_still_fills_new_columns` |

## CHƯA kiểm chứng được ở bước này

- **Chưa chạy thật lên Meta + BigQuery production.** Cần bấm `workflow_dispatch`
  một đợt nhỏ (1 account, 1 bảng, 7 ngày) rồi đối chiếu `max(date)` trong BigQuery.
- **Chưa biết retry có thực sự cứu được lỗi thật hay không** — chỉ chứng minh cơ chế
  retry hoạt động đúng như thiết kế. Nếu app vẫn ở `development_access` và limit quá
  thấp, retry có thể vẫn cạn 5 lần rồi fail (lúc đó Actions sẽ đỏ, đúng ý đồ).
- **Nguyên nhân account `874972305237436` đứng ở 2026-06-05** vẫn chưa xác định —
  chờ chạy catch-up thử rồi xem có trả về dòng nào không.
