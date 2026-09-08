# Plan — backfill.py incremental + retry + fail loud

Nguồn: `spec.md` cùng thư mục. Nhánh code: `feat/backfill-incremental-retry` (cắt từ nhánh này).

## Các bước

1. **`backfill.py` — lớp cấu hình**
   `_env_int()`, `_env_csv()`, `resolve_date_range()` (rolling vs catch-up),
   `iter_chunks(start, end, chunk_days)` → list `(s_str, e_str)` inclusive, không chồng lấn.
   Hằng số `TABLE_SPECS`: 7 phần tử `(key, table_name, breakdowns, extra_pk)`.

2. **`backfill.py` — phân loại lỗi + retry**
   `classify_fb_error(e)` → `"rate_limit" | "transient" | "fatal"`.
   `_call_insights_with_retry(acc, fields, params, label)` → trả về list rows thô, tự retry
   theo bảng ở spec §2. Generator `fetch_meta_ultimate()` gọi hàm này rồi map sang row —
   fetch lại từ đầu chunk khi retry (spec §3), dựa vào merge+PK khử trùng.

3. **`backfill.py` — vòng chạy + gom lỗi**
   `sync_table_chunk()` chạy 1 `pipeline.run` cho 1 (account, bảng, chunk).
   `run_pipeline()` lặp account × bảng × chunk, gom `failures`, nghỉ `SLEEP_BETWEEN_RUNS`
   giữa các lần run, in tóm tắt cuối, gọi webhook nếu có, `sys.exit(1)` nếu có lỗi.

4. **`backfill.py` — sửa bất nhất cột** (spec §6)
   Thêm `'fb_result_eng': 0, 'fb_share_fix': 0` vào dict khởi tạo row.

5. **`.github/workflows/daily_run.yml`**
   Thêm `workflow_dispatch.inputs` + map vào `env` của step chạy script.

6. **Kiểm chứng (Stage 4)** — xem `test-evidence.md`
   - Import + compile sạch.
   - Unit test không cần mạng cho `iter_chunks`, `resolve_date_range`, `classify_fb_error`,
     và cho backoff (monkeypatch `time.sleep`, giả lập lỗi rồi thành công).
   - Không chạy pipeline thật vào BigQuery production trong lúc code review.

## Không làm trong plan này
Chạy catch-up thật lên production, merge PR, đụng schema/tên bảng.
