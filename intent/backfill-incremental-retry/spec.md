# Spec — backfill.py incremental + retry + fail loud

Nguồn: `intent/backfill-incremental-retry/intent.md` (đã xác nhận 2026-09-08)
Quyết định đã chốt: rolling **7 ngày** · **có** catch-up chạy tay · alert = Actions đỏ + webhook tùy chọn.

## 1. Cấu hình bằng biến môi trường

Không hardcode ngày trong code nữa. `backfill.py` đọc:

| Biến | Mặc định | Ý nghĩa |
|---|---|---|
| `LOOKBACK_DAYS` | `7` | Chạy hằng ngày: kéo N ngày gần nhất (tính cả hôm nay) |
| `BACKFILL_START` | *(rỗng)* | Nếu set (YYYY-MM-DD) → **chế độ catch-up**, bỏ qua `LOOKBACK_DAYS` |
| `BACKFILL_END` | hôm nay | Ngày cuối của catch-up |
| `CHUNK_DAYS` | `7` | Chia dải ngày thành từng chunk ≤ N ngày, mỗi chunk 1 request |
| `TARGET_TABLES` | tất cả | CSV tên logic bảng, để catch-up riêng breakdown mà không đụng fact |
| `FB_ACCOUNT_IDS` | 2 acc hiện tại | CSV account id, để catch-up từng account một |
| `MAX_RETRIES` | `5` | Số lần thử lại cho lỗi transient |
| `SLEEP_BETWEEN_RUNS` | `5` | Giây nghỉ giữa 2 lần `pipeline.run` (giảm áp lực dev tier) |
| `ALERT_WEBHOOK_URL` | *(rỗng)* | Nếu set → POST JSON tóm tắt khi có lỗi. Không set → bỏ qua |

Tên logic bảng dùng cho `TARGET_TABLES`:
`performance, demographic, platform, geographic, placement_detail, device_detail, device_platform`

## 2. Phân loại lỗi Meta và cách xử lý

`fetch_meta_ultimate()` phân loại `FacebookRequestError` thành 3 nhóm:

| Nhóm | Mã | Xử lý |
|---|---|---|
| **RATE_LIMIT** | code `4`, `17`, `32`, `613`; subcode `1504022`, `2446079` | Nghỉ dài: `120s × 2^lần thử`, trần 900s, rồi thử lại |
| **TRANSIENT** | code `1` (gồm subcode `99`), `2`; lỗi mạng/timeout không phải FacebookRequestError | Backoff mũ: `5s × 2^lần thử` + jitter, trần 120s, thử lại |
| **FATAL** | code `100`, `190`, `200`, `803` | `raise` ngay, không thử lại (sai tham số / token hỏng / thiếu quyền) |

Mã không nằm trong bảng → coi là TRANSIENT (thử lại), vì "Call was not successful" của
run 34177997452 rơi đúng vào nhóm mơ hồ này.

## 3. Retry bên trong generator — và rủi ro phải nói rõ

`fetch_meta_ultimate()` là generator, có thể gãy **giữa chừng khi đang phân trang**,
lúc đó đã yield ra một phần rows.

**Chọn:** khi retry thì **fetch lại từ đầu chunk và yield lại toàn bộ**, không cố bỏ qua
phần đã yield.

**Lý do:** phương án "bỏ qua N dòng đã yield" giả định Meta trả về đúng thứ tự cũ cho
cùng một request — Meta **không cam kết** điều này. Nếu thứ tự đổi, bỏ qua N dòng sẽ
**mất dữ liệu thật**. Ngược lại, yield trùng là vô hại vì tất cả bảng đều dùng
`write_disposition="merge"` + `primary_key`, dlt tự khử trùng theo primary key.

**Cái giá:** một chunk gãy ở cuối sẽ phải kéo lại từ đầu. Chấp nhận được vì chunk chỉ 7 ngày.

## 4. Fail loud — chấm dứt "success giả"

- Bỏ `try/except ... continue` tầng ngoài kiểu nuốt lỗi.
- Mỗi (account × bảng × chunk) fail được ghi vào danh sách `failures`, pipeline **vẫn chạy tiếp**
  các phần còn lại (một breakdown hỏng không được giết cả run).
- Cuối run: in bảng tóm tắt `✅ thành công / ❌ thất bại`, liệt kê rõ account + bảng + chunk + lỗi.
- Nếu `failures` không rỗng → `sys.exit(1)` → **GitHub Actions đỏ**.
- Nếu có `ALERT_WEBHOOK_URL` → POST JSON `{text: <tóm tắt>}` bằng `urllib` (không thêm dependency).
  Webhook lỗi thì chỉ log warning, không được che mất exit code thật.

## 5. Thay đổi `daily_run.yml`

Thêm `workflow_dispatch.inputs`: `backfill_start`, `backfill_end`, `chunk_days`,
`lookback_days`, `target_tables`, `account_ids` → map vào env. Lịch cron hằng ngày
không truyền input → rơi về mặc định rolling 7 ngày.

## 6. Điểm bất nhất phát hiện thêm — cần quyết

`backfill.py` khởi tạo sẵn hầu hết cột = 0, nhưng **`fb_result_eng` và `fb_share_fix`
thì không** (dòng 79 và 81 — chỉ gán khi action type xuất hiện). Hệ quả: ad-day nào
không có `post_engagement`/`post.share` sẽ ra **NULL** thay vì **0**.
`backfill_geo.py` dòng 62 thì khởi tạo đúng `= 0`.

**Đề xuất:** khởi tạo `'fb_result_eng': 0, 'fb_share_fix': 0` trong `backfill.py` cho khớp.
Đây là ADD-only, không phá schema, không đổi ý nghĩa số liệu (NULL và 0 đều là "không có").
→ **Sẽ làm**, và ghi rõ ở đây để không phải là thay đổi lén.

## 7. Không đụng tới

- Tên pipeline `meta_ultimate_v15_4_refresh`, dataset `fb_ads_master_v4`, tên 7 bảng, primary key.
- Danh sách `fields` gọi Meta, logic map `actions` → cột (ngoài mục 6).
- `backfill_geo.py`, `main.py`, `patch.py`, `ad_breakdown.py`.
- Không tự chạy pipeline production, không tự merge PR.

## 8. Rủi ro còn lại sau khi sửa

1. **Chưa hết nghẽn ngay.** App vẫn ở `development_access` cho tới khi Meta duyệt business
   verification. Incremental + retry chỉ làm pipeline *sống sót* trong limit thấp, không nâng limit.
   Lên Full Access còn cần thêm 500 call/15 ngày với error rate <15%.
2. **Catch-up ~90 ngày là tải nặng** đúng lúc limit đang thấp. Phải chạy tay từng đợt nhỏ
   (1 account + 1-2 bảng mỗi lần), không bấm một phát toàn bộ.
3. **Account `874972305237436` chưa rõ nguyên nhân.** Nếu catch-up trả về 0 dòng cho account này
   thì kết luận là account ngừng chạy ads, không phải pipeline lỗi.
4. **Rolling 7 ngày** không bắt được attribution Meta chốt muộn sau ngày thứ 7. Nếu sau này thấy
   số liệu lệch so với Ads Manager ở các ngày cũ, tăng `LOOKBACK_DAYS` lên 14 (chỉ đổi biến, không sửa code).
