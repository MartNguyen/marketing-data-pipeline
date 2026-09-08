# Intent — backfill.py: incremental + retry + hết "success giả"

Ngày: 2026-09-08 · Repo: MartNguyen/marketing-data-pipeline · File chính: `backfill.py`

## 1. Vấn đề đang gặp

Pipeline `daily_run.yml` → `backfill.py` đẩy Meta Ads vào BigQuery
(`ahb-dltxgg-bigquery.fb_ads_master_v4`, 7 bảng). Data đứng yên nhưng GitHub Actions
vẫn báo **success** → không ai biết pipeline đã gãy.

### Bằng chứng đã có (phiên trước)
- Log GitHub Actions run `34177997452`: `dlt.extract.exceptions.ResourceExtractionError`
  — "Call was not successful" / Method GET, tại resource `fact_fb_performance`.
- Đã loại trừ token: Graph API trả 200 OK, không phải `OAuthException`.
- App "AHB - DLT x GG BigQuery" (App ID `895543509638824`) đang ở
  `ads_api_access_tier = development_access`, business verification **Unverified**
  → rate limit dev tier rất thấp.
- `account_groups` hardcode `start="2026-01-01"`, `end=date.today()` → mỗi lần chạy
  kéo lại full dải ngày, dải càng dài càng chắc chắn chạm limit giữa chừng.
- `sync_account_worker()` bọc trong 1 `try/except` tầng ngoài, chỉ `log + continue`,
  không `raise` → lỗi bị nuốt, Actions vẫn xanh.

### Bằng chứng MỚI — query BigQuery hôm nay (khác với brief)

| Bảng | acc 779857487799415 | acc 874972305237436 |
|---|---|---|
| fact_fb_performance | max 2026-08-14 | **max 2026-06-05** |
| 6 bảng breakdown | max 2026-06-11 | **max 2026-06-05** |

→ Account `874972305237436` **KHÔNG hề "chạy trót lọt"** — nó đứng yên từ 2026-06-05
trên cả 7 bảng, tức là còn cũ hơn account bị báo lỗi. Brief đang hiểu nhầm điểm này.

Hai cách giải thích cho `874972305237436`, chưa phân biệt được:
- (a) pipeline cũng gãy trên account này, hoặc
- (b) account này **ngừng chạy ads** từ 06-06 → Meta Insights không trả dòng nào →
  `max(date)` đứng yên một cách hợp lệ (account này chỉ có 48 dòng fact cho cả
  2026-02-04 → 2026-06-05, mức chi rất nhỏ).

Ngược lại, với `779857487799415` thì đây **chắc chắn là lỗi pipeline**: cùng account,
cùng khoảng thời gian, bảng fact có data tới 08-14 nhưng 6 bảng breakdown dừng ở 06-11.
Có delivery thì phải có cả breakdown.

## 2. Rủi ro lớn nhất của yêu cầu "chuyển sang incremental"

Nếu chỉ đổi sang rolling 7–14 ngày gần nhất, **các lỗ hổng hiện tại sẽ không bao giờ
được lấp**, vì cửa sổ rolling không bao giờ chạm tới chúng:

- `874972305237436`: 2026-06-06 → nay (~94 ngày, cả 7 bảng)
- `779857487799415`: breakdown 2026-06-12 → nay (~88 ngày); fact 2026-08-15 → nay (~24 ngày)

→ Incremental phải đi kèm **chế độ catch-up chạy tay** (chỉ định start/end, chia nhỏ
theo chunk) thì mới vừa nhẹ tải hằng ngày vừa lấp được lỗ cũ.

## 3. Kết quả mong muốn

1. Chạy hằng ngày chỉ kéo N ngày gần nhất (mặc định đề xuất 14) → giảm mạnh số request,
   sống được trên dev tier.
2. Lỗi transient của Meta (code 1 / subcode 99, code 2, timeout khi phân trang dataset
   lớn) được **retry + exponential backoff**, không bung thẳng ra ngoài.
3. Pipeline gãy thật thì **GitHub Actions phải đỏ**, kèm log tóm tắt rõ account/bảng nào
   fail — chấm dứt "success giả".
4. Có đường lấp lỗ hổng cũ mà không phải sửa code (chạy `workflow_dispatch` với
   start/end tùy chọn).

## 4. Ngoài phạm vi (không làm ở lần này)

- Không đụng schema BigQuery, không đổi tên bảng/pipeline (`meta_ultimate_v15_4_refresh`)
  — giữ nguyên checkpoint & merge an toàn.
- Không sửa `backfill_geo.py`, `main.py`, `patch.py`.
- Không tự chạy pipeline production / không tự merge PR.
- Business verification Meta: đang chờ duyệt, chạy song song, không block việc sửa code.

## 5. Cần người dùng xác nhận

- [ ] Số ngày rolling mặc định cho lần chạy hằng ngày
- [ ] Có làm chế độ catch-up chạy tay để lấp lỗ hổng cũ không
- [ ] Kênh cảnh báo khi fail (tối thiểu: Actions đỏ + log rõ)
