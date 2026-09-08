# Domain: AHB Ads — Pipeline + BigQuery + Dashboard
Cập nhật: 8/9/2026

## Overview
Toàn bộ luồng: API (Meta/Google/TikTok) → Python pipeline (dlt) → BigQuery → /ads dashboard trên portal.

---

## GitHub Repos (Pipeline Scripts)
| Repo | Platform | Ghi chú |
|---|---|---|
| MartNguyen/marketing-data-pipeline | Meta Ads | App Facebook: "AHB - DLT x GG BigQuery", App ID `895543509638824`. Tên có "GG BigQuery" = đích đến Google BigQuery, KHÔNG phải app của Google Ads. App `AHB_API1` (ID `849008491378671`) chưa dùng ở đâu, đừng nhầm. |
| MartNguyen/marketing-data-pipeline-google-ads | Google Ads | |
| MartNguyen/marketing-data-pipeline-tiktok | TikTok Ads | Luôn ổn định, chưa từng có vấn đề. |

> ⚠️ Đã clone thật vào `~/apps/marketing-data-pipeline` và `~/apps/marketing-data-pipeline-tiktok`
> trên server (2). Remote dùng HTTPS + token nhúng thẳng URL (`git remote -v` để xem) — không có SSH key riêng cho GitHub.

## Pipeline Structure — Meta (`marketing-data-pipeline`)
```
marketing-data-pipeline/
├── backfill.py         ← script chính, daily_run.yml gọi cái này
├── backfill_geo.py     ← job backfill riêng cho breakdown geo (debug_meta.yml gọi, TÊN GÂY HIỂU LẦM — không nhẹ như tên)
├── main.py, patch.py   ← không đụng, ngoài phạm vi các lần sửa gần đây
└── intent/backfill-incremental-retry/  ← intent.md/spec.md/plan.md/test-evidence.md/production-validation.md
                                            của lần sửa 8/9/2026, đọc trước khi động vào backfill.py
```

## GitHub Actions (CI/CD) — Meta
- `daily_run.yml`: cron `15 0 * * *` UTC, gọi `backfill.py`. **Từ 8/9/2026: đã đúng, chạy
  incremental rolling (mặc định 7 ngày gần nhất), KHÔNG còn full re-sync từ 2026-01-01
  mỗi lần chạy như trước.** Đọc env, không hardcode ngày nữa — xem bảng biến bên dưới.
- `debug_meta.yml`: manual trigger, thực chất chạy `backfill_geo.py` — job backfill nặng
  thật (full năm, nhiều account), KHÔNG nhẹ như tên gợi ý. Cẩn thận khi trigger để "test nhanh".

### Biến môi trường của `backfill.py` (từ 8/9/2026, dùng cho `workflow_dispatch`)
| Biến | Mặc định | Ý nghĩa |
|---|---|---|
| `LOOKBACK_DAYS` | `7` | Chạy hằng ngày: kéo N ngày gần nhất |
| `BACKFILL_START` / `BACKFILL_END` | rỗng / hôm nay | Set `BACKFILL_START` → bật chế độ catch-up, bỏ qua `LOOKBACK_DAYS` |
| `CHUNK_DAYS` | `7` | Chia dải ngày catch-up thành từng đoạn, mỗi đoạn 1 request — né rate limit |
| `TARGET_TABLES` | tất cả 7 bảng | CSV: `performance,demographic,platform,geographic,placement_detail,device_detail,device_platform` |
| `FB_ACCOUNT_IDS` | 2 account hiện tại | CSV account id, để catch-up riêng từng account |
| `MAX_RETRIES` | `5` | Số lần retry cho lỗi transient Facebook (code 1/2, backoff mũ) |
| `ALERT_WEBHOOK_URL` | rỗng | Set thì POST JSON tóm tắt khi fail — chưa cấu hình kênh nào, để trống là bỏ qua |

**Fail loud:** lỗi thật (không phải transient được retry hết) → `sys.exit(1)` → Actions đỏ
đúng nghĩa. KHÔNG còn `try/except: log + continue` nuốt lỗi im lặng như bản cũ.

**Cách catch-up thủ công đúng:** luôn xác định gap thật bằng query BigQuery
`MAX(date)` TRƯỚC, rồi set `BACKFILL_START` sát nhất có thể — set `merge` write_disposition
KHÔNG giảm số request gọi Facebook, backfill xa (vd từ 2024) vẫn tốn y hệt full re-sync cũ.

---

## BigQuery Schema — ĐÃ SỬA LẠI CHO ĐÚNG (bản cũ trước 8/9/2026 ghi SAI tên dataset)

- Project: `ahb-dltxgg-bigquery`
- Service account pipeline: `dlt-bigquery-pusher@ahb-dltxgg-bigquery.iam.gserviceaccount.com`

### Dataset thật (xác nhận qua query trực tiếp 8/9/2026)
| Dataset | Platform | Ghi chú |
|---|---|---|
| `fb_ads_master_v4` | Meta, raw | 7 bảng: `fact_fb_performance` + 6 breakdown (`fact_fb_demographic`, `fact_fb_platform`, `fact_fb_geographic`, `fact_fb_placement_detail`, `fact_fb_device_detail`, `fact_fb_device_platform`) |
| `google_ads_v3_star_schema` | Google, raw | `gg_performance_ad_fact` |
| `tiktok_raw` | TikTok, raw | `v_daily_ads_latest`, `v_campaigns_latest` |
| `ahb_mart_unified` | Mart hợp nhất | `daily_ads_summary` (Meta+Google), `v_ads_unified` (UNION cả 3 platform, dashboard đọc từ đây), `job_contracts`, `deposits`, ... |

> Dataset `raw_meta`/`raw_google`/`mart_meta`/`mart_google`/`unified_ads` ghi trong bản
> cũ của file này **KHÔNG tồn tại** — có thể là tên dự kiến ban đầu, không khớp thực tế.

### `ahb_mart_unified.daily_ads_summary` — CƠ CHẾ QUAN TRỌNG, DỄ TÌM NHẦM
Đây là **BigQuery Scheduled Query** (config thẳng trong BigQuery Console), **không nằm
trong bất kỳ repo git nào**. Chạy **6h sáng giờ VN mỗi ngày**, đọc `fb_ads_master_v4` +
Google, ghi vào bảng này. Muốn sửa logic mart phải vào BigQuery Console > Scheduled
Queries, đừng tìm trong code. Bảng này (qua view `v_ads_unified`) là cái dashboard
`/ads` Overall tab thực sự đọc — raw table mới không tự nhiên xuất hiện trên dashboard
ngay, phải đợi tới lần chạy 6h sáng kế tiếp.

### Account Meta thật đang track (`account_groups` trong `backfill.py`)
| Account ID | Trạng thái |
|---|---|
| `779857487799415` | Đang chạy ads. Có giai đoạn nghỉ ~3 tuần (15/8-3/9/2026) rồi chạy lại — đừng nhầm là lỗi pipeline nếu thấy gap tương tự, luôn verify qua catch-up test nhỏ trước khi kết luận. |
| `874972305237436` | **Đã NGỪNG chạy ads hoàn toàn từ 6/6/2026** — xác nhận bằng test catch-up thật (0 dòng suốt 6/6→8/9). KHÔNG PHẢI lỗi pipeline nếu account này đứng yên, đừng tốn thời gian debug. |
| `587898528769829` | Account CŨ, đã bị bỏ khỏi `account_groups` từ trước 8/9/2026 (không rõ từ khi nào). Data đứng ở `2025-12-31` vĩnh viễn trong các bảng breakdown — không phải gap, bỏ qua nếu thấy. |

---

## Meta Ads — Business Logic Quan Trọng
### Engagement Objective
- Engagement hiện tại = **Reaction + Share + Comment + Save post**
- "Result" của campaign Engagement = `post_engagement` từ `actions[]`

### Action Types cần track
| action_type | Ý nghĩa | Cột BigQuery |
|---|---|---|
| `post_reaction` | Like/Love/Haha/Wow/Sad/Angry | `fb_interaction` |
| `post.share` | Share bài (⚠️ KHÔNG phải `'post'`) | `fb_share_fix` |
| `comment` | Bình luận | `fb_comment` |
| `onsite_conversion.post_save` | Lưu bài | `fb_save` |
| `post_engagement` | Tổng interaction ("Result") | `fb_result_eng` |

---

## 6 Pending Changes (chốt 2/7/2026) — CẬP NHẬT TRẠNG THÁI 8/9/2026

1. ~~Fix `fb_share` action_type: `'post'` → `'post.share'`~~ → **XONG**, commit 26/6/2026 (`fb_share_fix`)
2. ~~Thêm `post_engagement` vào pipeline~~ → **XONG**, commit 26/6/2026 (`fb_result_eng`)
3. ~~Tách `main_daily.py` chỉ kéo 7 ngày gần nhất~~ → **XONG theo cách khác 8/9/2026**: không
   tách file riêng, mà thêm `LOOKBACK_DAYS` env vào chính `backfill.py` — đạt cùng mục
   tiêu (rolling 7 ngày), không cần thêm script mới
4. ~~Update `daily_run.yml` gọi `main_daily.py`~~ → **Không cần nữa**, vì mục 3 đã giải
   quyết bằng cách khác — `daily_run.yml` vẫn gọi `backfill.py` như cũ, chỉ khác là giờ
   `backfill.py` tự đọc env để chạy incremental
5. **CHƯA LÀM** — Build `dim_ad_creative` script (fetch `object_story_spec` per `ad_id`, map Video/Image format)
6. **CHƯA LÀM** — Update Layer 2 SQL tính lại interaction đúng: Reaction + Share + Comment + Save

---

## Bài học vận hành (đúc kết sau 2 lần gặp cùng 1 pattern — 5/8 và 8/9/2026)

**GitHub Actions "success" KHÔNG đảm bảo data mới.** Cả 2 lần pipeline Meta chết hàng
tuần/tháng mà Actions vẫn báo xanh mỗi ngày — nguyên nhân khác nhau (lần 1: token hết
hạn; lần 2: request quá tải dev-tier) nhưng cùng 1 cơ chế che giấu: code cũ dùng
`try/except: log + continue`, không `raise`. **Luôn verify bằng query `MAX(date)` trực
tiếp trên BigQuery**, không tin status Actions một mình. Từ 8/9/2026 code đã fail loud
(exit 1 khi lỗi thật), nhưng thói quen verify bằng BigQuery vẫn nên giữ.

**App Facebook đang ở `development_access` tier** (Business Verification "AHB Solutions"
đã nộp, đang chờ Meta duyệt, không có deadline). Không chặn pipeline hằng ngày chạy
(đã fix incremental để sống được ở tier này), chỉ cần thiết nếu muốn: tăng quota
catch-up nặng, thêm account, hoặc lên Full Access (cần 500 call/15 ngày + error rate <15%).

---

## /ads Dashboard (trên AHB Portal)
- Route: `portal.ahbsolutions.vn/ads`
- Stack: HTML/JS thuần, fetch từ FastAPI endpoint
- Data source: `ahb_mart_unified.v_ads_unified` (xem cơ chế Scheduled Query ở trên — có
  độ trễ tới 6h sáng hôm sau so với raw table)
- Filters: Date range, Platform, Campaign, Geography
- Pending UI: Export CSV, so sánh kỳ trước
- Repo: `MartNguyen/ahb-portal` — SQL view liên quan: `sql/v_ads_unified.sql`, `sql/v_campaign_performance.sql`

---

## Google Ads — Notes
- Pipeline tách repo riêng (`marketing-data-pipeline-google-ads`), workflow `daily_sync.yml`
- Data dừng ở `2026-05-21` — **xác nhận 2 lần (5/8 và 8/9/2026) là account hết spend
  thật, KHÔNG PHẢI lỗi pipeline.** Đừng debug lại trừ khi có campaign mới chạy.
- Đã join vào `ahb_mart_unified.daily_ads_summary`

## TikTok Ads — Notes
- Pipeline riêng (`marketing-data-pipeline-tiktok`), dataset raw `tiktok_raw`
- Ổn định xuyên suốt, không có sự cố nào tính đến 8/9/2026
