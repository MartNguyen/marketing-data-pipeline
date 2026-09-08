import dlt
import os
import sys
import json
import time
import random
import logging
import urllib.request
from datetime import date, datetime, timedelta
from facebook_business.api import FacebookAdsApi
from facebook_business.adobjects.adaccount import AdAccount
from facebook_business.exceptions import FacebookRequestError

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Cấu hình — đọc từ biến môi trường, KHÔNG hardcode ngày như bản cũ.
# Bản cũ luôn kéo từ 2026-01-01 -> hôm nay, dải càng dài càng chắc chạm rate
# limit của app đang ở development_access.
# ---------------------------------------------------------------------------

DEFAULT_ACCOUNT_IDS = ["874972305237436", "779857487799415"]

# (key dùng cho TARGET_TABLES, tên bảng BigQuery, breakdowns gửi Meta, primary key phụ)
BASE_PK = ["date", "fb_ad_id"]
TABLE_SPECS = [
    ("performance",      "fact_fb_performance",      [],                                     []),
    ("demographic",      "fact_fb_demographic",      ['age', 'gender'],                      ["age", "gender"]),
    ("platform",         "fact_fb_platform",         ['publisher_platform'],                 ["publisher_platform"]),
    ("geographic",       "fact_fb_geographic",       ['region'],                             ["region"]),
    ("placement_detail", "fact_fb_placement_detail", ['publisher_platform', 'platform_position'], ["publisher_platform", "platform_position"]),
    ("device_detail",    "fact_fb_device_detail",    ['impression_device'],                  ["impression_device"]),
    ("device_platform",  "fact_fb_device_platform",  ['device_platform'],                    ["device_platform"]),
]

# Meta trả các mã này khi bị bóp băng thông -> nghỉ dài rồi thử lại.
RATE_LIMIT_CODES = {4, 17, 32, 613}
RATE_LIMIT_SUBCODES = {1504022, 2446079}
# Sai tham số / token hỏng / thiếu quyền -> thử lại vô nghĩa, bung ra ngay.
FATAL_CODES = {100, 190, 200, 803}


def _env_int(name, default):
    raw = os.environ.get(name, "").strip()
    if not raw:
        return default
    try:
        return int(raw)
    except ValueError:
        logger.warning(f"⚠️  {name}='{raw}' không phải số, dùng mặc định {default}")
        return default


def _env_csv(name, default):
    raw = os.environ.get(name, "").strip()
    if not raw:
        return list(default)
    return [item.strip() for item in raw.split(",") if item.strip()]


def _parse_date(raw, label):
    try:
        return datetime.strptime(raw.strip(), "%Y-%m-%d").date()
    except ValueError:
        raise SystemExit(f"❌ {label}='{raw}' sai định dạng, cần YYYY-MM-DD")


def resolve_date_range():
    """Chế độ catch-up khi có BACKFILL_START, ngược lại rolling LOOKBACK_DAYS ngày."""
    today = date.today()
    start_raw = os.environ.get("BACKFILL_START", "").strip()
    end_raw = os.environ.get("BACKFILL_END", "").strip()

    if start_raw:
        start = _parse_date(start_raw, "BACKFILL_START")
        end = _parse_date(end_raw, "BACKFILL_END") if end_raw else today
        mode = "catch-up"
    else:
        lookback = max(1, _env_int("LOOKBACK_DAYS", 7))
        end = today
        start = end - timedelta(days=lookback - 1)
        mode = f"rolling {lookback} ngày"

    if start > end:
        raise SystemExit(f"❌ Dải ngày ngược: {start} > {end}")
    return start, end, mode


def iter_chunks(start, end, chunk_days):
    """Cắt [start, end] thành các đoạn <= chunk_days ngày, bao gồm 2 đầu, không chồng lấn."""
    chunk_days = max(1, chunk_days)
    cursor = start
    while cursor <= end:
        chunk_end = min(cursor + timedelta(days=chunk_days - 1), end)
        yield cursor.strftime('%Y-%m-%d'), chunk_end.strftime('%Y-%m-%d')
        cursor = chunk_end + timedelta(days=1)


def classify_fb_error(err):
    """rate_limit | transient | fatal.

    Mã lạ được coi là transient — lỗi 'Call was not successful' làm gãy pipeline
    (Actions run 34177997452) rơi đúng vào vùng mơ hồ này.
    """
    if not isinstance(err, FacebookRequestError):
        return "transient"
    code = err.api_error_code()
    subcode = err.api_error_subcode()
    if code in RATE_LIMIT_CODES or subcode in RATE_LIMIT_SUBCODES:
        return "rate_limit"
    if code in FATAL_CODES:
        return "fatal"
    return "transient"


def _describe_error(err):
    if isinstance(err, FacebookRequestError):
        return (f"code={err.api_error_code()} subcode={err.api_error_subcode()} "
                f"http={err.http_status()} msg={err.api_error_message()}")
    return f"{type(err).__name__}: {err}"


def _call_insights_with_retry(acc, fields, params, label, max_retries):
    """Gọi Insights và phân trang hết, có retry + exponential backoff.

    Trả về list rows thô. Cố tình materialize cả chunk: lỗi phân trang giữa chừng
    được nuốt gọn ở đây thay vì bung ra khi dlt đã ghi dở một phần.
    """
    for attempt in range(max_retries + 1):
        try:
            return list(acc.get_insights(fields=fields, params=params))
        except Exception as err:
            kind = classify_fb_error(err)
            detail = _describe_error(err)

            if kind == "fatal":
                logger.error(f"💀 {label} — lỗi không thể retry ({detail})")
                raise
            if attempt >= max_retries:
                logger.error(f"💀 {label} — hết {max_retries} lần thử ({detail})")
                raise

            if kind == "rate_limit":
                delay = min(120 * (2 ** attempt), 900)
                icon = "🚨 Rate limit"
            else:
                delay = min(5 * (2 ** attempt), 120) + random.uniform(0, 3)
                icon = "🔁 Lỗi transient"
            logger.warning(
                f"{icon} {label} — thử lại lần {attempt + 1}/{max_retries} sau {delay:.0f}s ({detail})"
            )
            time.sleep(delay)


def fetch_meta_ultimate(account_id, access_token, start_date, end_date, breakdown=None, max_retries=5):
    FacebookAdsApi.init(access_token=access_token)
    acc = AdAccount(f'act_{str(account_id).replace("act_", "")}')

    fields = [
        "account_id", "campaign_id", "campaign_name", "objective",
        "adset_id", "adset_name", "ad_id", "ad_name", 
        "date_start", "spend", "impressions", "clicks", "reach", "frequency",
        "inline_post_engagement", "actions", "video_avg_time_watched_actions", "video_play_actions"
    ]
    
    has_breakdown = breakdown is not None and len(breakdown) > 0
    
    params = {
        'time_range': {'since': start_date, 'until': end_date},
        'level': 'ad',
        'time_increment': 1,
        'breakdowns': breakdown if has_breakdown else []
    }

    label = f"acc {account_id} [{','.join(breakdown) if has_breakdown else 'no-breakdown'}] {start_date}->{end_date}"
    # Toàn bộ retry + phân trang xong xuôi TRƯỚC khi yield dòng đầu tiên. Nhờ vậy
    # dlt không bao giờ nhận được nửa chunk rồi mới gãy, và cũng không cần mẹo
    # "bỏ qua N dòng đã yield" — mẹo đó nguy hiểm vì Meta không cam kết giữ nguyên
    # thứ tự giữa 2 lần gọi, bỏ qua theo số dòng sẽ làm MẤT dữ liệu.
    # Cái giá: giữ cả chunk trong RAM. Chunk 7 ngày / 1 account nên không đáng kể.
    insights = _call_insights_with_retry(acc, fields, params, label, max_retries)

    for entry in insights:
        raw = dict(entry)
        row = {
            'fb_account_id': raw.get('account_id'),
            'fb_campaign_name': raw.get('campaign_name'),
            'fb_objective': raw.get('objective'),
            'fb_adset_name': raw.get('adset_name'),
            'fb_ad_id': raw.get('ad_id'),
            'fb_ad_name': raw.get('ad_name'),
            'date': raw.get('date_start'),
            'fb_spend': round(float(raw.get('spend', 0)), 2),
            'fb_impressions': int(raw.get('impressions', 0)),
            'fb_clicks': int(raw.get('clicks', 0)),
            'fb_reach': int(raw.get('reach', 0)),
            'fb_frequency': float(raw.get('frequency', 0)),
            'fb_eng_total': int(raw.get('inline_post_engagement', 0)),
            'fb_interaction': 0, 'fb_comment': 0, 'fb_share': 0, 'fb_save': 0,
            'fb_video_2s': 0, 'fb_video_3s': 0, 'fb_thruplay': 0,
            'fb_video_avg_time': 0, 'fb_video_plays': 0,
            'fb_purchase': 0, 'fb_lead': 0,
            # Khởi tạo cho khớp backfill_geo.py — thiếu 2 dòng này thì ad-day nào
            # không có action tương ứng sẽ ra NULL thay vì 0.
            'fb_result_eng': 0, 'fb_share_fix': 0,
            # Đắp cứng giá trị 'All' để khớp khít cấu trúc REQUIRED cũ của BigQuery, bảo vệ an toàn data
            'age': 'All', 'gender': 'All', 'region': 'All', 
            'publisher_platform': 'All', 'platform_position': 'All', 
            'impression_device': 'All', 'device_platform': 'All'
        }

        if has_breakdown:
            for b_field in breakdown:
                row[b_field] = raw.get(b_field, 'Unknown')

        if 'video_avg_time_watched_actions' in raw:
            for v_avg in raw['video_avg_time_watched_actions']:
                row['fb_video_avg_time'] = int(v_avg.get('value', 0))
        if 'video_play_actions' in raw:
            for v_play in raw['video_play_actions']:
                row['fb_video_plays'] = int(v_play.get('value', 0))

        if 'actions' in raw:
            for act in raw['actions']:
                val = int(act.get('value', 0))
                a_type = act.get('action_type')
                if a_type == 'post_reaction': row['fb_interaction'] = val
                elif a_type == 'comment': row['fb_comment'] = val
                elif a_type == 'post': row['fb_share'] = val
                elif a_type == 'post.share': row['fb_share_fix'] = val # NEW
                elif a_type == 'onsite_conversion.post_save': row['fb_save'] = val
                elif a_type == 'post_engagement': row['fb_result_eng'] = val # NEW
                elif a_type == 'video_view': row['fb_video_3s'] = val
                elif a_type == 'video_2_sec_continuous_video_view': row['fb_video_2s'] = val
                elif a_type in ['thruplay', 'video_thruplay_watched_actions']: row['fb_thruplay'] = val
                elif a_type == 'purchase': row['fb_purchase'] = val
                elif a_type == 'lead': row['fb_lead'] = val
        yield row


def notify_failure(summary_lines):
    """Bắn webhook nếu có ALERT_WEBHOOK_URL. Webhook hỏng không được che exit code thật."""
    url = os.environ.get("ALERT_WEBHOOK_URL", "").strip()
    if not url:
        return
    payload = json.dumps({"text": "\n".join(summary_lines)}).encode("utf-8")
    req = urllib.request.Request(url, data=payload, headers={"Content-Type": "application/json"})
    try:
        with urllib.request.urlopen(req, timeout=15) as resp:
            logger.info(f"📣 Đã gửi cảnh báo webhook (HTTP {resp.status})")
    except Exception as err:
        logger.warning(f"⚠️  Không gửi được webhook: {type(err).__name__}: {err}")


def run_pipeline():
    os.environ["DESTINATION__BIGQUERY__LOCATION"] = "asia-southeast1"
    os.environ["DESTINATION__BIGQUERY__CREDENTIALS__PROJECT_ID"] = os.environ.get("GCP_PROJECT_ID")
    os.environ["DESTINATION__BIGQUERY__CREDENTIALS__CLIENT_EMAIL"] = os.environ.get("GCP_CLIENT_EMAIL")
    os.environ["DESTINATION__BIGQUERY__CREDENTIALS__PRIVATE_KEY"] = os.environ.get("GCP_PRIVATE_KEY", "").replace("\\n", "\n")

    # Giữ nguyên tên pipeline để nó khớp checkpoint cũ, nạp nối tiếp/merge an toàn dữ liệu
    pipeline = dlt.pipeline(
        pipeline_name="meta_ultimate_v15_4_refresh",
        destination="bigquery", 
        dataset_name="fb_ads_master_v4"
    )

    token = os.environ.get("FB_ACCESS_TOKEN")
    if not token:
        raise SystemExit("❌ Thiếu FB_ACCESS_TOKEN")

    start, end, mode = resolve_date_range()
    chunk_days = _env_int("CHUNK_DAYS", 7)
    max_retries = _env_int("MAX_RETRIES", 5)
    pause = _env_int("SLEEP_BETWEEN_RUNS", 5)
    account_ids = _env_csv("FB_ACCOUNT_IDS", DEFAULT_ACCOUNT_IDS)

    wanted = _env_csv("TARGET_TABLES", [key for key, _, _, _ in TABLE_SPECS])
    unknown = [w for w in wanted if w not in {key for key, _, _, _ in TABLE_SPECS}]
    if unknown:
        raise SystemExit(f"❌ TARGET_TABLES có tên lạ: {unknown}. Hợp lệ: "
                         f"{[key for key, _, _, _ in TABLE_SPECS]}")
    specs = [s for s in TABLE_SPECS if s[0] in wanted]

    chunks = list(iter_chunks(start, end, chunk_days))
    total = len(account_ids) * len(specs) * len(chunks)
    logger.info(f"🚀 Chế độ {mode} | {start} -> {end} | {len(chunks)} chunk x {chunk_days} ngày "
                f"| {len(account_ids)} account | {len(specs)} bảng | tổng {total} lần nạp")

    failures = []
    ok_count = 0

    for acc_id in account_ids:
        for key, table_name, breakdowns, extra_pk in specs:
            for s_str, e_str in chunks:
                job = f"{acc_id} / {table_name} / {s_str}->{e_str}"
                logger.info(f"⏳ {job}")
                try:
                    pipeline.run(
                        fetch_meta_ultimate(acc_id, token, s_str, e_str, breakdowns, max_retries),
                        table_name=table_name,
                        write_disposition="merge",
                        # Ép primary_key bảng phụ khớp để tránh dlt ghi lộn xộn
                        primary_key=BASE_PK + extra_pk,
                    )
                    ok_count += 1
                except Exception as err:
                    logger.error(f"❌ {job} — {_describe_error(err)}")
                    failures.append((job, _describe_error(err)))
                if pause:
                    time.sleep(pause)

    # ---- Tổng kết: không được im lặng báo success nữa ----
    summary = [
        "===== TỔNG KẾT PIPELINE META =====",
        f"Chế độ: {mode} | Dải ngày: {start} -> {end} | Chunk: {chunk_days} ngày",
        f"✅ Thành công: {ok_count}/{total}",
        f"❌ Thất bại:   {len(failures)}/{total}",
    ]
    for job, detail in failures:
        summary.append(f"   - {job} :: {detail}")

    for line in summary:
        (logger.error if failures else logger.info)(line)

    if failures:
        notify_failure(summary)
        sys.exit(1)

    logger.info("🎉 Toàn bộ chạy sạch.")


if __name__ == "__main__":
    run_pipeline()
