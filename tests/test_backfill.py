"""Test cho backfill.py — chạy được offline, không gọi Meta, không đụng BigQuery.

Chạy:  python -m pytest tests/test_backfill.py -v
"""
import os
import sys
from datetime import date, timedelta

import pytest

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import backfill
from facebook_business.exceptions import FacebookRequestError


def fb_error(code, subcode=None, message="Call was not successful", http_status=500):
    body = {"error": {"message": message, "code": code}}
    if subcode is not None:
        body["error"]["error_subcode"] = subcode
    return FacebookRequestError(message, {"method": "GET", "path": "/insights"},
                                http_status, {}, body)


@pytest.fixture(autouse=True)
def clean_env(monkeypatch):
    for name in ("BACKFILL_START", "BACKFILL_END", "LOOKBACK_DAYS", "CHUNK_DAYS",
                 "TARGET_TABLES", "FB_ACCOUNT_IDS", "MAX_RETRIES",
                 "SLEEP_BETWEEN_RUNS", "ALERT_WEBHOOK_URL"):
        monkeypatch.delenv(name, raising=False)


# --------------------------------------------------------------------------
# iter_chunks — phải phủ kín dải ngày, không chồng lấn, không hụt ngày nào
# --------------------------------------------------------------------------

def test_iter_chunks_covers_range_exactly():
    start, end = date(2026, 6, 1), date(2026, 6, 15)
    chunks = list(backfill.iter_chunks(start, end, 7))
    assert chunks == [("2026-06-01", "2026-06-07"),
                      ("2026-06-08", "2026-06-14"),
                      ("2026-06-15", "2026-06-15")]

    # phủ kín: hợp của các chunk == đúng tập ngày [start, end]
    covered = set()
    for s, e in chunks:
        s_d = date.fromisoformat(s)
        e_d = date.fromisoformat(e)
        assert s_d <= e_d
        day = s_d
        while day <= e_d:
            assert day not in covered, f"ngày {day} bị kéo 2 lần"
            covered.add(day)
            day += timedelta(days=1)
    expected = {start + timedelta(days=i) for i in range((end - start).days + 1)}
    assert covered == expected


def test_iter_chunks_single_day():
    d = date(2026, 9, 8)
    assert list(backfill.iter_chunks(d, d, 7)) == [("2026-09-08", "2026-09-08")]


def test_iter_chunks_range_shorter_than_chunk():
    chunks = list(backfill.iter_chunks(date(2026, 9, 1), date(2026, 9, 3), 30))
    assert chunks == [("2026-09-01", "2026-09-03")]


def test_iter_chunks_zero_chunk_days_does_not_hang():
    chunks = list(backfill.iter_chunks(date(2026, 9, 1), date(2026, 9, 2), 0))
    assert chunks == [("2026-09-01", "2026-09-01"), ("2026-09-02", "2026-09-02")]


# --------------------------------------------------------------------------
# resolve_date_range — rolling vs catch-up
# --------------------------------------------------------------------------

def test_rolling_default_is_seven_days_inclusive():
    start, end, mode = backfill.resolve_date_range()
    today = date.today()
    assert end == today
    assert start == today - timedelta(days=6)      # 7 ngày tính cả hôm nay
    assert (end - start).days + 1 == 7
    assert "rolling 7" in mode


def test_rolling_respects_lookback_days(monkeypatch):
    monkeypatch.setenv("LOOKBACK_DAYS", "14")
    start, end, _ = backfill.resolve_date_range()
    assert (end - start).days + 1 == 14


def test_backfill_start_switches_to_catch_up(monkeypatch):
    monkeypatch.setenv("BACKFILL_START", "2026-06-06")
    monkeypatch.setenv("BACKFILL_END", "2026-09-08")
    monkeypatch.setenv("LOOKBACK_DAYS", "7")        # phải bị bỏ qua
    start, end, mode = backfill.resolve_date_range()
    assert (start, end) == (date(2026, 6, 6), date(2026, 9, 8))
    assert mode == "catch-up"


def test_backfill_end_defaults_to_today(monkeypatch):
    monkeypatch.setenv("BACKFILL_START", "2026-06-06")
    _, end, _ = backfill.resolve_date_range()
    assert end == date.today()


def test_reversed_range_is_rejected(monkeypatch):
    monkeypatch.setenv("BACKFILL_START", "2026-09-08")
    monkeypatch.setenv("BACKFILL_END", "2026-06-06")
    with pytest.raises(SystemExit):
        backfill.resolve_date_range()


def test_bad_date_format_is_rejected(monkeypatch):
    monkeypatch.setenv("BACKFILL_START", "08/09/2026")
    with pytest.raises(SystemExit):
        backfill.resolve_date_range()


def test_garbage_lookback_falls_back_to_default(monkeypatch):
    monkeypatch.setenv("LOOKBACK_DAYS", "bảy")
    start, end, _ = backfill.resolve_date_range()
    assert (end - start).days + 1 == 7


# --------------------------------------------------------------------------
# classify_fb_error
# --------------------------------------------------------------------------

@pytest.mark.parametrize("code,subcode,expected", [
    (4, None, "rate_limit"),        # app-level throttle
    (17, None, "rate_limit"),
    (32, None, "rate_limit"),
    (613, None, "rate_limit"),
    (1, 1504022, "rate_limit"),     # subcode thắng
    (1, 99, "transient"),           # đúng lỗi làm gãy run 34177997452
    (2, None, "transient"),
    (999, None, "transient"),       # mã lạ -> vẫn thử lại
    (190, None, "fatal"),           # token hỏng
    (100, None, "fatal"),           # sai tham số
    (200, None, "fatal"),
    (803, None, "fatal"),
])
def test_classify_fb_error(code, subcode, expected):
    assert backfill.classify_fb_error(fb_error(code, subcode)) == expected


def test_non_facebook_exception_is_transient():
    assert backfill.classify_fb_error(ConnectionResetError("boom")) == "transient"


# --------------------------------------------------------------------------
# _call_insights_with_retry — đây là hạng mục 2 của yêu cầu
# --------------------------------------------------------------------------

class FakeAccount:
    """Giả AdAccount: trả lỗi theo kịch bản rồi mới trả data."""

    def __init__(self, script, rows=None):
        self.script = list(script)
        self.rows = rows if rows is not None else [{"ad_id": "1"}]
        self.calls = 0

    def get_insights(self, fields=None, params=None):
        self.calls += 1
        if self.script:
            raise self.script.pop(0)
        return list(self.rows)


@pytest.fixture
def no_sleep(monkeypatch):
    slept = []
    monkeypatch.setattr(backfill.time, "sleep", lambda s: slept.append(s))
    return slept


def test_transient_error_is_retried_then_succeeds(no_sleep):
    acc = FakeAccount([fb_error(1, 99), fb_error(1, 99)], rows=[{"ad_id": "42"}])
    rows = backfill._call_insights_with_retry(acc, [], {}, "test", max_retries=5)
    assert rows == [{"ad_id": "42"}]
    assert acc.calls == 3                    # 1 lần đầu + 2 lần thử lại
    assert len(no_sleep) == 2


def test_backoff_is_exponential(no_sleep):
    acc = FakeAccount([fb_error(1, 99)] * 3)
    backfill._call_insights_with_retry(acc, [], {}, "test", max_retries=5)
    assert len(no_sleep) == 3
    # 5s, 10s, 20s (+ jitter <=3s) -> phải tăng dần, không phải retry ngay lập tức
    assert no_sleep[0] >= 5
    assert no_sleep == sorted(no_sleep)
    assert no_sleep[-1] >= 20


def test_rate_limit_backoff_is_much_longer_than_transient(no_sleep):
    acc = FakeAccount([fb_error(4)])
    backfill._call_insights_with_retry(acc, [], {}, "test", max_retries=5)
    assert no_sleep == [120]


def test_rate_limit_backoff_is_capped(no_sleep):
    acc = FakeAccount([fb_error(4)] * 5)
    backfill._call_insights_with_retry(acc, [], {}, "test", max_retries=6)
    assert max(no_sleep) <= 900


def test_fatal_error_raises_immediately_without_sleeping(no_sleep):
    acc = FakeAccount([fb_error(190, message="Invalid OAuth access token")])
    with pytest.raises(FacebookRequestError) as exc:
        backfill._call_insights_with_retry(acc, [], {}, "test", max_retries=5)
    assert exc.value.api_error_code() == 190
    assert acc.calls == 1
    assert no_sleep == []


def test_gives_up_after_max_retries_and_raises(no_sleep):
    acc = FakeAccount([fb_error(1, 99)] * 10)
    with pytest.raises(FacebookRequestError):
        backfill._call_insights_with_retry(acc, [], {}, "test", max_retries=3)
    assert acc.calls == 4                    # 1 lần đầu + 3 lần thử lại
    assert len(no_sleep) == 3


# --------------------------------------------------------------------------
# fetch_meta_ultimate — retry phải xong TRƯỚC khi yield dòng đầu tiên,
# và các cột mới phải mặc định 0 chứ không NULL
# --------------------------------------------------------------------------

@pytest.fixture
def stub_meta(monkeypatch):
    holder = {}

    def make(script, rows):
        acc = FakeAccount(script, rows)
        holder["acc"] = acc
        monkeypatch.setattr(backfill.FacebookAdsApi, "init", staticmethod(lambda **kw: None))
        monkeypatch.setattr(backfill, "AdAccount", lambda ident: acc)
        return acc

    return make


def test_no_partial_yield_before_retries_finish(stub_meta, no_sleep):
    acc = stub_meta([fb_error(1, 99), fb_error(1, 99)],
                    [{"account_id": "1", "ad_id": "a", "date_start": "2026-09-01"}])
    gen = backfill.fetch_meta_ultimate("1", "tok", "2026-09-01", "2026-09-07", max_retries=5)
    assert acc.calls == 0                    # generator chưa chạy gì
    rows = list(gen)
    assert acc.calls == 3
    assert len(rows) == 1                    # không nhân bản dòng sau retry


def test_new_action_columns_default_to_zero_not_null(stub_meta):
    stub_meta([], [{"account_id": "1", "ad_id": "a", "date_start": "2026-09-01",
                    "actions": [{"action_type": "post_reaction", "value": "5"}]}])
    row = list(backfill.fetch_meta_ultimate("1", "tok", "2026-09-01", "2026-09-01"))[0]
    assert row["fb_result_eng"] == 0
    assert row["fb_share_fix"] == 0
    assert row["fb_interaction"] == 5


def test_action_mapping_still_fills_new_columns(stub_meta):
    stub_meta([], [{"account_id": "1", "ad_id": "a", "date_start": "2026-09-01",
                    "actions": [{"action_type": "post_engagement", "value": "12"},
                                {"action_type": "post.share", "value": "3"}]}])
    row = list(backfill.fetch_meta_ultimate("1", "tok", "2026-09-01", "2026-09-01"))[0]
    assert row["fb_result_eng"] == 12
    assert row["fb_share_fix"] == 3


def test_breakdown_columns_are_populated_and_others_stay_all(stub_meta):
    stub_meta([], [{"account_id": "1", "ad_id": "a", "date_start": "2026-09-01",
                    "age": "25-34", "gender": "female"}])
    row = list(backfill.fetch_meta_ultimate("1", "tok", "2026-09-01", "2026-09-01",
                                            ["age", "gender"]))[0]
    assert (row["age"], row["gender"]) == ("25-34", "female")
    assert row["region"] == "All"
    assert row["publisher_platform"] == "All"


# --------------------------------------------------------------------------
# TABLE_SPECS — primary key phải khớp cấu hình cũ, không được đổi ngầm
# --------------------------------------------------------------------------

def test_table_specs_match_legacy_primary_keys():
    expected = {
        "fact_fb_performance": ["date", "fb_ad_id"],
        "fact_fb_demographic": ["date", "fb_ad_id", "age", "gender"],
        "fact_fb_platform": ["date", "fb_ad_id", "publisher_platform"],
        "fact_fb_geographic": ["date", "fb_ad_id", "region"],
        "fact_fb_placement_detail": ["date", "fb_ad_id", "publisher_platform", "platform_position"],
        "fact_fb_device_detail": ["date", "fb_ad_id", "impression_device"],
        "fact_fb_device_platform": ["date", "fb_ad_id", "device_platform"],
    }
    actual = {name: backfill.BASE_PK + extra for _, name, _, extra in backfill.TABLE_SPECS}
    assert actual == expected


def test_every_breakdown_field_is_part_of_its_primary_key():
    for key, name, breakdowns, extra_pk in backfill.TABLE_SPECS:
        assert sorted(breakdowns) == sorted(extra_pk), f"{name} lệch breakdown vs primary key"


# --------------------------------------------------------------------------
# run_pipeline — chấm dứt "success giả": có lỗi thì PHẢI exit != 0
# --------------------------------------------------------------------------

class FakePipeline:
    """Giả dlt pipeline: ghi lại mọi lần run, fail ở những bảng được chỉ định."""

    def __init__(self, fail_tables=()):
        self.fail_tables = set(fail_tables)
        self.runs = []

    def run(self, data, table_name=None, write_disposition=None, primary_key=None):
        self.runs.append((table_name, primary_key))
        if table_name in self.fail_tables:
            raise fb_error(1, 99)
        list(data)          # ép generator chạy như dlt thật


@pytest.fixture
def pipeline_env(monkeypatch, stub_meta):
    """Dựng đủ env + stub để gọi run_pipeline() mà không chạm mạng/BigQuery."""
    stub_meta([], [{"account_id": "1", "ad_id": "a", "date_start": "2026-09-01"}])
    for name, val in (("GCP_PROJECT_ID", "p"), ("GCP_CLIENT_EMAIL", "e"),
                      ("GCP_PRIVATE_KEY", "k"), ("FB_ACCESS_TOKEN", "tok")):
        monkeypatch.setenv(name, val)
    monkeypatch.setenv("SLEEP_BETWEEN_RUNS", "0")
    monkeypatch.setenv("FB_ACCOUNT_IDS", "111")
    monkeypatch.setenv("LOOKBACK_DAYS", "1")

    def install(fail_tables=()):
        fake = FakePipeline(fail_tables)
        monkeypatch.setattr(backfill.dlt, "pipeline", lambda **kw: fake)
        return fake

    return install


def test_failure_makes_the_job_exit_nonzero(pipeline_env, monkeypatch):
    monkeypatch.setenv("TARGET_TABLES", "performance,geographic")
    fake = pipeline_env(fail_tables={"fact_fb_geographic"})

    with pytest.raises(SystemExit) as exc:
        backfill.run_pipeline()

    assert exc.value.code == 1, "pipeline gãy mà vẫn báo success -> lỗi cũ tái diễn"
    # bảng hỏng không được giết cả run: bảng còn lại vẫn phải chạy
    assert [t for t, _ in fake.runs] == ["fact_fb_performance", "fact_fb_geographic"]


def test_clean_run_does_not_exit(pipeline_env, monkeypatch):
    monkeypatch.setenv("TARGET_TABLES", "performance")
    fake = pipeline_env()
    backfill.run_pipeline()                  # không được raise SystemExit
    assert len(fake.runs) == 1


def test_one_broken_table_does_not_stop_the_rest(pipeline_env, monkeypatch):
    fake = pipeline_env(fail_tables={"fact_fb_performance"})
    with pytest.raises(SystemExit):
        backfill.run_pipeline()
    assert len(fake.runs) == 7               # đủ cả 7 bảng, không dừng ở bảng đầu


def test_target_tables_filters_what_runs(pipeline_env, monkeypatch):
    monkeypatch.setenv("TARGET_TABLES", "demographic,device_detail")
    fake = pipeline_env()
    backfill.run_pipeline()
    assert [t for t, _ in fake.runs] == ["fact_fb_demographic", "fact_fb_device_detail"]


def test_unknown_target_table_is_rejected_early(pipeline_env, monkeypatch):
    monkeypatch.setenv("TARGET_TABLES", "performance,gheographic")
    fake = pipeline_env()
    with pytest.raises(SystemExit) as exc:
        backfill.run_pipeline()
    assert "gheographic" in str(exc.value)
    assert fake.runs == [], "phải chặn trước khi bắn request nào"


def test_missing_token_fails_fast(pipeline_env, monkeypatch):
    monkeypatch.delenv("FB_ACCESS_TOKEN")
    fake = pipeline_env()
    with pytest.raises(SystemExit):
        backfill.run_pipeline()
    assert fake.runs == []


def test_catch_up_runs_one_pipeline_call_per_chunk(pipeline_env, monkeypatch):
    monkeypatch.setenv("TARGET_TABLES", "performance")
    monkeypatch.setenv("BACKFILL_START", "2026-06-06")
    monkeypatch.setenv("BACKFILL_END", "2026-06-19")     # 14 ngày
    monkeypatch.setenv("CHUNK_DAYS", "7")
    fake = pipeline_env()
    backfill.run_pipeline()
    assert len(fake.runs) == 2                            # 14 ngày / chunk 7 = 2 lần


def test_webhook_is_skipped_when_not_configured(monkeypatch):
    called = []
    monkeypatch.setattr(backfill.urllib.request, "urlopen",
                        lambda *a, **k: called.append(1))
    backfill.notify_failure(["x"])
    assert called == []


def test_broken_webhook_does_not_mask_the_real_failure(pipeline_env, monkeypatch):
    monkeypatch.setenv("TARGET_TABLES", "performance")
    monkeypatch.setenv("ALERT_WEBHOOK_URL", "https://example.invalid/hook")
    monkeypatch.setattr(backfill.urllib.request, "urlopen",
                        lambda *a, **k: (_ for _ in ()).throw(OSError("hook down")))
    pipeline_env(fail_tables={"fact_fb_performance"})
    with pytest.raises(SystemExit) as exc:
        backfill.run_pipeline()
    assert exc.value.code == 1               # webhook hỏng không được nuốt exit code
