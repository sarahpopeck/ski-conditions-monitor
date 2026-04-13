from datetime import date

from runner import epoch_to_iso_utc, safe_pct, to_iso_utc, to_report_date


def test_safe_pct_returns_expected_percentage():
    assert safe_pct(50, 100) == 50
    assert safe_pct(1, 4) == 25


def test_safe_pct_returns_none_for_invalid_denominator():
    assert safe_pct(10, 0) is None
    assert safe_pct(10, None) is None
    assert safe_pct(10, "0") is None


def test_safe_pct_returns_none_for_invalid_numerator():
    assert safe_pct(None, 100) is None
    assert safe_pct("abc", 100) is None


def test_to_iso_utc_converts_offset_timestamp():
    value = "2026-02-24T07:00:00-05:00"
    result = to_iso_utc(value)

    assert result is not None
    assert result.startswith("2026-02-24T12:00:00")
    assert result.endswith("+00:00")


def test_to_iso_utc_returns_none_for_empty_or_invalid():
    assert to_iso_utc(None) is None
    assert to_iso_utc("") is None
    assert to_iso_utc("not-a-date") is None


def test_to_report_date_returns_local_date():
    value = "2026-02-24T07:00:00-05:00"
    result = to_report_date(value)

    assert isinstance(result, date)
    assert result == date(2026, 2, 24)


def test_epoch_to_iso_utc_converts_unix_seconds():
    result = epoch_to_iso_utc(1700000000)

    assert result is not None
    assert result.endswith("+00:00")


def test_epoch_to_iso_utc_returns_none_for_invalid_input():
    assert epoch_to_iso_utc(None) is None
    assert epoch_to_iso_utc("") is None
    assert epoch_to_iso_utc("bad-value") is None