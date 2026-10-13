import pytest

from app.actions.gfwclient import (
    IntegratedAlert,
    IntegratedAlertsConfidenceEnum,
    IntegratedAlertsConfidenceEnumOrder,
    NasaViirsFireAlert,
    NasaViirsFireAlertConfidenceEnum,
    NasaViirsFireAlertConfidenceEnumOrder,
)
from app.actions.utils import summarize_by_confidence, fire_alert_confidence, integrated_alert_confidence


def make_integrated(confidence, lat=1.0, lon=2.0, date="2026-10-01"):
    return IntegratedAlert.parse_obj({
        "latitude": lat,
        "longitude": lon,
        "gfw_integrated_alerts__confidence": confidence,
        "gfw_integrated_alerts__date": date,
    })


def make_fire(confidence_cat, lat=1.0, lon=2.0, date="2026-10-01"):
    return NasaViirsFireAlert.parse_obj({
        "latitude": lat,
        "longitude": lon,
        "confidence__cat": confidence_cat,
        "alert__date": date,
    })


def test_summary_counts_every_level_and_only_sends_at_or_above_threshold():
    alerts = [
        make_integrated("nominal"),
        make_integrated("nominal"),
        make_integrated("high"),
        make_integrated("highest"),
    ]

    summary = summarize_by_confidence(
        alerts,
        confidence_of=integrated_alert_confidence,
        confidence_order=IntegratedAlertsConfidenceEnumOrder,
        lowest_confidence=IntegratedAlertsConfidenceEnum.high,
    )

    assert summary["lowest_confidence"] == "high"
    assert summary["available_total"] == 4
    assert summary["sent_total"] == 2
    assert summary["available_by_confidence"] == {"nominal": 2, "high": 1, "highest": 1}
    assert summary["sent_by_confidence"] == {"high": 1, "highest": 1}


def test_summary_zero_fills_levels_with_no_alerts():
    summary = summarize_by_confidence(
        [],
        confidence_of=integrated_alert_confidence,
        confidence_order=IntegratedAlertsConfidenceEnumOrder,
        lowest_confidence=IntegratedAlertsConfidenceEnum.highest,
    )

    assert summary["available_total"] == 0
    assert summary["sent_total"] == 0
    assert summary["available_by_confidence"] == {"nominal": 0, "high": 0, "highest": 0}
    assert summary["sent_by_confidence"] == {"highest": 0}


def test_summary_samples_are_capped_per_level_and_carry_location_date_and_confidence():
    alerts = [make_integrated("nominal", lat=float(i), lon=-float(i), date=f"2026-10-0{i}") for i in range(1, 8)]

    summary = summarize_by_confidence(
        alerts,
        confidence_of=integrated_alert_confidence,
        confidence_order=IntegratedAlertsConfidenceEnumOrder,
        lowest_confidence=IntegratedAlertsConfidenceEnum.nominal,
        sample_size=3,
    )

    assert len(summary["samples"]["nominal"]) == 3
    assert summary["samples"]["high"] == []
    assert summary["samples"]["nominal"][0] == {
        "latitude": 1.0,
        "longitude": -1.0,
        "date": "2026-10-01",
        "confidence": "nominal",
    }


def test_summary_keeps_unknown_confidence_labels_visible_but_never_sent():
    alerts = [make_integrated("mystery"), make_integrated("highest")]

    summary = summarize_by_confidence(
        alerts,
        confidence_of=integrated_alert_confidence,
        confidence_order=IntegratedAlertsConfidenceEnumOrder,
        lowest_confidence=IntegratedAlertsConfidenceEnum.nominal,
    )

    assert summary["available_by_confidence"]["mystery"] == 1
    assert summary["available_total"] == 2
    assert summary["sent_total"] == 1
    assert "mystery" not in summary["sent_by_confidence"]


@pytest.mark.parametrize("cat,expected", [("l", "low"), ("n", "nominal"), ("h", "high"), ("x", "x")])
def test_fire_alert_confidence_expands_single_letter_category(cat, expected):
    assert fire_alert_confidence(make_fire(cat)) == expected


def test_fire_summary_uses_expanded_labels():
    alerts = [make_fire("l"), make_fire("n"), make_fire("h")]

    summary = summarize_by_confidence(
        alerts,
        confidence_of=fire_alert_confidence,
        confidence_order=NasaViirsFireAlertConfidenceEnumOrder,
        lowest_confidence=NasaViirsFireAlertConfidenceEnum.high,
    )

    assert summary["available_by_confidence"] == {"low": 1, "nominal": 1, "high": 1}
    assert summary["sent_by_confidence"] == {"high": 1}
    assert summary["samples"]["high"][0]["confidence"] == "high"


from app.actions.utils import format_sample_report


def sample_result():
    return {
        "aoi_id": "aoi-1",
        "aoi_name": "Test AOI",
        "geostore_id": "geo-1",
        "sampled_at": "2026-10-12T00:00:00+00:00",
        "fire_alerts": {
            "enabled": True,
            "dataset": "nasa_viirs_fire_alerts",
            "lookback_days": 5,
            "lowest_confidence": "high",
            "available_total": 163,
            "sent_total": 3,
            "available_by_confidence": {"low": 40, "nominal": 120, "high": 3},
            "sent_by_confidence": {"high": 3},
            "samples": {"low": [{"latitude": 1.0, "longitude": 2.0, "date": "2026-10-01", "confidence": "low"}], "nominal": [], "high": []},
            "dataset_updated_on": "2026-10-11T00:00:00+00:00",
            "last_processed_dataset_update": "2026-10-10T00:00:00+00:00",
            "quiet_period_active": True,
        },
        "integrated_alerts": {"enabled": False},
        "message": "Fire alerts: 163 available in last 5 days, 3 pass 'high'. Integrated alerts: disabled.",
    }


def test_report_shows_available_and_sent_per_confidence_level():
    report = format_sample_report(sample_result())

    assert "Test AOI" in report
    assert "geo-1" in report
    lines = [line.split() for line in report.splitlines() if line.strip().startswith(("low", "nominal", "high", "total"))]
    assert ["low", "40", "-"] in lines
    assert ["nominal", "120", "-"] in lines
    assert ["high", "3", "3"] in lines
    assert ["total", "163", "3"] in lines


def test_report_shows_pipeline_state_and_disabled_feed():
    report = format_sample_report(sample_result())

    assert "quiet period: active" in report
    assert "dataset updated: 2026-10-11" in report
    assert "last processed: 2026-10-10" in report
    assert "Integrated alerts: disabled" in report


def test_report_shows_feed_error_and_top_level_error():
    result = sample_result()
    result["fire_alerts"] = {"enabled": True, "lookback_days": 5, "error": "GFW timed out"}
    assert "GFW timed out" in format_sample_report(result)

    assert "no geostore" in format_sample_report({"error": "no geostore"})


from app.actions.utils import unique_alerts


def test_unique_alerts_drops_duplicates_fetched_from_overlapping_date_chunks():
    boundary = make_integrated("highest", lat=1.0, lon=2.0, date="2026-10-05")
    same_boundary = make_integrated("highest", lat=1.0, lon=2.0, date="2026-10-05")
    other_day = make_integrated("highest", lat=1.0, lon=2.0, date="2026-10-06")
    other_place = make_integrated("highest", lat=1.5, lon=2.0, date="2026-10-05")
    other_level = make_integrated("high", lat=1.0, lon=2.0, date="2026-10-05")

    result = unique_alerts([boundary, same_boundary, other_day, other_place, other_level], confidence_of=integrated_alert_confidence)

    assert len(result) == 4
    assert result[0] is boundary
