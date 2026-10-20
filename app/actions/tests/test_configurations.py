import json

import pytest

from app.actions.configurations import PullEventsConfig, SampleDataConfig, AuthenticateConfig


@pytest.mark.parametrize("model", [PullEventsConfig, SampleDataConfig, AuthenticateConfig])
def test_ui_order_covers_every_schema_property(model):
    # react-jsonschema-form throws "uiSchema order list does not contain
    # property X" for any property missing from ui:order unless the list has a
    # "*" wildcard. Template fields such as run_on_schedule get added to the
    # base config over time, so the order must tolerate them.
    properties = set(model.schema()["properties"])
    order = model.ui_schema().get("ui:order")
    if not order:
        return
    assert "*" in order or properties <= set(order), properties - set(order)


def test_pull_events_ui_order_keeps_the_share_link_first():
    assert PullEventsConfig.ui_schema()["ui:order"][0] == "gfw_share_link_url"


@pytest.mark.parametrize("definition,expected_order", [
    ("NasaViirsFireAlertConfidenceEnum", ["low", "nominal", "high"]),
    ("IntegratedAlertsConfidenceEnum", ["nominal", "high", "highest"]),
])
def test_confidence_choices_are_described_in_rank_order(definition, expected_order):
    # The portal shows each oneOf entry's description under its option, and
    # lists the entries in this order. schema_json() is what registration sends.
    choices = json.loads(PullEventsConfig.schema_json())["definitions"][definition]
    assert "enum" not in choices
    assert [choice["const"] for choice in choices["oneOf"]] == expected_order
    assert all(choice["title"] and choice["description"] for choice in choices["oneOf"])


def test_saved_confidence_values_still_parse():
    config = PullEventsConfig.parse_obj({
        "gfw_share_link_url": "https://www.globalforestwatch.org/dashboards/aoi/abc",
        "fire_alerts_lowest_confidence": "nominal",
        "integrated_alerts_lowest_confidence": "high",
    })
    assert config.fire_alerts_lowest_confidence == "nominal"
    assert config.integrated_alerts_lowest_confidence == "high"
