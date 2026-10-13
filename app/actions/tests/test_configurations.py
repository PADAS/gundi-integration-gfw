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
