from click.testing import CliRunner

from cli import cli


def test_both_fire_alert_commands_are_registered_under_distinct_names():
    result = CliRunner().invoke(cli, ["--help"])

    assert result.exit_code == 0
    assert "nasa-viirs-fire-alerts " in result.output or "nasa-viirs-fire-alerts\n" in result.output
    assert "nasa-viirs-fire-alerts-by-geostore" in result.output


def test_url_based_fire_alert_command_still_takes_a_url():
    result = CliRunner().invoke(cli, ["nasa-viirs-fire-alerts", "--help"])

    assert result.exit_code == 0
    assert "URL" in result.output
    assert "--lowest_confidence" in result.output


def test_sample_integration_rejects_out_of_range_options_as_usage_errors():
    days = CliRunner().invoke(cli, ["sample-integration", "some-id", "--days", "60"])
    size = CliRunner().invoke(cli, ["sample-integration", "some-id", "--sample-size", "99"])

    assert days.exit_code == 2 and "Invalid value" in days.output
    assert size.exit_code == 2 and "Invalid value" in size.output


import json
from unittest.mock import AsyncMock, MagicMock

from app.actions.gfwclient import DataAPIKey


def test_optimized_integrated_alerts_command_is_gone():
    # Its helpers (performance_monitor, optimize_geometry_partitioning,
    # get_gfw_integrated_alerts_optimized) do not exist in this checkout.
    result = CliRunner().invoke(cli, ["--help"])

    assert "gfw-integrated-alerts-optimized" not in result.output


def test_get_api_keys_prints_the_key_list_as_json(mocker):
    key = DataAPIKey.parse_obj({
        "created_on": "2026-09-05T23:15:00.000Z", "updated_on": "2026-09-05T23:15:00.000Z",
        "user_id": "er_user", "expires_on": "2026-12-05T23:15:00.000Z", "api_key": "1234567890",
        "alias": "test_key", "email": "test@example.com", "organization": "EarthRanger", "domains": [],
    })
    api = MagicMock()
    api.get_api_keys = AsyncMock(return_value=[key])
    mocker.patch("cli.DataAPI", return_value=api)

    result = CliRunner().invoke(cli, ["get-api-keys", "--username", "u", "--password", "p"])

    assert result.exit_code == 0, result.output
    assert json.loads(result.output)[0]["api_key"] == "1234567890"


def test_sample_integration_as_json_writes_only_json_to_stdout(mocker):
    gundi = MagicMock()
    gundi.__aenter__ = AsyncMock(return_value=gundi)
    gundi.__aexit__ = AsyncMock(return_value=False)
    gundi.get_integration_details = AsyncMock(return_value=MagicMock(id="int-1", name="Test"))
    mocker.patch("gundi_client_v2.GundiClient", return_value=gundi)
    mocker.patch("app.actions.handlers.sample_integration_data", AsyncMock(return_value={"aoi_id": "aoi-1", "message": "ok"}))

    result = CliRunner(mix_stderr=False).invoke(cli, ["sample-integration", "int-1", "--as-json"])

    assert result.exit_code == 0, result.output
    assert json.loads(result.stdout)["aoi_id"] == "aoi-1"
