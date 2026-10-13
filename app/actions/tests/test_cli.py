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
