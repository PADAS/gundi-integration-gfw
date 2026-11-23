import json
from unittest.mock import AsyncMock, MagicMock

import pydantic
import pytest

from app.actions.configurations import AuthenticateConfig
from app.actions.handlers import action_auth
from app.services.errors import ConfigurationNotFound


SHARED_EMAIL = "shared@example.com"
SHARED_PASSWORD = "shared-password"


@pytest.fixture
def shared_account(mocker):
    mocker.patch("app.actions.configurations.settings.GNW_DEFAULT_EMAIL", SHARED_EMAIL)
    mocker.patch("app.actions.configurations.settings.GNW_DEFAULT_PASSWORD", SHARED_PASSWORD)


@pytest.fixture
def no_shared_account(mocker):
    mocker.patch("app.actions.configurations.settings.GNW_DEFAULT_EMAIL", None)
    mocker.patch("app.actions.configurations.settings.GNW_DEFAULT_PASSWORD", None)


def test_new_config_uses_the_shared_account(shared_account):
    config = AuthenticateConfig.parse_obj({})

    assert config.use_own_credentials is False
    assert config.data_api_credentials() == {"username": SHARED_EMAIL, "password": SHARED_PASSWORD}
    assert config.login == "gundi-default-account"


def test_saved_config_with_a_login_keeps_using_it(shared_account):
    # Configs saved before the toggle existed have no use_own_credentials key.
    config = AuthenticateConfig.parse_obj({"email": "ranger@example.com", "password": "pw"})

    assert config.use_own_credentials is True
    assert config.data_api_credentials() == {"username": "ranger@example.com", "password": "pw"}
    assert config.login == "ranger@example.com"


def test_toggle_off_ignores_a_leftover_login(shared_account):
    config = AuthenticateConfig.parse_obj(
        {"use_own_credentials": False, "email": "ranger@example.com", "password": "pw"}
    )

    assert config.data_api_credentials() == {"username": SHARED_EMAIL, "password": SHARED_PASSWORD}


@pytest.mark.parametrize("data", [
    {"use_own_credentials": True},
    {"use_own_credentials": True, "email": "ranger@example.com"},
    {"use_own_credentials": True, "password": "pw"},
])
def test_own_account_needs_email_and_password(data):
    with pytest.raises(pydantic.ValidationError):
        AuthenticateConfig.parse_obj(data)


def test_missing_shared_account_raises_a_clear_error(no_shared_account):
    with pytest.raises(ConfigurationNotFound, match="Use my own Global Nature Watch account"):
        AuthenticateConfig.parse_obj({}).data_api_credentials()


def test_schema_only_shows_the_login_fields_when_the_toggle_is_on():
    # schema_json() is what registration sends to the portal.
    schema = json.loads(AuthenticateConfig.schema_json())

    assert list(schema["properties"]) == ["use_own_credentials"]
    conditional = schema["allOf"][0]
    assert conditional["if"] == {"properties": {"use_own_credentials": {"const": True}}}
    assert conditional["then"]["required"] == ["email", "password"]
    assert set(conditional["then"]["properties"]) == {"email", "password"}
    assert conditional["then"]["properties"]["password"]["format"] == "password"


@pytest.mark.asyncio
async def test_auth_signs_in_with_the_shared_account(mocker, integration_v2, shared_account):
    api = MagicMock()
    api.get_access_token = AsyncMock(return_value="tok")
    data_api = mocker.patch("app.actions.handlers.DataAPI", return_value=api)

    result = await action_auth(integration=integration_v2, action_config=AuthenticateConfig.parse_obj({}))

    assert result == {"valid_credentials": True}
    data_api.assert_called_once_with(username=SHARED_EMAIL, password=SHARED_PASSWORD)


@pytest.mark.asyncio
async def test_auth_reports_a_missing_shared_account(mocker, integration_v2, no_shared_account):
    data_api = mocker.patch("app.actions.handlers.DataAPI")

    result = await action_auth(integration=integration_v2, action_config=AuthenticateConfig.parse_obj({}))

    assert result["valid_credentials"] is False
    assert "Use my own Global Nature Watch account" in result["message"]
    data_api.assert_not_called()
