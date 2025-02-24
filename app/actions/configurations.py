import pydantic

from datetime import datetime

from app.actions.core import PullActionConfiguration, AuthActionConfiguration, ExecutableActionMixin
from app.actions.gfwclient import AOIData, DatasetResponseItem, IntegratedAlertsConfidenceEnum, NasaViirsFireAlertConfidenceEnum
from app.services.errors import ConfigurationNotFound
from app.services.utils import find_config_for_action


class AuthenticateConfig(AuthActionConfiguration, ExecutableActionMixin):
    email: str
    password: pydantic.SecretStr = pydantic.Field(..., format="password", 
                                                  title="Password",
                                                  description="Password for the Global Forest Watch account.")


def get_auth_config(integration):
    # Look for the login credentials, needed for any action
    auth_config = find_config_for_action(
        configurations=integration.configurations,
        action_id="auth"
    )
    if not auth_config:
        raise ConfigurationNotFound(
            f"Authentication settings for integration {str(integration.id)} "
            f"are missing. Please fix the integration setup in the portal."
        )
    return AuthenticateConfig.parse_obj(auth_config.data)


class PullEventsConfig(PullActionConfiguration):

    gfw_share_link_url: pydantic.HttpUrl = pydantic.Field(
        ...,
        title="Global Forest Watch AOI Share Link",
        description="AOI share link from your MyGFW dashboard."
    )
    include_fire_alerts: bool = pydantic.Field(
        True,
        title="Include fire alerts",
        description="Fetch fire alerts from Global Forest Watch and include them in this connection."
    )

    fire_alerts_lowest_confidence: NasaViirsFireAlertConfidenceEnum = pydantic.Field(
        NasaViirsFireAlertConfidenceEnum.high,
        title="Fire alerts lowest confidence",
        description="Lowest confidence level to include in the connection."
    )

    include_integrated_alerts: bool = pydantic.Field(
        True,
        title="Include integrated deforestation alerts",
        description="Fetch integrated deforestation alerts from Global Forest Watch and include them in the connection."
    )

    integrated_alerts_lowest_confidence: IntegratedAlertsConfidenceEnum = pydantic.Field(
        IntegratedAlertsConfidenceEnum.highest,
        title="Integrated deforestation alerts lowest confidence",
        description="Lowest confidence level to include in the connection."
    )

    fire_lookback_days: int = pydantic.Field(
        10,
        le=10,
        ge=1,
        title="Fire alerts lookback days",
        description="Number of days to look back for fire alerts."
    )
    integrated_alerts_lookback_days: int = pydantic.Field(
        30,
        le=30,
        ge=1,
        title="Integrated deforestation alerts lookback days",
        description="Number of days to look back for integrated deforestation alerts."
    )

    force_fetch: bool = pydantic.Field(
        False,
        title="Force fetch",
        description="Force fetch even if in a quiet period."
    )


class GetDatasetAndGeostoresConfig(PullActionConfiguration):
    integration_id: str
    pull_events_config: PullEventsConfig
    auth_config: AuthenticateConfig
    aoi_data: AOIData


class GetNasaVIIRSFireAlertsForGeostoreID(PullActionConfiguration):
    integration_id: str
    geostore_id: str
    auth_config: AuthenticateConfig
    date_range: tuple[datetime, datetime]
    lowest_confidence: NasaViirsFireAlertConfidenceEnum
    dataset: DatasetResponseItem


class GetIntegratedAlertsForGeostoreID(PullActionConfiguration):
    integration_id: str
    geostore_id: str
    auth_config: AuthenticateConfig
    date_range: tuple[datetime, datetime]
    lowest_confidence: IntegratedAlertsConfidenceEnum
    dataset: DatasetResponseItem
