import pydantic

from datetime import datetime

from app.actions.core import PullActionConfiguration, AuthActionConfiguration, ExecutableActionMixin, InternalActionConfiguration
from app.actions.gfwclient import AOIData, DatasetResponseItem, IntegratedAlertsConfidenceEnum, NasaViirsFireAlertConfidenceEnum
from app.services.errors import ConfigurationNotFound
from app.services.utils import find_config_for_action, FieldWithUIOptions, UIOptions, GlobalUISchemaOptions


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

    fire_lookback_days: int = FieldWithUIOptions(
        5,
        le=10,
        ge=1,
        title="Fire alerts lookback days",
        description="Number of days to look back for fire alerts.",
        ui_options=UIOptions(
            widget="range",  # This will be rendered ad a range slider
        )
    )
    integrated_alerts_lookback_days: int = FieldWithUIOptions(
        15,
        le=30,
        ge=1,
        title="Integrated deforestation alerts lookback days",
        description="Number of days to look back for integrated deforestation alerts.",
        ui_options = UIOptions(
            widget="range",  # This will be rendered ad a range slider
        )
    )

    force_fetch: bool = pydantic.Field(
        False,
        title="Force fetch",
        description="Force fetch even if in a quiet period."
    )

    partition_interval_size_in_degrees: float = FieldWithUIOptions(
        1.0,
        title="Partition interval size in degrees",
        description="Size of the partition interval in degrees.",
        le=1.0,
        ge=0.1,
        multiple_of=0.01,
        # ui_options=UIOptions(
        #     widget="range",  # This will be rendered ad a range slider
        # )
        # TODO: Check the hardcoded step in the UI
    )

    max_partitions: int = FieldWithUIOptions(
        10,
        title="Maximum partitions per AOI",
        description="Maximum number of geometry partitions to create for large areas. Higher values create more partitions but may increase processing time.",
        le=50,
        ge=1,
        ui_options=UIOptions(
            widget="range",  # This will be rendered as a range slider
        )
    )

    ui_global_options: GlobalUISchemaOptions = GlobalUISchemaOptions(
        order=[
            "gfw_share_link_url",
            "partition_interval_size_in_degrees",
            "max_partitions",
            "include_fire_alerts",
            "fire_lookback_days",
            "fire_alerts_lowest_confidence",
            "include_integrated_alerts",
            "integrated_alerts_lookback_days",
            "integrated_alerts_lowest_confidence",
            "force_fetch"
        ],
    )

    class Config:
        @staticmethod
        def schema_extra(schema: dict):
            # Remove lookback days and confidence from the root properties
            schema["properties"].pop("fire_alerts_lowest_confidence", None)
            schema["properties"].pop("fire_lookback_days", None)
            schema["properties"].pop("integrated_alerts_lookback_days", None)
            schema["properties"].pop("integrated_alerts_lowest_confidence", None)

            # Show region_code OR latitude & longitude & distance based on search_parameter
            schema.update({
                "allOf": [{
                    "if": {
                        "properties": {
                            "include_fire_alerts": {
                                "const": True
                            }
                        }
                    },
                    "then": {
                        "required": ["fire_lookback_days", "fire_alerts_lowest_confidence"],
                        "properties": {
                            "fire_lookback_days": {
                                "type": "integer",
                                "title": "Fire alerts lookback days",
                                "default": 5,
                                "maximum": 10,
                                "minimum": 1,
                                "description": "Number of days to look back for fire alerts."
                            },
                            "fire_alerts_lowest_confidence": {
                                "allOf": [{
                                    "$ref": "#/definitions/NasaViirsFireAlertConfidenceEnum"
                                }],
                                "title": "Fire alerts lowest confidence",
                                "default": "high",
                                "description": "Lowest confidence level to include in the connection."
                            }
                        }
                    }
                }, {
                    "if": {
                        "properties": {
                            "include_integrated_alerts": {
                                "const": True
                            }
                        }
                    },
                    "then": {
                        "required": ["integrated_alerts_lookback_days", "integrated_alerts_lowest_confidence"],
                        "properties": {
                            "integrated_alerts_lookback_days": {
                                "type": "integer",
                                "title": "Integrated deforestation alerts lookback days",
                                "default": 15,
                                "maximum": 30,
                                "minimum": 1,
                                "description": "Number of days to look back for integrated deforestation alerts."
                            },
                            "integrated_alerts_lowest_confidence": {
                                "allOf": [{
                                    "$ref": "#/definitions/IntegratedAlertsConfidenceEnum"
                                }],
                                "title": "Integrated deforestation alerts lowest confidence",
                                "default": "highest",
                                "description": "Lowest confidence level to include in the connection."
                            }
                        }
                    }
                }]
            })


class GetDatasetAndGeostoresConfig(InternalActionConfiguration):
    integration_id: str
    pull_events_config: PullEventsConfig
    aoi_data: AOIData


class GetNasaVIIRSFireAlertsForGeostoreID(InternalActionConfiguration):
    integration_id: str
    geostore_id: str
    date_range: tuple[datetime, datetime]
    lowest_confidence: NasaViirsFireAlertConfidenceEnum
    dataset: DatasetResponseItem


class GetIntegratedAlertsForGeostoreID(InternalActionConfiguration):
    integration_id: str
    geostore_id: str
    date_range: tuple[datetime, datetime]
    lowest_confidence: IntegratedAlertsConfidenceEnum
    dataset: DatasetResponseItem
