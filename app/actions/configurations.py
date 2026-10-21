import pydantic

from datetime import datetime
from typing import Optional

from app.actions.core import PullActionConfiguration, AuthActionConfiguration, ExecutableActionMixin, InternalActionConfiguration, GenericActionConfiguration
from app.actions.gfwclient import AOIData, DatasetResponseItem, IntegratedAlertsConfidenceEnum, IntegratedAlertsConfidenceEnumOrder, \
    NasaViirsFireAlertConfidenceEnum, NasaViirsFireAlertConfidenceEnumOrder
from app.services.errors import ConfigurationNotFound, ConfigurationValidationError
from app.services.utils import find_config_for_action, FieldWithUIOptions, UIOptions, GlobalUISchemaOptions


class AuthenticateConfig(AuthActionConfiguration, ExecutableActionMixin):
    email: str
    password: pydantic.SecretStr = pydantic.Field(..., format="password", 
                                                  title="Password",
                                                  description="Password for the Global Nature Watch account.")


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


# What each confidence level means, shown under the option in the portal dropdown.
# Fire levels follow NASA FIRMS' VIIRS 375m confidence classes; integrated levels
# follow GFW's integrated deforestation alerts (GLAD-L, GLAD-S2, RADD).
FIRE_ALERT_CONFIDENCE_DESCRIPTIONS = {
    NasaViirsFireAlertConfidenceEnum.low: "Daytime detections with sun glint or a weak temperature anomaly. Most likely to be false positives.",
    NasaViirsFireAlertConfidenceEnum.nominal: "No sun glint and a strong temperature anomaly, day or night.",
    NasaViirsFireAlertConfidenceEnum.high: "Saturated pixels, day or night. The most certain detections.",
}

INTEGRATED_ALERT_CONFIDENCE_DESCRIPTIONS = {
    IntegratedAlertsConfidenceEnum.nominal: "Detected once by a single alert system.",
    IntegratedAlertsConfidenceEnum.high: "Detected more than once by a single alert system.",
    IntegratedAlertsConfidenceEnum.highest: "Detected by more than one alert system.",
}


def described_choices(definition: dict, order: list, descriptions: dict) -> dict:
    """
    Rewrite an enum definition as oneOf [{const, title, description}] in rank order,
    so the portal dropdown lists the levels lowest-first with a description under each.
    Stored values are unchanged: each const is the enum value.
    """
    choices = [
        {"const": level.value, "title": level.value.capitalize(), "description": descriptions[level]}
        for level in order
    ]
    return {key: value for key, value in definition.items() if key != "enum"} | {"oneOf": choices}


class PullEventsConfig(PullActionConfiguration):

    gfw_share_link_url: pydantic.HttpUrl = pydantic.Field(
        ...,
        title="Global Nature Watch AOI Share Link",
        description="AOI share link from your Global Nature Watch dashboard."
    )
    include_fire_alerts: bool = pydantic.Field(
        True,
        title="Include fire alerts",
        description="Fetch fire alerts from Global Nature Watch and include them in this connection."
    )

    fire_alerts_lowest_confidence: NasaViirsFireAlertConfidenceEnum = pydantic.Field(
        NasaViirsFireAlertConfidenceEnum.high,
        title="Fire alerts lowest confidence",
        description="Lowest confidence level to include in the connection."
    )

    include_integrated_alerts: bool = pydantic.Field(
        True,
        title="Include integrated deforestation alerts",
        description="Fetch integrated deforestation alerts from Global Nature Watch and include them in the connection."
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

    ui_global_options: GlobalUISchemaOptions = GlobalUISchemaOptions(
        order=[
            "gfw_share_link_url",
            "include_fire_alerts",
            "fire_lookback_days",
            "fire_alerts_lowest_confidence",
            "include_integrated_alerts",
            "integrated_alerts_lookback_days",
            "integrated_alerts_lowest_confidence",
            "force_fetch",
            "run_on_schedule",  # template toggle from PullActionConfiguration
            # react-jsonschema-form throws if any property is missing from
            # ui:order; "*" places any future template fields after ours.
            "*",
        ],
    )

    @classmethod
    def schema(cls, **kwargs):
        # Present each confidence enum as described choices. Done here rather than
        # in schema_extra, which pydantic runs before it attaches "definitions".
        json_schema_dict = super().schema(**kwargs)
        definitions = json_schema_dict.get("definitions", {})
        for name, order, descriptions in (
            ("NasaViirsFireAlertConfidenceEnum", NasaViirsFireAlertConfidenceEnumOrder, FIRE_ALERT_CONFIDENCE_DESCRIPTIONS),
            ("IntegratedAlertsConfidenceEnum", IntegratedAlertsConfidenceEnumOrder, INTEGRATED_ALERT_CONFIDENCE_DESCRIPTIONS),
        ):
            if name in definitions:
                definitions[name] = described_choices(definitions[name], order, descriptions)
        return json_schema_dict

    class Config:
        @staticmethod
        def schema_extra(schema: dict):
            # Remove conditional fields from the root properties
            schema["properties"].pop("fire_alerts_lowest_confidence", None)
            schema["properties"].pop("fire_lookback_days", None)
            schema["properties"].pop("integrated_alerts_lookback_days", None)
            schema["properties"].pop("integrated_alerts_lowest_confidence", None)

            # Show conditional fields based on include_* toggles
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


class GetFireAlertsDatasetAndGeostoresConfig(InternalActionConfiguration):
    integration_id: str
    pull_events_config: PullEventsConfig
    aoi_data: AOIData


class GetIntegratedAlertsDatasetAndGeostoresConfig(InternalActionConfiguration):
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


class SampleDataConfig(GenericActionConfiguration, ExecutableActionMixin):
    """
    Diagnostic action: query GFW for everything in the AOI (no confidence filter)
    and report what exists vs. what this connection's settings would send on.
    """
    lookback_days: Optional[int] = pydantic.Field(
        None,
        ge=1,
        le=30,
        title="Lookback days (optional)",
        description="Days to sample for both feeds. Leave empty to use each feed's configured lookback."
    )
    sample_size: int = pydantic.Field(
        5,
        ge=0,
        le=25,
        title="Sample records per confidence level",
        description="How many example records to include for each confidence level."
    )


def get_pull_events_config(integration) -> PullEventsConfig:
    """Load the integration's pull_events config, or raise with a reason support can act on."""
    config = find_config_for_action(
        configurations=integration.configurations,
        action_id="pull_events"
    )
    if not config:
        raise ConfigurationNotFound(
            f"Integration {str(integration.id)} has no 'pull_events' configuration to sample against."
        )
    try:
        return PullEventsConfig.parse_obj(config.data)
    except pydantic.ValidationError as e:
        raise ConfigurationValidationError(
            f"Invalid 'pull_events' configuration for integration {str(integration.id)}: {e}"
        )
