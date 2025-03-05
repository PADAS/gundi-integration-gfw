import asyncio
import httpx
import logging
import random
import app.settings

from app.actions import utils
from app.actions.gfwclient import DataAPI, Geostore, DatasetStatus, \
    AOIData, DATASET_GFW_INTEGRATED_ALERTS, DATASET_NASA_VIIRS_FIRE_ALERTS, DataAPIAuthException
from shapely.geometry import GeometryCollection, shape, mapping
from datetime import timezone, timedelta, datetime

from app.actions.configurations import (
    AuthenticateConfig,
    PullEventsConfig,
    get_auth_config,
    GetDatasetAndGeostoresConfig,
    GetNasaVIIRSFireAlertsForGeostoreID,
    GetIntegratedAlertsForGeostoreID
)
from app.services.action_scheduler import trigger_action
from app.services.activity_logger import activity_logger, log_action_activity
from app.services.gundi import send_events_to_gundi
from app.services.state import IntegrationStateManager

from gundi_core.schemas.v2 import Integration, LogLevel

GFW_INTEGRATED_ALERTS = "gfwgladalert"
GFW_FIRE_ALERT = "gfwfirealert"

MAX_DAYS_PER_QUERY = 2

logger = logging.getLogger(__name__)
state_manager = IntegrationStateManager()


# This semaphore is meant to limit the concurrent requests to GFW's dataset API query endpoints.
# When configuring a cloud run service, include this in a calculation so that
# GFW_DATASET_QUERY_CONCURRENCY * maximum-number-of-instances * maximum-concurrent-requests-per-instance <= N
# where N is the maximum concurrent requests allowed by GFW's API.
# (ex. in practice, N is around 50)
sema = asyncio.Semaphore(app.settings.GFW_DATASET_QUERY_CONCURRENCY)


async def handle_transformed_data(transformed_data, integration_id, action_id):
    try:
        response = await send_events_to_gundi(
            events=transformed_data,
            integration_id=integration_id
        )
    except httpx.HTTPError as e:
        msg = f'Sensors API returned error for integration_id: {integration_id}. Exception: {e}'
        logger.exception(
            msg,
            extra={
                'needs_attention': True,
                'integration_id': integration_id,
                'action_id': action_id
            }
        )
        return {"error": msg}
    else:
        return response


def transform_fire_alert(alert):
    event_time = alert.alert_date.replace(tzinfo=timezone.utc).isoformat()
    title = "GFW VIIRS Alert"

    return dict(
        title=title,
        event_type=GFW_FIRE_ALERT,
        recorded_at=event_time,
        location={"lat": alert.latitude, "lon": alert.longitude},
        event_details=dict(
            confidence=alert.confidence,
            alert_time=event_time
        )
    )


def transform_integrated_alert(alert):
    title = ("GFW Integrated Deforestation Alert")

    return dict(
        title=title,
        event_type=GFW_INTEGRATED_ALERTS,
        recorded_at=alert.recorded_at,
        location={"lat": alert.latitude, "lon": alert.longitude},
        event_details=dict(
            confidence=alert.confidence
        )
    )


async def action_auth(integration, action_config: AuthenticateConfig):
    logger.info(f"Executing auth action with integration {integration} and action_config {action_config}...")
    try:
        dataapi = DataAPI(username=action_config.email, password=action_config.password.get_secret_value())
        token = await dataapi.get_access_token()
    except DataAPIAuthException as e:
        return {"valid_credentials": False, "message": f"Failed to authenticate with Global Forest Watch Data API: {e}"}
    else:
        logger.info(f"Authenticated with success. token: {token}")
    
    return {"valid_credentials": token is not None}


@activity_logger()
async def action_pull_events(integration: Integration, action_config: PullEventsConfig):
    result = {}

    if not action_config.force_fetch and await state_manager.is_quiet_period(str(integration.id), "pull_events"):
        result["message"] = "Quiet period is active."
        return result


    logger.info(f"Executing 'pull_events' action with integration {integration} and action_config {action_config}...")

    auth_config = get_auth_config(integration)

    # Get AOI data first.
    dataapi = DataAPI(username=auth_config.email, password=auth_config.password.get_secret_value())

    aoi_id = await dataapi.aoi_from_url(action_config.gfw_share_link_url)
    aoi_data = await dataapi.get_aoi(aoi_id=aoi_id)

    # Some AOIs do not have an associated Geostore so we short-circuit here a report in the logs.
    if not aoi_data.attributes.geostore:
        msg = f"No Geostore associated with AOI {aoi_data.id}."
        logger.error(
            msg,
            extra={
                "needs_attention": True,
                "integration_id": str(integration.id),
                "aoi_id": aoi_data.id,
                "gfw_url": integration.base_url,
            },
        )
        await log_action_activity(
            integration_id=integration.id,
            action_id="pull_events",
            level=LogLevel.ERROR,
            title=msg,
            data={"aoi_data": aoi_data.dict()}
        )
        result["message"] = msg
        return result

    # Get AOI and Geostore data.
    geostore_ids = await state_manager.get_geostore_ids(aoi_data.id)

    if not geostore_ids:
        geostore:Geostore = await dataapi.get_geostore(geostore_id=aoi_data.attributes.geostore)

        geometry_collection = GeometryCollection(
            [
                shape(feature["geometry"]).buffer(0)
                for feature in geostore.attributes.geojson["features"]
            ]
        )

        try:
            for partition in utils.generate_geometry_fragments(geometry_collection=geometry_collection, 
                                                           interval=action_config.partition_interval_size_in_degrees):
                try:
                    geostore = await dataapi.create_geostore(geometry=mapping(partition))
                except AttributeError:
                    msg = f"Error while creating Geostore for Geometry Collection (invalid partition)."
                    logger.exception(msg)
                    await log_action_activity(
                        integration_id=integration.id,
                        action_id="pull_events",
                        level=LogLevel.WARNING,
                        title=msg,
                        data={"aoi_data": aoi_data.dict(), "geometry_collection": geometry_collection.wkt}
                    )
                else:
                    await state_manager.add_geostore_id(aoi_data.id, geostore.gfw_geostore_id)
        except ValueError:
            msg = f"Error while generating geometry fragments for Geometry Collection."
            logger.exception(msg)
            await log_action_activity(
                integration_id=integration.id,
                action_id="pull_events",
                level=LogLevel.WARNING,
                title=msg,
                data={"aoi_data": aoi_data.dict(), "geometry_collection": geometry_collection.wkt}
            )

        await state_manager.set_geostores_id_ttl(aoi_data.id, 86400*7)

    # Trigger "get_dataset_and_geostores" sub-action
    config = GetDatasetAndGeostoresConfig(
        integration_id=str(integration.id),
        pull_events_config=action_config,
        aoi_data=aoi_data
    )
    await trigger_action(integration.id, "get_dataset_and_geostores", config=config)

    quiet_minutes = random.randint(240, 720) # Todo: change to be more fair.
    await state_manager.set_quiet_period(str(integration.id), "pull_events", timedelta(minutes=quiet_minutes))

    result["message"] = "'get_dataset_and_geostores' action triggered successfully."
    return result


def generate_date_pairs(lower_date, upper_date, interval=MAX_DAYS_PER_QUERY):
    while upper_date > lower_date:
        yield max(lower_date, upper_date - timedelta(days=interval)), upper_date
        upper_date -= timedelta(days=interval)


async def action_get_integrated_alerts_for_geostore_and_date_range(
        integration:Integration,
        action_config: GetIntegratedAlertsForGeostoreID
):
    total_alerts = 0
    auth_config = get_auth_config(integration)
    dataapi = DataAPI(
        username=auth_config.email,
        password=auth_config.password.get_secret_value()
    )

    integrated_alerts = await dataapi.get_gfw_integrated_alerts(
        geostore_id=action_config.geostore_id,
        date_range=(action_config.date_range[0], action_config.date_range[1]),
        lowest_confidence=action_config.lowest_confidence,
        semaphore=sema
    )

    if integrated_alerts:
        logger.info(f"Integrated alerts pulled with success.")
        transformed_data = [transform_integrated_alert(alert) for alert in integrated_alerts]
        await handle_transformed_data(
            transformed_data,
            str(integration.id),
            "pull_events"
        )
        total_alerts += len(integrated_alerts)

    dataset_status = DatasetStatus(
        dataset=action_config.dataset.dataset,
        version=action_config.dataset.version,
        latest_updated_on=action_config.dataset.updated_on
    )

    return {"dataset": DATASET_GFW_INTEGRATED_ALERTS, "response": dataset_status.dict(), "total_alerts": total_alerts}


async def action_get_dataset_and_geostores(integration: Integration, action_config: GetDatasetAndGeostoresConfig):
    auth_config = get_auth_config(integration)
    dataapi = DataAPI(
        username=auth_config.email,
        password=auth_config.password.get_secret_value()
    )

    fire_dataset_metadata = None
    integrated_dataset_metadata = None

    fire_alerts_actions_triggered = 0
    integrated_alerts_actions_triggered = 0

    geostore_ids = await state_manager.get_geostore_ids(action_config.aoi_data.id)

    # Date ranges are in whole days, so we round to next midnight.
    end_date = (datetime.now(tz=timezone.utc) + timedelta(days=1)).replace(hour=0, minute=0, second=0, microsecond=0)
    start_date = end_date - timedelta(days=action_config.pull_events_config.integrated_alerts_lookback_days)

    if action_config.pull_events_config.include_fire_alerts:
        fire_dataset_metadata = await dataapi.get_dataset_metadata(DATASET_NASA_VIIRS_FIRE_ALERTS)
        fire_dataset_status = await state_manager.get_state(
            str(integration.id),
            "pull_events",
            DATASET_NASA_VIIRS_FIRE_ALERTS
        )

        if fire_dataset_status:
            logger.info(f"Saved fire dataset status: {fire_dataset_status}")
            fire_dataset_status = DatasetStatus.parse_obj(fire_dataset_status)
        else:
            fire_dataset_status = DatasetStatus(
                dataset=fire_dataset_metadata.dataset,
                version=fire_dataset_metadata.version,
            )

            # If I've saved a status for this dataset, compare 'updated_on' timestamp to avoid redundant queries.
        if not action_config.pull_events_config.force_fetch and fire_dataset_status.latest_updated_on >= fire_dataset_metadata.updated_on:
            msg = f"No updates reported for dataset '{DATASET_NASA_VIIRS_FIRE_ALERTS}' so skipping nasa_viirs_fire_alerts queries"
            logger.info(
                msg,
                extra={
                    "integration_id": str(integration.id),
                    "integration_login": auth_config.email,
                    "dataset_updated_on": fire_dataset_metadata.updated_on.isoformat(),
                },
            )
            await log_action_activity(
                integration_id=integration.id,
                action_id="get_dataset_and_geostores",
                level=LogLevel.INFO,
                title=msg,
                data={"dataset_updated_on": fire_dataset_metadata.updated_on.isoformat()}
            )
            fire_dataset_metadata = None

    if action_config.pull_events_config.include_integrated_alerts:
        integrated_dataset_metadata = await dataapi.get_dataset_metadata(DATASET_GFW_INTEGRATED_ALERTS)
        integrated_dataset_status = await state_manager.get_state(
            str(integration.id),
            "pull_events",
            DATASET_GFW_INTEGRATED_ALERTS
        )

        if integrated_dataset_status:
            logger.info(f"Saved integrated dataset status: {integrated_dataset_status}")
            integrated_dataset_status = DatasetStatus.parse_obj(integrated_dataset_status)
        else:
            integrated_dataset_status = DatasetStatus(
                dataset=integrated_dataset_metadata.dataset,
                version=integrated_dataset_metadata.version,
            )

            # If I've saved a status for this dataset, compare 'updated_on' timestamp to avoid redundant queries.
        if not action_config.pull_events_config.force_fetch and integrated_dataset_status.latest_updated_on >= integrated_dataset_metadata.updated_on:
            msg = f"No updates reported for dataset '{DATASET_GFW_INTEGRATED_ALERTS}' so skipping integrated_alerts queries"
            logger.info(
                msg,
                extra={
                    "integration_id": str(integration.id),
                    "integration_login": auth_config.email,
                    "dataset_updated_on": integrated_dataset_metadata.updated_on.isoformat(),
                },
            )
            await log_action_activity(
                integration_id=integration.id,
                action_id="get_dataset_and_geostores",
                level=LogLevel.INFO,
                title=msg,
                data={"dataset_updated_on": integrated_dataset_metadata.updated_on.isoformat()}
            )
            integrated_dataset_metadata = None

    # Check if any of the datasets is valid to trigger sub-actions
    if any([fire_dataset_metadata, integrated_dataset_metadata]):
        for geostore_id in geostore_ids:
            for lower, upper in generate_date_pairs(start_date, end_date):
                if fire_dataset_metadata:
                    # Trigger "get_nasa_viirs_fire_alerts" sub-action
                    config = GetNasaVIIRSFireAlertsForGeostoreID(
                        integration_id=str(integration.id),
                        geostore_id=geostore_id.decode('utf8'),
                        date_range=(lower, upper),
                        lowest_confidence=action_config.pull_events_config.fire_alerts_lowest_confidence,
                        dataset=fire_dataset_metadata
                    )

                    await trigger_action(
                        integration.id,
                        "get_nasa_viirs_fire_alerts_for_geostore_and_date_range",
                        config=config
                    )
                    fire_alerts_actions_triggered += 1

                if integrated_dataset_metadata:
                    # Trigger "get_gfw_integrated_alerts" sub-action
                    config = GetIntegratedAlertsForGeostoreID(
                        integration_id=str(integration.id),
                        geostore_id=geostore_id.decode('utf8'),
                        date_range=(lower, upper),
                        lowest_confidence=action_config.pull_events_config.integrated_alerts_lowest_confidence,
                        dataset=integrated_dataset_metadata
                    )

                    await trigger_action(
                        integration.id,
                        "get_integrated_alerts_for_geostore_and_date_range",
                        config=config
                    )
                    integrated_alerts_actions_triggered += 1

    # Save status for both datasets.
    if fire_dataset_metadata:
        fire_dataset_status = DatasetStatus(
            dataset=fire_dataset_metadata.dataset,
            version=fire_dataset_metadata.version,
            latest_updated_on=fire_dataset_metadata.updated_on
        )

        await state_manager.set_state(
            str(integration.id),
            "pull_events",
            fire_dataset_status.dict(),
            source_id=DATASET_NASA_VIIRS_FIRE_ALERTS
        )

    if integrated_dataset_metadata:
        integrated_dataset_status = DatasetStatus(
            dataset=integrated_dataset_metadata.dataset,
            version=integrated_dataset_metadata.version,
            latest_updated_on=integrated_dataset_metadata.updated_on
        )

        await state_manager.set_state(
            str(integration.id),
            "pull_events",
            integrated_dataset_status.dict(),
            source_id=DATASET_GFW_INTEGRATED_ALERTS
        )

    return {
        "fire_alerts_actions_triggered": fire_alerts_actions_triggered,
        "integrated_alerts_actions_triggered": integrated_alerts_actions_triggered
    }


async def action_get_nasa_viirs_fire_alerts_for_geostore_and_date_range(
        integration: Integration,
        action_config: GetNasaVIIRSFireAlertsForGeostoreID
):
    total_alerts = 0
    auth_config = get_auth_config(integration)
    dataapi = DataAPI(
        username=auth_config.email,
        password=auth_config.password.get_secret_value()
    )
    fire_alerts = await dataapi.get_nasa_viirs_fire_alerts(
        geostore_id=action_config.geostore_id,
        date_range=(action_config.date_range[0], action_config.date_range[1]),
        lowest_confidence=action_config.lowest_confidence,
        semaphore=sema
    )

    if fire_alerts:
        logger.info(f"Fire alerts pulled with success.")
        transformed_data = [transform_fire_alert(alert) for alert in fire_alerts]
        await handle_transformed_data(
            transformed_data,
            str(integration.id),
            "pull_events"
        )
        total_alerts += len(fire_alerts)

    dataset_status = DatasetStatus(
        dataset=action_config.dataset.dataset,
        version=action_config.dataset.version,
        latest_updated_on=action_config.dataset.updated_on
    )

    return {"dataset": DATASET_NASA_VIIRS_FIRE_ALERTS, "response": dataset_status.dict(), "total_alerts": total_alerts}


