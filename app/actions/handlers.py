import asyncio
import httpx
import logging
import random
import app.settings

from app.actions.gfwclient import DataAPI, DatasetStatus, \
    AOIData, DATASET_GFW_INTEGRATED_ALERTS, DATASET_NASA_VIIRS_FIRE_ALERTS, DataAPIAuthException, \
    JobResponse, IntegratedAlert, DownloadLinkExpiredException
from datetime import timezone, timedelta, datetime

from app.actions.configurations import (
    AuthenticateConfig,
    PullEventsConfig,
    get_auth_config,
    GetFireAlertsDatasetAndGeostoresConfig,
    GetIntegratedAlertsDatasetAndGeostoresConfig,
    GetNasaVIIRSFireAlertsForGeostoreID,
    GetIntegratedAlertsForGeostoreID
)
from app.services.action_scheduler import trigger_action
from app.services.activity_logger import activity_logger, log_action_activity
from app.services.action_scheduler import crontab_schedule
from app.services.gundi import send_events_to_gundi
from app.services.state import IntegrationStateManager

from gundi_core.schemas.v2 import Integration, LogLevel
from pydantic import ValidationError

GFW_INTEGRATED_ALERTS = "gfwgladalert"
GFW_FIRE_ALERT = "gfwfirealert"

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
@crontab_schedule("*/10 * * * *")
async def action_pull_events(integration: Integration, action_config: PullEventsConfig):
    result = {}

    logger.info(f"Executing 'pull_events' action with integration {integration} and action_config {action_config}")

    auth_config = get_auth_config(integration)

    # Get AOI data from API, with fallback to cached data
    dataapi = DataAPI(username=auth_config.email, password=auth_config.password.get_secret_value())
    aoi_data = None

    # Prefer cached AOI data, fall back to API if not available
    cached_data = await state_manager.get_aoi_data(str(integration.id))
    if cached_data:
        aoi_data = AOIData.parse_obj(cached_data)
        logger.info(f"Using cached AOI data for integration {integration.id}")
    else:
        try:
            aoi_id = await dataapi.aoi_from_url(action_config.gfw_share_link_url)
            aoi_data = await dataapi.get_aoi(aoi_id=aoi_id)
            # Cache the AOI data for future use
            await state_manager.set_aoi_data(str(integration.id), aoi_data.dict())
            logger.info(f"Fetched and cached AOI data for integration {integration.id}")
        except Exception as e:
            msg = f"Failed to fetch AOI data for {action_config.gfw_share_link_url} and no cached data is available: {e}"
            logger.error(msg, extra={"needs_attention": True, "integration_id": str(integration.id)})
            await log_action_activity(
                integration_id=integration.id,
                action_id=action_pull_events.__name__.replace("action_", ""),
                level=LogLevel.ERROR,
                title=msg,
                data={"error": str(e)}
            )
            result["message"] = msg
            return result

    # Some AOIs do not have an associated Geostore so we short-circuit here and report in the logs.
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
            action_id=action_pull_events.__name__.replace("action_", ""),
            level=LogLevel.ERROR,
            title=msg,
            data={"aoi_data": aoi_data.dict()}
        )
        result["message"] = msg
        return result

    # Trigger feed-specific sub-actions independently (each with its own quiet period)
    triggered_actions = []
    skipped_actions = []
    
    # Fire alerts - check its own quiet period
    if action_config.include_fire_alerts:
        if not action_config.force_fetch and await state_manager.is_quiet_period(str(integration.id), "fire_alerts"):
            logger.info("Fire alerts quiet period active, skipping")
            skipped_actions.append("fire_alerts")
        else:
            fire_config = GetFireAlertsDatasetAndGeostoresConfig(
                integration_id=str(integration.id),
                pull_events_config=action_config,
                aoi_data=aoi_data
            )
            await trigger_action(integration.id, action_get_nasa_viirs_fire_alerts.__name__.replace("action_", ""), config=fire_config)
            triggered_actions.append("fire_alerts")
    
    # Integrated alerts - check its own quiet period (but always run if pending jobs exist)
    if action_config.include_integrated_alerts:
        # Check if there are pending jobs that need polling (download links expire in 15 min)
        integrated_action_id = action_get_gfw_integrated_alerts.__name__.replace("action_", "")
        has_pending_jobs = len(await state_manager.get_pending_jobs(str(integration.id), integrated_action_id)) > 0
        
        if has_pending_jobs:
            # Always trigger if there are pending jobs - we need to poll them regardless of quiet period
            logger.info("Integrated alerts has pending jobs, triggering to poll them")
            integrated_config = GetIntegratedAlertsDatasetAndGeostoresConfig(
                integration_id=str(integration.id),
                pull_events_config=action_config,
                aoi_data=aoi_data
            )
            await trigger_action(integration.id, integrated_action_id, config=integrated_config)
            triggered_actions.append("integrated_alerts (polling jobs)")
        elif not action_config.force_fetch and await state_manager.is_quiet_period(str(integration.id), "integrated_alerts"):
            logger.info("Integrated alerts quiet period active, skipping")
            skipped_actions.append("integrated_alerts")
        else:
            integrated_config = GetIntegratedAlertsDatasetAndGeostoresConfig(
                integration_id=str(integration.id),
                pull_events_config=action_config,
                aoi_data=aoi_data
            )
            await trigger_action(integration.id, integrated_action_id, config=integrated_config)
            triggered_actions.append("integrated_alerts")

    # Build result message
    messages = []
    if triggered_actions:
        messages.append(f"Actions triggered: {', '.join(triggered_actions)}")
    if skipped_actions:
        messages.append(f"Skipped (quiet period): {', '.join(skipped_actions)}")
    if not triggered_actions and not skipped_actions:
        messages.append("No actions configured.")
    
    result["message"] = ". ".join(messages)
    return result


def generate_date_pairs(lower_date, upper_date, interval=10):
    while upper_date > lower_date:
        yield max(lower_date, upper_date - timedelta(days=interval)), upper_date
        upper_date -= timedelta(days=interval)


async def action_get_nasa_viirs_fire_alerts(integration: Integration, action_config: GetFireAlertsDatasetAndGeostoresConfig):
    auth_config = get_auth_config(integration)
    dataapi = DataAPI(
        username=auth_config.email,
        password=auth_config.password.get_secret_value()
    )

    fire_dataset_metadata = None
    fire_alerts_actions_triggered = 0

    # Use the geostore ID from AOI data
    geostore_ids = [action_config.aoi_data.attributes.geostore]

    # Date ranges are in whole days, so we round to next midnight.
    end_date = (datetime.now(tz=timezone.utc) + timedelta(days=1)).replace(hour=0, minute=0, second=0, microsecond=0)
    start_date = end_date - timedelta(days=action_config.pull_events_config.fire_lookback_days)

    fire_dataset_metadata = await dataapi.get_dataset_metadata(DATASET_NASA_VIIRS_FIRE_ALERTS)
    fire_dataset_status = await state_manager.get_state(
        str(integration.id),
        "pull_events",
        DATASET_NASA_VIIRS_FIRE_ALERTS
    )

    if fire_dataset_status:
        logger.info(f"Saved fire dataset status: {fire_dataset_status}")
        try:
            fire_dataset_status = DatasetStatus.parse_obj(fire_dataset_status)
        except ValidationError:
            logger.exception(
                f"Invalid fire dataset status: {fire_dataset_status}. Setting it from metadata..."
            )
            fire_dataset_status = DatasetStatus(
                dataset=fire_dataset_metadata.dataset,
                version=fire_dataset_metadata.version,
            )
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
            action_id=action_get_nasa_viirs_fire_alerts.__name__.replace("action_", ""),
            level=LogLevel.INFO,
            title=msg,
            data={"dataset_updated_on": fire_dataset_metadata.updated_on.isoformat()}
        )
        fire_dataset_metadata = None

    # Check if the dataset is valid to trigger sub-actions
    if fire_dataset_metadata:
        for geostore_id in geostore_ids:
            for lower, upper in generate_date_pairs(start_date, end_date, interval=7):
                # Trigger "get_nasa_viirs_fire_alerts" sub-action
                config = GetNasaVIIRSFireAlertsForGeostoreID(
                    integration_id=str(integration.id),
                    geostore_id=geostore_id,
                    date_range=(lower, upper),
                    lowest_confidence=action_config.pull_events_config.fire_alerts_lowest_confidence,
                    dataset=fire_dataset_metadata
                )

                await trigger_action(
                    integration.id,
                    action_get_nasa_viirs_fire_alerts_for_geostore_and_date_range.__name__.replace("action_", ""),
                    config=config
                )
                fire_alerts_actions_triggered += 1

        # Save status for fire dataset
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

    # Set tiered quiet period based on outcome
    if fire_alerts_actions_triggered > 0:
        # Work was done - long quiet period to rate limit expensive queries
        quiet_minutes = random.randint(120, 240)
    else:
        # No updates - short quiet period for frequent cheap metadata checks
        quiet_minutes = random.randint(30, 60)

    await state_manager.set_quiet_period(
        str(integration.id), "fire_alerts", timedelta(minutes=quiet_minutes)
    )

    return {
        "fire_alerts_actions_triggered": fire_alerts_actions_triggered,
        "quiet_period_minutes": quiet_minutes
    }


async def action_get_gfw_integrated_alerts(integration: Integration, action_config: GetIntegratedAlertsDatasetAndGeostoresConfig):
    auth_config = get_auth_config(integration)
    dataapi = DataAPI(
        username=auth_config.email,
        password=auth_config.password.get_secret_value()
    )

    result = {
        "jobs_polled": 0,
        "jobs_completed": 0,
        "jobs_pending": 0,
        "jobs_failed": 0,
        "batch_jobs_created": 0,
        "total_alerts_processed": 0
    }

    # Derive action ID from function name
    action_id = action_get_gfw_integrated_alerts.__name__.replace("action_", "")

    # Step 1: Poll any pending jobs from previous runs
    pending_jobs = await state_manager.get_pending_jobs(str(integration.id), action_id)
    
    for job_data in pending_jobs:
        result["jobs_polled"] += 1
        job_id = job_data.get("job_id")
        job_link = job_data.get("job_link")
        
        try:
            job_status = await dataapi.get_job_status(job_link)
            
            # Job is ready when status is success/partial_success AND download_link is available
            if job_status.status in ("success", "partial_success") and job_status.download_link:
                # Log warning for partial success
                if job_status.status == "partial_success":
                    msg = f"Batch job {job_id} completed with partial success"
                    logger.warning(
                        msg,
                        extra={
                            "integration_id": str(integration.id),
                            "job_id": job_id,
                            "failed_geometries_link": str(job_status.failed_geometries_link) if job_status.failed_geometries_link else None
                        }
                    )
                    await log_action_activity(
                        integration_id=integration.id,
                        action_id=action_id,
                        level=LogLevel.WARNING,
                        title=msg,
                        data={"job_id": job_id, "failed_geometries_link": str(job_status.failed_geometries_link) if job_status.failed_geometries_link else None}
                    )
                
                # Download and process results
                try:
                    alerts_data = await dataapi.download_job_results(str(job_status.download_link))
                    
                    if alerts_data:
                        # Parse and transform alerts
                        integrated_alerts = [IntegratedAlert.parse_obj(alert) for alert in alerts_data]
                        transformed_data = [transform_integrated_alert(alert) for alert in integrated_alerts]
                        
                        await handle_transformed_data(
                            transformed_data,
                            str(integration.id),
                            action_id
                        )
                        result["total_alerts_processed"] += len(integrated_alerts)
                        logger.info(f"Processed {len(integrated_alerts)} integrated alerts from job {job_id}")
                    
                    result["jobs_completed"] += 1
                    await state_manager.remove_pending_job(str(integration.id), action_id, job_id)
                    
                except DownloadLinkExpiredException as e:
                    msg = f"Batch job {job_id} download link expired"
                    logger.error(
                        msg,
                        extra={
                            "integration_id": str(integration.id),
                            "job_id": job_id,
                            "error": str(e)
                        }
                    )
                    await log_action_activity(
                        integration_id=integration.id,
                        action_id=action_id,
                        level=LogLevel.ERROR,
                        title=msg,
                        data={"job_id": job_id, "error": str(e)}
                    )
                    result["jobs_failed"] += 1
                    await state_manager.remove_pending_job(str(integration.id), action_id, job_id)
                
            elif job_status.status == "failed":
                msg = f"Batch job {job_id} failed"
                logger.error(
                    msg,
                    extra={
                        "integration_id": str(integration.id),
                        "job_id": job_id,
                        "message": job_status.message
                    }
                )
                await log_action_activity(
                    integration_id=integration.id,
                    action_id=action_id,
                    level=LogLevel.ERROR,
                    title=msg,
                    data={"job_id": job_id, "message": job_status.message}
                )
                result["jobs_failed"] += 1
                await state_manager.remove_pending_job(str(integration.id), action_id, job_id)
                
            else:
                # Job is still pending (status is "pending" OR status is success/partial_success but no download_link yet)
                result["jobs_pending"] += 1
                if job_status.status in ("success", "partial_success"):
                    logger.info(f"Job {job_id} status is {job_status.status} but download_link not ready yet")
                else:
                    logger.info(f"Job {job_id} still pending with progress: {job_status.progress}")
                
        except Exception as e:
            logger.exception(f"Error polling job {job_id}: {e}")

    # Step 2: Check if dataset has been updated and create new batch query
    # Use the geostore ID from AOI data
    geostore_ids = [action_config.aoi_data.attributes.geostore]

    # Date ranges are in whole days, so we round to next midnight.
    end_date = (datetime.now(tz=timezone.utc) + timedelta(days=1)).replace(hour=0, minute=0, second=0, microsecond=0)
    start_date = end_date - timedelta(days=action_config.pull_events_config.integrated_alerts_lookback_days)

    integrated_dataset_metadata = await dataapi.get_dataset_metadata(DATASET_GFW_INTEGRATED_ALERTS)
    integrated_dataset_status = await state_manager.get_state(
        str(integration.id),
        "pull_events",
        DATASET_GFW_INTEGRATED_ALERTS
    )

    if integrated_dataset_status:
        logger.info(f"Saved integrated dataset status: {integrated_dataset_status}")
        try:
            integrated_dataset_status = DatasetStatus.parse_obj(integrated_dataset_status)
        except ValidationError:
            logger.exception(
                f"Invalid integrated dataset status: {integrated_dataset_status}. Setting it from metadata..."
            )
            integrated_dataset_status = DatasetStatus(
                dataset=integrated_dataset_metadata.dataset,
                version=integrated_dataset_metadata.version,
            )
    else:
        integrated_dataset_status = DatasetStatus(
            dataset=integrated_dataset_metadata.dataset,
            version=integrated_dataset_metadata.version,
        )

    # Check if dataset has been updated
    dataset_updated = action_config.pull_events_config.force_fetch or integrated_dataset_status.latest_updated_on < integrated_dataset_metadata.updated_on

    if not dataset_updated:
        msg = f"No updates reported for dataset '{DATASET_GFW_INTEGRATED_ALERTS}' so skipping new batch query"
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
            action_id=action_id,
            level=LogLevel.INFO,
            title=msg,
            data={"dataset_updated_on": integrated_dataset_metadata.updated_on.isoformat()}
        )
    else:
        # Create batch query for the full lookback period
        for geostore_id in geostore_ids:
            try:
                async with sema:
                    batch_result = await dataapi.query_batch_gfw_integrated_alerts(
                        geostore_id=geostore_id,
                        date_range=(start_date, end_date),
                        lowest_confidence=action_config.pull_events_config.integrated_alerts_lowest_confidence,
                        semaphore=sema
                    )
                
                if batch_result:
                    job_response = JobResponse.parse_obj(batch_result)
                    
                    # Store job in pending jobs cache
                    job_data = {
                        "job_id": job_response.data.job_id,
                        "job_link": str(job_response.data.job_link),
                        "geostore_id": geostore_id,
                        "date_range_start": start_date.isoformat(),
                        "date_range_end": end_date.isoformat(),
                        "created_at": datetime.now(tz=timezone.utc).isoformat()
                    }
                    await state_manager.add_pending_job(str(integration.id), action_id, job_data)
                    result["batch_jobs_created"] += 1
                    
                    logger.info(
                        f"Created batch job {job_response.data.job_id} for geostore {geostore_id}",
                        extra={
                            "integration_id": str(integration.id),
                            "job_id": job_response.data.job_id,
                            "geostore_id": geostore_id
                        }
                    )
                    
            except Exception as e:
                logger.exception(f"Error creating batch query for geostore {geostore_id}: {e}")

        # Save status for integrated alerts dataset
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

    # Set tiered quiet period based on outcome
    if result["jobs_pending"] > 0:
        # Jobs still processing - very short quiet period to poll frequently
        quiet_minutes = 0
    elif result["batch_jobs_created"] > 0 or result["jobs_completed"] > 0:
        # Work was done - long quiet period to rate limit expensive operations
        quiet_minutes = random.randint(240, 720)
    else:
        # No updates, no pending jobs - short quiet period for frequent cheap metadata checks
        quiet_minutes = random.randint(30, 60)

    await state_manager.set_quiet_period(
        str(integration.id), "integrated_alerts", timedelta(minutes=quiet_minutes)
    )
    
    result["quiet_period_minutes"] = quiet_minutes
    return result


# DEPRECATED: This function is kept for backwards compatibility with any pending sub-actions.
# New integrated alerts use the batch query approach in action_get_gfw_integrated_alerts.
async def action_get_gfw_integrated_alerts_for_date_range(
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
        logger.info(f"Total fire alerts fetched: {len(fire_alerts)}")
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


