import click
from click.types import DateTime
from datetime import datetime, timedelta, timezone
import asyncio
from app.actions.gfwclient import DataAPI, Geostore, GeostoreView,CreatedGeostore, \
    IntegratedAlertsConfidenceEnum, NasaViirsFireAlertConfidenceEnum, \
        NasaViirsFireAlertConfidenceEnumOrder, IntegratedAlertsConfidenceEnumOrder
from app.actions import utils
from shapely.geometry import GeometryCollection, shape, mapping
from app.services.state import IntegrationStateManager
import json
import logging

logging.getLogger("httpx").setLevel(logging.WARNING)

@click.group(help="A group of commands for getting data from Global Forest Watch API.")
def cli():
    pass

common_options = [
    click.option('--username', help='username/email used to log into Global Forest Watch dashboard', required=True),
    click.option('--password', help='password', required=True),
]

def add_options(options):
    def _add_options(func):
        for option in reversed(options):
            func = option(func)
        return func
    return _add_options

@cli.command(help="Fetch AOI Information for the provided URL")
@click.argument('url', type=str)
@add_options(common_options)
def aoi_info(url, username, password):
    client = DataAPI(username=username, password=password)

    async def fn():
        aoi_id = await client.aoi_from_url(url)
        print(f'AOI ID: {aoi_id}')
        aoi_data = await client.get_aoi(aoi_id=aoi_id)
        print(aoi_data.json(indent=2))
    asyncio.run(fn())


@cli.command(help="Fetch Geostore Information for the provided URL")
@click.argument('url', type=str)
@add_options(common_options)
def geostore_info(url, username, password):
    client = DataAPI(username=username, password=password)

    async def fn():
        aoi_id = await client.aoi_from_url(url)
        print(f'AOI ID: {aoi_id}')
        aoi_data = await client.get_aoi(aoi_id=aoi_id)
        print(f'GEOSTORE ID: {aoi_data.attributes.geostore}')
        geostore_data = await client.get_geostore(geostore_id=aoi_data.attributes.geostore)
        print(geostore_data.json(indent=2))
    asyncio.run(fn())


@cli.command(help="Get NASA Viirs Fire Alerts for the provided URL")
@click.argument('url', type=str)
@click.option('--days', type=int, required=False, default=2, help="Number of days to look back for fire alerts")
@click.option('--end_date', type=DateTime(), help='End date in YYYY-MM-DD format')
@click.option('--lowest_confidence', '-lc', type=click.Choice(NasaViirsFireAlertConfidenceEnumOrder), default=NasaViirsFireAlertConfidenceEnum.high)
@add_options(common_options)
def nasa_viirs_fire_alerts(url, username, password, days, lowest_confidence, end_date=None):
    client = DataAPI(username=username, password=password)

    end_date = end_date or datetime.now(tz=timezone.utc)
    chosen_date_range = (end_date - timedelta(days=days), end_date)

    async def fn():
        aoi_id = await client.aoi_from_url(url)
        print(f'AOI ID: {aoi_id}')
        aoi_data = await client.get_aoi(aoi_id=aoi_id)
        print(f'GEOSTORE ID: {aoi_data.attributes.geostore}')
        geostore_data = await client.get_geostore(geostore_id=aoi_data.attributes.geostore)

        geometry_collection = GeometryCollection(
            [
                shape(feature["geometry"]).buffer(0)
                for feature in geostore_data.attributes.geojson["features"]
            ]
        )
        for partition in utils.generate_geometry_fragments(geometry_collection=geometry_collection):

            geostore:CreatedGeostore = await client.create_geostore(geometry=mapping(partition))

            fire_alerts = await client.get_nasa_viirs_fire_alerts(
                geostore_id=geostore.gfw_geostore_id,
                date_range=chosen_date_range,
                lowest_confidence=lowest_confidence,
                semaphore=asyncio.Semaphore(5)    
            )
            print(fire_alerts)
    asyncio.run(fn())

@cli.command(help="Get GFW Integrated Alerts for the provided URL")
@click.argument('url', type=str)
@click.option('--days', type=int, required=False, default=2, help="Number of days to look back for fire alerts")
@click.option('--end_date', type=DateTime(), help='End date in YYYY-MM-DD format')
@click.option('--lowest_confidence', '-lc', type=click.Choice(IntegratedAlertsConfidenceEnum), default=IntegratedAlertsConfidenceEnum.high)
@add_options(common_options)
def gfw_integrated_alerts(url, username, password, days, lowest_confidence, end_date=None):
    client = DataAPI(username=username, password=password)

    end_date = end_date or datetime.now(tz=timezone.utc)
    chosen_date_range = (end_date - timedelta(days=days), end_date)
    async def fn():
        aoi_id = await client.aoi_from_url(url)
        print(f'AOI ID: {aoi_id}')
        aoi_data = await client.get_aoi(aoi_id=aoi_id)
        print(f'GEOSTORE ID: {aoi_data.attributes.geostore}')
        geostore_data = await client.get_geostore(geostore_id=aoi_data.attributes.geostore)


        geometry_collection = GeometryCollection(
            [
                shape(feature["geometry"]).buffer(0)
                for feature in geostore_data.attributes.geojson["features"]
            ]
        )
        for partition in utils.generate_geometry_fragments(geometry_collection=geometry_collection):

            geostore:CreatedGeostore = await client.create_geostore(geometry=mapping(partition))

            integrated_alerts = await client.get_gfw_integrated_alerts(
                geostore_id=geostore.gfw_geostore_id,
                date_range=chosen_date_range,
                lowest_confidence=lowest_confidence,
                semaphore=asyncio.Semaphore(5)    
            )
            for item in integrated_alerts:
                print(item.json(indent=2))
            
    asyncio.run(fn())


@cli.command(help="Get GFW Integrated Alerts using optimized methods")
@click.argument('url', type=str)
@click.option('--days', type=int, required=False, default=7, help="Number of days to look back")
@click.option('--end_date', type=DateTime(), help='End date in YYYY-MM-DD format')
@click.option('--lowest_confidence', '-lc', type=click.Choice(IntegratedAlertsConfidenceEnum), default=IntegratedAlertsConfidenceEnum.highest)
@click.option('--max_partitions', type=int, default=10, help="Maximum number of geometry partitions")
@click.option('--max_concurrent', type=int, default=5, help="Maximum concurrent requests")
@click.option('--enable_smart_dates', is_flag=True, default=True, help="Enable smart date range optimization")
@add_options(common_options)
def gfw_integrated_alerts_optimized(url, username, password, days, lowest_confidence, end_date, max_partitions, max_concurrent, enable_smart_dates):
    """Test the optimized GFW Integrated Alerts retrieval."""
    client = DataAPI(username=username, password=password)

    end_date = end_date or datetime.now(tz=timezone.utc)
    chosen_date_range = (end_date - timedelta(days=days), end_date)
    
    async def fn():
        async with utils.performance_monitor("Optimized GFW Alerts Retrieval") as monitor:
            # Get AOI and create optimized partitions
            aoi_id = await client.aoi_from_url(url)
            print(f'AOI ID: {aoi_id}')
            
            aoi_data = await client.get_aoi(aoi_id=aoi_id)
            print(f'GEOSTORE ID: {aoi_data.attributes.geostore}')
            
            geostore_data = await client.get_geostore(geostore_id=aoi_data.attributes.geostore)

            geometry_collection = GeometryCollection(
                [
                    shape(feature["geometry"]).buffer(0)
                    for feature in geostore_data.attributes.geojson["features"]
                ]
            )
            
            # Create optimized partitions
            geostore_ids = []
            partition_count = 0
            
            for partition in utils.optimize_geometry_partitioning(
                geometry_collection=geometry_collection, 
                max_partitions=max_partitions
            ):
                try:
                    geostore: CreatedGeostore = await client.create_geostore(geometry=mapping(partition))
                    geostore_ids.append(geostore.gfw_geostore_id)
                    partition_count += 1
                except Exception as e:
                    print(f"Failed to create geostore for partition: {e}")
            
            print(f"Created {partition_count} optimized partitions")
            
            # Use optimized batch retrieval
            all_alerts = await client.get_gfw_integrated_alerts_optimized(
                geostore_ids=geostore_ids,
                date_range=chosen_date_range,
                lowest_confidence=lowest_confidence,
                max_concurrent=max_concurrent,
                enable_smart_dates=enable_smart_dates
            )
            
            # Process results
            total_alerts = 0
            for geostore_id, alerts in all_alerts.items():
                print(f"Geostore {geostore_id}: {len(alerts)} alerts")
                total_alerts += len(alerts)
                for alert in alerts:
                    print(alert.json(indent=2))
            
            monitor.end(
                total_alerts=total_alerts,
                partitions=partition_count,
                geostores=len(geostore_ids),
                date_range_days=days
            )
            
    asyncio.run(fn())

@cli.command(help="List Datasets")
def datasets():
    client = DataAPI()

    async def fn():
        datasets = await client.get_datasets()
        print(datasets.json(indent=2))
    asyncio.run(fn())

@cli.command(help="Dataset Metadata")
@click.argument('dataset', type=str)
@add_options(common_options)
def dataset_metadata(dataset, username, password):
    client = DataAPI(username=username, password=password)

    async def fn():
        metadata = await client.get_dataset_metadata(dataset=dataset)
        print(metadata.json(indent=2))
    asyncio.run(fn())
    
@cli.command(help="Get Datasets")
@click.option('--search', type=str, required=False)
def get_datasets(search=None):
    client = DataAPI()

    async def fn():
        datasets = await client.get_datasets()
        return datasets
    val = asyncio.run(fn())

    if search:
        search = search.lower()
        val = [d for d in val if search in d.dataset.lower()]

    for d in val:
        print(d.dataset)

@cli.command(help="Get Dataset Fields")
@click.argument('dataset', type=str, required=True)
@click.option('--version', type=str, required=False, default="latest")
def get_dataset_fields(dataset, version):
    client = DataAPI()

    async def fn():
        f = await client.get_dataset_fields(dataset=dataset, version=version)
        return f
    val = asyncio.run(fn())
    for v in val:
        print(v.json())

@cli.command(help="Get API Keys")
@add_options(common_options)
def get_api_keys(username, password):
    client = DataAPI(username=username, password=password)
    async def fn():
        api_keys = await client.get_api_keys()
        print(api_keys.json(indent=2))

    asyncio.run(fn())

@cli.command(help="Batch Query GFW Integrated Alerts")
@add_options(common_options)
@click.option('--geostore_id', type=str, required=True)
@click.option('--dataset', type=click.Choice([
    "gfw_integrated_alerts",
    ]), required=True)
def batch_query(username, password, geostore_id, dataset):
    client = DataAPI(username=username, password=password)
    async def fn():
        date_range = (datetime.now(tz=timezone.utc) - timedelta(days=7), datetime.now(tz=timezone.utc))
        if dataset == "gfw_integrated_alerts":
            result = await client.query_batch_gfw_integrated_alerts(geostore_id=geostore_id,
             date_range=date_range, lowest_confidence=IntegratedAlertsConfidenceEnum.nominal,
              semaphore=asyncio.Semaphore(5))
        print(json.dumps(result, indent=2))
    asyncio.run(fn())

@cli.command(help="Query NASA VIIRS Fire Alerts")
@add_options(common_options)
@click.option('--geostore_id', type=str, required=True)
def nasa_viirs_fire_alerts(username, password, geostore_id):
    client = DataAPI(username=username, password=password)
    async def fn():
        date_range = (datetime.now(tz=timezone.utc) - timedelta(days=7), datetime.now(tz=timezone.utc))
        result = await client.get_nasa_viirs_fire_alerts(geostore_id=geostore_id,
            date_range=date_range, lowest_confidence=IntegratedAlertsConfidenceEnum.nominal,
            semaphore=asyncio.Semaphore(5))
        print(result)
    asyncio.run(fn())


@cli.command(help="Get Geostore IDs")
@add_options(common_options)
@click.option('--url', type=str, required=True)
def get_aoi_data(username, password, url):
    client = DataAPI(username=username, password=password)

    async def fn():
        aoi_id = await client.aoi_from_url(url)
        aoi_data = await client.get_aoi(aoi_id=aoi_id)
        print(aoi_data.json(indent=2))
    asyncio.run(fn())

@cli.command(help="Sample what GFW has for a Gundi integration vs. what its settings send on to EarthRanger")
@click.argument('integration_id', type=str)
@click.option('--days', type=int, default=None, help="Lookback days for both feeds (default: each feed's configured lookback)")
@click.option('--sample-size', type=int, default=5, help="Example records to show per confidence level")
@click.option('--use-state/--no-state', default=False,
              help="Also read Redis state (AOI cache, quiet periods, pending jobs). Needs REDIS_HOST. Default: off")
@click.option('--as-json', is_flag=True, default=False, help="Print the raw result as JSON instead of a report")
def sample_integration(integration_id, days, sample_size, use_state, as_json):
    """
    Needs Gundi API env vars (GUNDI_API_BASE_URL, KEYCLOAK_*) to look up the integration.
    GFW credentials come from the integration's own auth config.
    """
    from gundi_client_v2 import GundiClient
    from app.actions.configurations import SampleDataConfig
    from app.actions.handlers import sample_integration_data
    from app.actions.utils import format_sample_report

    async def fn():
        try:
            async with GundiClient() as gundi:
                integration = await gundi.get_integration_details(integration_id)
        except Exception as e:
            raise click.ClickException(
                f"Could not load integration {integration_id} from Gundi "
                f"(check GUNDI_API_BASE_URL / KEYCLOAK_* env vars): {type(e).__name__}: {e}"
            )
        print(f"Integration: {integration.name} ({integration.id})")
        result = await sample_integration_data(
            integration,
            SampleDataConfig(lookback_days=days, sample_size=sample_size),
            use_state=use_state,
        )
        if as_json:
            print(json.dumps(result, indent=2, default=str))
        else:
            print(format_sample_report(result))
        if "error" in result:
            raise SystemExit(1)

    asyncio.run(fn())


if __name__ == '__main__':
    cli()

