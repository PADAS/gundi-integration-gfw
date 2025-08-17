import httpx
import json
import logging
import asyncio
import pydantic
import random
import re
import backoff
from enum import Enum
from functools import lru_cache
from datetime import datetime, timedelta, timezone
from typing import Optional, List, Set, Tuple, Dict, Any

import httpx

logger = logging.getLogger(__name__)

DATASET_GFW_INTEGRATED_ALERTS = "gfw_integrated_alerts"
DATASET_NASA_VIIRS_FIRE_ALERTS = "nasa_viirs_fire_alerts"

# Cache for dataset metadata to avoid repeated API calls
_dataset_metadata_cache = {}
_metadata_cache_ttl = GFW_METADATA_CACHE_TTL  # Use config-defined cache TTL

def random_string(n=4):
    return "".join(random.sample([chr(x) for x in range(97, 97 + 26)], n))


class DatasetStatus(pydantic.BaseModel):
    latest_updated_on: datetime = pydantic.Field(
        default_factory=lambda: datetime(1970, 1, 1, tzinfo=timezone.utc)
    )
    version: Optional[str] = ""
    dataset: Optional[str] = ""

    class Config:
        json_encoders = {datetime: lambda val: val.isoformat()}


class DataAPIToken(pydantic.BaseModel):
    access_token: str
    token_type: str

    # In case GFW's Oauth2 token does not provide expiration, we assume it's good for a day
    expires_in: int = 86400
    expires_at: datetime = None

    @pydantic.root_validator
    def calculator(cls, values):
        expires_at = values.get("expires_at")
        if not expires_at:
            values["expires_at"] = datetime.now(tz=timezone.utc) + timedelta(
                seconds=values["expires_in"]
            )
        return values


class DatasetResponseItem(pydantic.BaseModel):
    created_on: datetime
    updated_on: datetime
    dataset: str
    version: str
    is_latest: bool
    is_mutable: bool

    @pydantic.validator("created_on", "updated_on")
    def clean_timestamp(val):
        return val if val.tzinfo else val.replace(tzinfo=timezone.utc)


class DataAPIKey(pydantic.BaseModel):
    created_on: datetime
    updated_on: datetime
    alias: str
    user_id: str
    api_key: str
    organization: str
    email: str
    domains: List[str]
    expires_on: datetime

    @pydantic.validator("created_on", "updated_on", "expires_on")
    def sanitize_datetimes(val):
        if not val.tzinfo:
            return val.replace(tzinfo=timezone.utc)
        return val


class DataAPIKeyResponse(pydantic.BaseModel):
    data: DataAPIKey


class DataAPIKeysResponse(pydantic.BaseModel):
    data: List[DataAPIKey]


class DataAPIAuthException(Exception):
    pass


class DataAPIQueryException(Exception):
    pass

class GFWClientException(Exception):
    pass


class AOIAttributes(pydantic.BaseModel):
    name: Optional[str]
    application: Optional[str]
    geostore: Optional[str] = None
    createdAt: datetime
    updatedAt: datetime
    datasets: Optional[List[str]] = []
    use: dict
    env: str
    tags: Optional[List[str]]
    status: str
    public: bool
    fireAlerts: Optional[bool] = True
    deforestationAlerts: Optional[bool] = True
    webhookUrl: Optional[str]
    monthlySummary: Optional[bool] = False
    subscriptionId: Optional[str]
    email: Optional[str]
    language: Optional[str]
    confirmed: Optional[bool] = True


class AOIData(pydantic.BaseModel):
    type: str
    id: str
    attributes: AOIAttributes


class GeostoreAttributes(pydantic.BaseModel):
    geojson: dict
    hash: str
    provider: dict
    areaHa: float
    bbox: List[float]
    lock: bool
    info: dict


class Geostore(pydantic.BaseModel):
    type: str = pydantic.Field("geoStore", const=True)
    id: str
    attributes: GeostoreAttributes
    area: float = 0

class GeostoreView(pydantic.BaseModel):
    view_link: str

class IntegratedAlert(pydantic.BaseModel):
    latitude: float
    longitude: float
    confidence_label: str = pydantic.Field(
        ..., alias="gfw_integrated_alerts__confidence"
    )
    confidence: float = 0.0
    recorded_at: datetime = pydantic.Field(..., alias="gfw_integrated_alerts__date")
    intensity: float = pydantic.Field(0.0, alias="gfw_integrated_alerts__intensity")

    @pydantic.validator(
        "recorded_at",
        pre=True,
    )
    def sanitized_date(cls, val) -> datetime:
        return datetime.strptime(val, "%Y-%m-%d").replace(tzinfo=timezone.utc)

    @pydantic.root_validator
    def compute_confidence(cls, values):
        values["confidence"] = (
            1.0 if values.get("confidence_label", "") in {"high", "highest"} else 0.0
        )
        return values

class NasaViirsFireAlert(pydantic.BaseModel):
    latitude: float
    longitude: float
    confidence: str = pydantic.Field(..., alias="confidence__cat")

    alert_date: datetime = pydantic.Field(..., alias="alert__date")
    frp: float = pydantic.Field(0.0, alias="frp__MW")
    bright_ti4: float = pydantic.Field(0.0, alias="bright_ti4__K")
    bright_ti5: float = pydantic.Field(0.0, alias="bright_ti5__K")

    @pydantic.validator("alert_date", pre=True)
    def sanitized_date(cls, val) -> datetime:
        return datetime.strptime(val, "%Y-%m-%d").replace(tzinfo=timezone.utc)


class Geometry(pydantic.BaseModel):
    type: str
    coordinates: List[List[List[float]]]


class CreatedGeostore(pydantic.BaseModel):
    created_on: datetime
    updated_on: datetime
    gfw_geostore_id: str
    gfw_geojson: Geometry
    gfw_area__ha: float
    gfw_bbox: List[float]

    @pydantic.validator("created_on", "updated_on")
    def clean_timestamp(val):
        return val if val.tzinfo else val.replace(tzinfo=timezone.utc)

    @pydantic.validator("gfw_geostore_id")
    def simply_geostore_id(val):
        # when we get a Geostore ID back, it's in standard form, but 
        # when using it as a query parameter, it needs to in compact form.
        return val.replace('-', '').lower() if val else val


class GeoStoreResponse(pydantic.BaseModel):
    data: CreatedGeostore
    status: str

class DatasetMetadata(pydantic.BaseModel):
    created_on: Optional[datetime] = None
    updated_on: Optional[datetime] = None
    resolution: Optional[int] = None
    geographic_coverage: Optional[str] = None
    update_frequency: Optional[str] = None
    scale: Optional[str] = None
    citation: Optional[str] = None
    title: Optional[str] = None
    source: Optional[str] = None
    license: Optional[str] = None
    data_language: Optional[str] = None
    overview: Optional[str] = None
    function: Optional[str] = None
    cautions: Optional[str] = None
    key_restrictions: Optional[str] = None
    tags: Optional[List[str]] = pydantic.Field(default_factory=list)
    why_added: Optional[Any] = None
    learn_more: Optional[Any] = None
    id: Optional[str] = None

    @pydantic.validator("created_on", "updated_on")
    def clean_timestamp(val):
        return val if val.tzinfo else val.replace(tzinfo=timezone.utc)


class Dataset(pydantic.BaseModel):
    created_on: datetime
    updated_on: datetime
    dataset: str
    is_downloadable: bool
    metadata: DatasetMetadata
    versions: Optional[List[str]] = pydantic.Field(default_factory=list)

    @pydantic.validator("created_on", "updated_on")
    def clean_timestamp(val):
        return val if val.tzinfo else val.replace(tzinfo=timezone.utc)


class DatasetsResponse(pydantic.BaseModel):
    data: List[Dataset] = []
    status: Optional[str] = None

class DatasetResponse(pydantic.BaseModel):
    data: Optional[Dataset] = None
    status: Optional[str] = None

class DatasetField(pydantic.BaseModel):
    name: str
    alias: str
    description: Any
    data_type: str
    unit: Any
    is_feature_info: bool
    is_filter: bool


class DatasetFields(pydantic.BaseModel):
    data: Optional[List[DatasetField]] = None
    status: Optional[str] = None

class IntegratedAlertsConfidenceEnum(str, Enum):
    high = 'high'
    highest = 'highest'

IntegratedAlertsConfidenceEnumOrder = [IntegratedAlertsConfidenceEnum.high, IntegratedAlertsConfidenceEnum.highest]

class NasaViirsFireAlertConfidenceEnum(str, Enum):
    nominal = 'nominal'
    low = 'low'
    high = 'high'

NasaViirsFireAlertConfidenceEnumOrder = [NasaViirsFireAlertConfidenceEnum.low, NasaViirsFireAlertConfidenceEnum.nominal, NasaViirsFireAlertConfidenceEnum.high]

def giveup_handler(details):
    d1, d2 = details['kwargs']['daterange']
    d1 = d1.strftime("%Y-%m-%d")
    d2 = d2.strftime("%Y-%m-%d")
    logger.error(f"Failed to get alerts for dataset: {details['kwargs']['dataset']}, geostore_id:{details['kwargs']['geostore_id']}, daterange: ({d1} - {d2})")

def backoff_hdlr(details):
    logger.warning("Backing off {wait:0.1f} seconds after {tries} tries "
           "calling function {target} with args {args} and kwargs "
           "{kwargs}".format(**details))
    

# Custom backoff strategy starting at 5, incrementing by 10, and capping at 45
def custom_backoff():
    delay = 5
    while delay <= 45:
        yield delay
        delay += 10

DEFAULT_REQUEST_TIMEOUT = httpx.Timeout(60.0, connect=3.1)
class DataAPI:

    DATA_API_URL = "https://data-api.globalforestwatch.org"
    RESOURCE_WATCH_URL = "https://api.resourcewatch.org"

    def __init__(self, *, username: str = None, password: str = None):

        self._username = username
        self._password = password
        self._auth_gen = None
        self._api_keys = []

    @backoff.on_exception(backoff.expo, (httpx.TimeoutException, httpx.HTTPStatusError), max_tries=3)
    async def get_access_token(self):

        async with httpx.AsyncClient(timeout=DEFAULT_REQUEST_TIMEOUT) as client:
            try:
                response = await client.post(
                    url=f"{self.DATA_API_URL}/auth/token",
                    data={"username": self._username, "password": self._password},
                    follow_redirects=True
                )
            except Exception as e:
                logger.exception(f"Failed to get an access token for username {self._username}. {e}")
            else:
                if httpx.codes.is_success(response.status_code):
                    dapitoken = DataAPIToken.parse_obj(response.json()["data"])
                    return dapitoken

            raise DataAPIAuthException(f"Failed to get an access token for username {self._username}.")

    async def auth_generator(self):
        """
        Simple generator to provide a header and keep it for a designated TTL.
        """
        expire_at = datetime(1970, 1, 1, tzinfo=timezone.utc)

        while True:
            present = datetime.now(tz=timezone.utc)
            try:
                if expire_at <= present:
                    token = await self.get_access_token()

                    ttl_seconds = token.expires_in - 5
                    expire_at = present + timedelta(seconds=ttl_seconds)
                if logger.isEnabledFor(logging.DEBUG):
                    ttl = (expire_at - present).total_seconds()
                    logger.debug(f"Using cached auth, expires in {ttl} seconds.")

            except DataAPIAuthException as e:
                logger.exception(f"Failed to authenticate with GFW Data API: {e}")
                raise e
            else:
                yield token

    async def get_auth_header(self, refresh=False) -> Dict[str, str]:
        if not self._auth_gen or refresh:
            self._auth_gen = self.auth_generator()
        try:
            token = await anext(self._auth_gen)
        except StopIteration:
            self._auth_gen = self.auth_generator()
            token = await anext(self._auth_gen)
        return {"authorization": f"{token.token_type} {token.access_token}"}

    @backoff.on_exception(backoff.expo, (httpx.TimeoutException, httpx.HTTPStatusError), max_tries=3)
    async def create_api_key(self):

        headers = await self.get_auth_header()

        payload = {
            "alias": "-".join((self._username, random_string())),
            "email": self._username,
            "organization": "EarthRanger",
            "domains": [],
        }

        async with httpx.AsyncClient(timeout=DEFAULT_REQUEST_TIMEOUT) as client:
            response = await client.post(
                url=f"{self.DATA_API_URL}/auth/apikey",
                headers=headers,
                json=payload,
                follow_redirects=True
            )

            if httpx.codes.is_success(response.status_code):
                return DataAPIKeyResponse.parse_obj(response.json())

    @backoff.on_exception(backoff.expo, (httpx.TimeoutException, httpx.HTTPStatusError), max_tries=3)
    async def get_api_keys(self) -> List[DataAPIKey]:

        # If we already have API keys, filter to only those that are still valid.
        if self._api_keys:
            good_api_keys = [
                                api_key for api_key in self._api_keys if api_key.expires_on > datetime.now(tz=timezone.utc)
                            ]
            if good_api_keys:
                return good_api_keys
            
        # Otherwise, go back, get them from the API and cache them.
        headers = await self.get_auth_header()

        for _ in range(0, 2):
            async with httpx.AsyncClient(timeout=DEFAULT_REQUEST_TIMEOUT) as client:
                response = await client.get(
                    f"{self.DATA_API_URL}/auth/apikeys", headers=headers,
                    follow_redirects=True
                )

                if httpx.codes.is_success(response.status_code):
                    data = DataAPIKeysResponse.parse_obj(response.json())

                    if good_api_keys := list([
                        api_key for api_key in data.data if api_key.expires_on > datetime.now(tz=timezone.utc)
                    ]):
                        self._api_keys = good_api_keys
                        break

                # Assume we need to create an API key.
                data = await self.create_api_key()

        return self._api_keys

    @backoff.on_exception(backoff.constant, httpx.HTTPError, max_tries=3, interval=10)
    async def get_aoi(self, aoi_id: str):
        async with httpx.AsyncClient(timeout=DEFAULT_REQUEST_TIMEOUT) as client:
            response = await client.get(
                url=f"{self.RESOURCE_WATCH_URL}/v2/area/{aoi_id}",
                headers=await self.get_auth_header()
,
                follow_redirects=True
            )
            response.raise_for_status()
            response = response.json()

            try:
                return AOIData.parse_obj(response.get("data"))
            except pydantic.ValidationError as e:
                logger.exception(f"Unexpected error parsing AOI data: {e}")

            logger.error(
                "Failed to get AOI for id: %s. result is: %s", aoi_id, response.text[:250]
            )

            raise GFWClientException(f"Failed to get AOI for id: '{aoi_id}'")


    @backoff.on_exception(backoff.constant, httpx.HTTPError, max_tries=3, interval=10)
    async def get_geostore(self, geostore_id=None):
        async with httpx.AsyncClient(timeout=DEFAULT_REQUEST_TIMEOUT) as client:
            response = await client.get(
                url=f"{self.RESOURCE_WATCH_URL}/v2/geostore/{geostore_id}",
                follow_redirects=True,
                headers=await self.get_auth_header()
            )
            response.raise_for_status()
            response = response.json()

            try:
                return Geostore.parse_obj(response.get("data"))
            except pydantic.ValidationError as e:
                logger.exception(f"Unexpected error parsing Geostore data: {e}")

            logger.error(
                "Failed to get Geostore for id: %s. result is: %s", geostore_id, response.text[:250]
            )

            raise GFWClientException(f"Failed to get Geostore for id: '{geostore_id}'")
        

    @backoff.on_exception(backoff.expo, (httpx.TimeoutException, httpx.HTTPStatusError), max_tries=3)
    async def create_geostore(self, geometry) -> CreatedGeostore:
        
        headers = await self.get_auth_header()

        async with httpx.AsyncClient(timeout=DEFAULT_REQUEST_TIMEOUT) as client:
            response = await client.post(
                # url=f"{integration.base_url}/geostore",
                url=f"{self.DATA_API_URL}/geostore/",
                headers=headers,
                json={'geometry': geometry},
                follow_redirects=True
            )
            response.raise_for_status()
            val = response.json()

            val = GeoStoreResponse.parse_obj(val)
            if val.status == 'success':
                return val.data

    @backoff.on_exception(custom_backoff, (httpx.TimeoutException, httpx.HTTPStatusError),
                          max_tries=3, 
                          on_giveup=giveup_handler, raise_on_giveup=False,
                          on_backoff=backoff_hdlr)
    async def get_alerts(
        self,
        *,
        dataset: str,
        fields: Set[str],
        date_field: str,
        daterange: Tuple[datetime, datetime],
        extra_where: str = "",
        geostore_id: str
    ):

        api_keys = await self.get_api_keys()
        headers = {"x-api-key": api_keys[0].api_key}

        fields = {"latitude", "longitude"} | fields or set()

        lower_bound, upper_bound = daterange

        lower_date = lower_bound.strftime("%Y-%m-%d")
        upper_date = upper_bound.strftime("%Y-%m-%d")

        sql_query = f"SELECT {','.join(fields)} FROM results WHERE ({date_field} >= '{lower_date}' AND {date_field} <= '{upper_date}')"
        if extra_where:
            sql_query += f" AND {extra_where}"

        logger.debug(f"Querying dataset with sql: {sql_query}")
        payload = {
            'geostore_id': geostore_id,
            'sql': sql_query
        }

        async def fn():
            async with httpx.AsyncClient(timeout=DEFAULT_REQUEST_TIMEOUT) as client:

                response = await client.get(f"{self.DATA_API_URL}/dataset/{dataset}/latest/query/json",
                    headers=headers,
                    params=payload,
                    follow_redirects=True
                )

                if httpx.codes.is_success(response.status_code):
                    data = response.json()
                    data_len = len(data.get("data"))
                    logger.info(f"Extracted {data_len} alerts from dataset {dataset}, geostore_id: {geostore_id} for period {lower_date} - {upper_date}.")
                    return data.get("data", [])
                else:
                    logger.error(
                        f"Failed getting data for dataset {dataset}. status: {response.status_code}, text: {response.text}",
                        extra=payload,
                    )
                    response.raise_for_status() 

        return await fn()

    @backoff.on_exception(backoff.expo, (httpx.TimeoutException, httpx.HTTPStatusError), max_tries=3, on_backoff=backoff_hdlr)
    async def get_gfw_integrated_alerts(self, *, geostore_id: str,date_range: Tuple[datetime, datetime],
                                        lowest_confidence: IntegratedAlertsConfidenceEnum = IntegratedAlertsConfidenceEnum.highest,
                                          semaphore: asyncio.Semaphore = None):

        try:
            index = IntegratedAlertsConfidenceEnumOrder.index(lowest_confidence)
            confidence_values = IntegratedAlertsConfidenceEnumOrder[index:] 
            confidence_values = ' OR '.join(f'gfw_integrated_alerts__confidence = \'{confidence_value.value}\'' for confidence_value in confidence_values)
            extra_where = f"({confidence_values})"
        except ValueError:
            extra_where = ''
            logger.warning(f"Invalid confidence value: {lowest_confidence}. Using all confidence values.")

        async with semaphore:
            fields = {"gfw_integrated_alerts__date", "gfw_integrated_alerts__confidence"}
            alerts = await self.get_alerts(
                dataset="gfw_integrated_alerts",
                date_field="gfw_integrated_alerts__date",
                daterange=date_range,
                fields=fields,
                extra_where=extra_where,
                geostore_id=geostore_id
            )
        
        return [IntegratedAlert.parse_obj(alert) for alert in alerts] if alerts else []

    @backoff.on_exception(backoff.expo, (httpx.TimeoutException, httpx.HTTPStatusError), max_tries=3, on_backoff=backoff_hdlr)
    async def get_gfw_integrated_alerts_batch(
        self, 
        *, 
        geostore_ids: List[str], 
        date_range: Tuple[datetime, datetime],
        lowest_confidence: IntegratedAlertsConfidenceEnum = IntegratedAlertsConfidenceEnum.highest,
        semaphore: asyncio.Semaphore = None,
        max_concurrent: int = 5
    ) -> Dict[str, List[IntegratedAlert]]:
        """
        Get GFW Integrated Alerts for multiple geostores concurrently.
        
        Args:
            geostore_ids: List of geostore IDs to query
            date_range: Date range for the query
            lowest_confidence: Minimum confidence level
            semaphore: Semaphore for rate limiting
            max_concurrent: Maximum concurrent requests
            
        Returns:
            Dictionary mapping geostore_id to list of alerts
        """
        if not geostore_ids:
            return {}
            
        # Create a semaphore for this batch if none provided
        batch_semaphore = semaphore or asyncio.Semaphore(max_concurrent)
        
        async def fetch_single_geostore(geostore_id: str) -> Tuple[str, List[IntegratedAlert]]:
            try:
                alerts = await self.get_gfw_integrated_alerts(
                    geostore_id=geostore_id,
                    date_range=date_range,
                    lowest_confidence=lowest_confidence,
                    semaphore=batch_semaphore
                )
                return geostore_id, alerts
            except Exception as e:
                logger.error(f"Failed to fetch alerts for geostore {geostore_id}: {e}")
                return geostore_id, []
        
        # Process all geostores concurrently
        tasks = [fetch_single_geostore(geostore_id) for geostore_id in geostore_ids]
        results = await asyncio.gather(*tasks, return_exceptions=True)
        
        # Convert results to dictionary
        alerts_by_geostore = {}
        for result in results:
            if isinstance(result, Exception):
                logger.error(f"Task failed with exception: {result}")
                continue
            geostore_id, alerts = result
            alerts_by_geostore[geostore_id] = alerts
            
        total_alerts = sum(len(alerts) for alerts in alerts_by_geostore.values())
        logger.info(f"Batch query completed: {len(geostore_ids)} geostores, {total_alerts} total alerts")
        
        return alerts_by_geostore
        
    async def get_gfw_integrated_alerts_optimized(
        self,
        *,
        geostore_ids: List[str],
        date_range: Tuple[datetime, datetime],
        lowest_confidence: IntegratedAlertsConfidenceEnum = IntegratedAlertsConfidenceEnum.highest,
        semaphore: asyncio.Semaphore = None,
        max_concurrent: int = 5,
        enable_smart_dates: bool = True,
        max_days_per_query: int = 7
    ) -> Dict[str, List[IntegratedAlert]]:
        """
        Optimized method to retrieve GFW Integrated Alerts with all optimizations applied.
        
        This method combines:
        - Smart date range optimization
        - Batch processing
        - Metadata caching
        - Concurrent processing
        
        Args:
            geostore_ids: List of geostore IDs to query
            date_range: Date range for the query
            lowest_confidence: Minimum confidence level
            semaphore: Semaphore for rate limiting
            max_concurrent: Maximum concurrent requests
            enable_smart_dates: Whether to use smart date range optimization
            max_days_per_query: Maximum days per query
            
        Returns:
            Dictionary mapping geostore_id to list of alerts
        """
        if not geostore_ids:
            return {}
            
        # Step 1: Optimize date ranges if enabled
        if enable_smart_dates:
            try:
                date_ranges = await self.optimize_date_range_for_dataset(
                    dataset="gfw_integrated_alerts",
                    requested_start=date_range[0],
                    requested_end=date_range[1],
                    max_days_per_query=max_days_per_query
                )
            except Exception as e:
                logger.warning(f"Smart date optimization failed, using original range: {e}")
                date_ranges = [date_range]
        else:
            date_ranges = [date_range]
            
        # Step 2: Process each date range with batch processing
        all_alerts = {}
        batch_semaphore = semaphore or asyncio.Semaphore(max_concurrent)
        
        for date_range_chunk in date_ranges:
            logger.info(f"Processing date range: {date_range_chunk[0]} to {date_range_chunk[1]}")
            
            chunk_alerts = await self.get_gfw_integrated_alerts_batch(
                geostore_ids=geostore_ids,
                date_range=date_range_chunk,
                lowest_confidence=lowest_confidence,
                semaphore=batch_semaphore,
                max_concurrent=max_concurrent
            )
            
            # Merge results
            for geostore_id, alerts in chunk_alerts.items():
                if geostore_id not in all_alerts:
                    all_alerts[geostore_id] = []
                all_alerts[geostore_id].extend(alerts)
                
        # Step 3: Log summary
        total_alerts = sum(len(alerts) for alerts in all_alerts.values())
        logger.info(f"Optimized query completed: {len(geostore_ids)} geostores, {len(date_ranges)} date ranges, {total_alerts} total alerts")
        
        return all_alerts
        
    @backoff.on_exception(backoff.expo, (httpx.TimeoutException, httpx.HTTPStatusError), max_tries=3, on_backoff=backoff_hdlr)
    async def get_nasa_viirs_fire_alerts(self, *, geostore_id: str,date_range: Tuple[datetime, datetime],
                                         lowest_confidence: NasaViirsFireAlertConfidenceEnum = NasaViirsFireAlertConfidenceEnum.high,
                                         semaphore: asyncio.Semaphore = None):

        try:
            index = NasaViirsFireAlertConfidenceEnumOrder.index(lowest_confidence)
            confidence_values = NasaViirsFireAlertConfidenceEnumOrder[index:] 
            confidence_values = [str(confidence_value.value).lower()[:1] for confidence_value in confidence_values]
            confidence_values = ' OR '.join(f'confidence__cat = \'{value}\'' for value in confidence_values)
            extra_where = f"({confidence_values})"
        except ValueError:
            extra_where = ''
            logger.warning(f"Invalid confidence value: {lowest_confidence}. Using all confidence values.")

        async with semaphore:
            # fields = {"confidence__cat", "alert__date", "frp__MW", "bright_ti4__K", "bright_ti5__K"}
            fields = {"confidence__cat", "alert__date"} 
            alerts = await self.get_alerts(
                dataset="nasa_viirs_fire_alerts",
                date_field="alert__date",
                daterange=date_range,
                fields=fields,
                extra_where=extra_where,
                geostore_id=geostore_id
            )
        
        return [NasaViirsFireAlert.parse_obj(alert) for alert in alerts] if alerts else []
        

    @backoff.on_exception(backoff.expo, (httpx.TimeoutException, httpx.HTTPStatusError), max_tries=3)
    async def get_dataset_metadata(self, dataset: str = "", version: str = "latest"):
        """Get dataset metadata with caching to reduce API calls."""
        cache_key = f"{dataset}_{version}"
        current_time = datetime.now(tz=timezone.utc)
        
        # Check cache first
        if cache_key in _dataset_metadata_cache:
            cached_data, cache_time = _dataset_metadata_cache[cache_key]
            if (current_time - cache_time).total_seconds() < _metadata_cache_ttl:
                logger.debug(f"Using cached metadata for dataset {dataset}")
                return cached_data
        
        # Fetch from API if not cached or expired
        api_keys = await self.get_api_keys()
        headers = {"x-api-key": api_keys[0].api_key}

        async with httpx.AsyncClient(timeout=DEFAULT_REQUEST_TIMEOUT) as client:
            response = await client.get(
                f"{self.DATA_API_URL}/dataset/{dataset}/{version}",
                headers=headers,
                follow_redirects=True
            )

            if httpx.codes.is_success(response.status_code):
                data = response.json()
                metadata = DatasetResponseItem.parse_obj(data.get("data"))
                
                # Cache the result
                _dataset_metadata_cache[cache_key] = (metadata, current_time)
                logger.debug(f"Cached metadata for dataset {dataset}")
                
                return metadata

    async def optimize_date_range_for_dataset(
        self, 
        dataset: str, 
        requested_start: datetime, 
        requested_end: datetime,
        max_days_per_query: int = 7
    ) -> List[Tuple[datetime, datetime]]:
        """
        Optimize date ranges for dataset queries based on update frequency and data availability.
        
        Args:
            dataset: Dataset name
            requested_start: Requested start date
            requested_end: Requested end date
            max_days_per_query: Maximum days per query
            
        Returns:
            List of optimized date ranges
        """
        try:
            metadata = await self.get_dataset_metadata(dataset)
            
            # Calculate optimal chunk size based on update frequency
            update_frequency = getattr(metadata, 'update_frequency', 'Daily')
            chunk_size = self._calculate_chunk_size(update_frequency, max_days_per_query)
            
            # Generate optimized date ranges
            date_ranges = []
            current_start = requested_start
            
            while current_start < requested_end:
                current_end = min(current_start + timedelta(days=chunk_size), requested_end)
                date_ranges.append((current_start, current_end))
                current_start = current_end
                
            logger.info(f"Optimized date ranges for {dataset}: {len(date_ranges)} chunks of {chunk_size} days each")
            return date_ranges
            
        except Exception as e:
            logger.warning(f"Failed to optimize date range for {dataset}, using fallback: {e}")
            # Fallback to simple chunking
            return self._simple_date_chunking(requested_start, requested_end, max_days_per_query)
    
    def _calculate_chunk_size(self, update_frequency: str, max_days: int) -> int:
        """Calculate optimal chunk size based on update frequency."""
        frequency_lower = update_frequency.lower()
        
        if 'daily' in frequency_lower:
            return min(7, max_days)  # Daily updates: 7-day chunks
        elif 'weekly' in frequency_lower:
            return min(14, max_days)  # Weekly updates: 14-day chunks
        elif 'monthly' in frequency_lower:
            return min(30, max_days)  # Monthly updates: 30-day chunks
        else:
            return min(7, max_days)  # Default: 7-day chunks
    
    def _simple_date_chunking(self, start: datetime, end: datetime, max_days: int) -> List[Tuple[datetime, datetime]]:
        """Simple date chunking fallback."""
        date_ranges = []
        current_start = start
        
        while current_start < end:
            current_end = min(current_start + timedelta(days=max_days), end)
            date_ranges.append((current_start, current_end))
            current_start = current_end
            
        return date_ranges


    @backoff.on_exception(backoff.constant, httpx.HTTPError, max_tries=3, interval=10)
    async def aoi_from_url(self, url) -> str:
        """
        Extracts the AOI ID from a GFW share link URL.
        """

        URL_PATTERN = ".*globalforestwatch.org.*aoi/([^/]+).*"
        if matches := re.match(URL_PATTERN, url):
            return matches[1]
        
        async with httpx.AsyncClient(timeout=DEFAULT_REQUEST_TIMEOUT) as client:
            head = await client.head(url, follow_redirects=True)

        try:
            matches = re.match(URL_PATTERN, str(head.url))
            return matches[1]
        except IndexError:
            logger.error("Unable to parse AOI from globalforestwatch URL: %s", url)


    @backoff.on_exception(backoff.constant, httpx.HTTPError, max_tries=3, interval=10)
    async def get_datasets(self):
        async with httpx.AsyncClient(timeout=DEFAULT_REQUEST_TIMEOUT) as client:
            response = await client.get(
                f"{self.DATA_API_URL}/datasets",
                follow_redirects=True
            )
            response.raise_for_status()
            content = response.json()
            datasets_response = DatasetsResponse.parse_obj(content)
            return datasets_response.data
        
    @backoff.on_exception(backoff.constant, httpx.HTTPError, max_tries=3, interval=10)
    async def get_dataset_fields(self, *, dataset:str, version:str="latest"):
        async with httpx.AsyncClient(timeout=DEFAULT_REQUEST_TIMEOUT) as client:
            response = await client.get(
                f"{self.DATA_API_URL}/dataset/{dataset}/{version}/fields",
                follow_redirects=True
            )
            response.raise_for_status()
            content = response.json()
            fields_response = DatasetFields.parse_obj(content)

            return fields_response.data