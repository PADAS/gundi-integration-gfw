import json
import stamina
import httpx
import redis.asyncio as redis
from datetime import timedelta
from app import settings


class IntegrationStateManager:

    def __init__(self, **kwargs):
        host = kwargs.get("host", settings.REDIS_HOST)
        port = kwargs.get("port", settings.REDIS_PORT)
        db = kwargs.get("db", settings.REDIS_STATE_DB)
        self.db_client = redis.Redis(host=host, port=port, db=db)

    async def get_state(self, integration_id: str, action_id: str, source_id: str = "no-source") -> dict:
        for attempt in stamina.retry_context(on=redis.RedisError, attempts=5, wait_initial=1.0, wait_max=30, wait_jitter=3.0):
            with attempt:
                json_value = await self.db_client.get(f"integration_state.{integration_id}.{action_id}.{source_id}")
        value = json.loads(json_value) if json_value else {}
        return value

    async def set_state(self, integration_id: str, action_id: str, state: dict, source_id: str = "no-source"):
        for attempt in stamina.retry_context(on=redis.RedisError, attempts=5, wait_initial=1.0, wait_max=30, wait_jitter=3.0):
            with attempt:
                await self.db_client.set(
                    f"integration_state.{integration_id}.{action_id}.{source_id}",
                    json.dumps(state, default=str)
                )

    async def delete_state(self, integration_id: str, action_id: str, source_id: str = "no-source"):
        for attempt in stamina.retry_context(on=redis.RedisError, attempts=5, wait_initial=1.0, wait_max=30, wait_jitter=3.0):
            with attempt:
                await self.db_client.delete(
                    f"integration_state.{integration_id}.{action_id}.{source_id}"
                )

    async def set_quiet_period(self, integration_id: str, action_id:str, quiet_period: int | timedelta):
        for attempt in stamina.retry_context(on=redis.RedisError, attempts=5, wait_initial=1.0, wait_max=30, wait_jitter=3.0):
            with attempt:
                if quiet_period: # handle both int and timedelta
                    await self.db_client.setex(
                        f"integration_state.{integration_id}.{action_id}.quiet_period", quiet_period, 1)
                else:
                    await self.db_client.delete(
                        f"integration_state.{integration_id}.{action_id}.quiet_period"
                    )

    async def is_quiet_period(self, integration_id: str, action_id: str):
        for attempt in stamina.retry_context(on=redis.RedisError, attempts=5, wait_initial=1.0, wait_max=30, wait_jitter=3.0):
            with attempt:
                val = await self.db_client.exists(
                    f"integration_state.{integration_id}.{action_id}.quiet_period",
                )
                return val

    # Default TTL for cached AOI data: 7 days
    DEFAULT_AOI_DATA_TTL_SECONDS = 86400 * 7

    async def set_aoi_data(self, integration_id: str, aoi_data: dict, ttl_seconds: int = None):
        """
        Cache AOI data for an integration.
        
        This provides resilience if the GFW API is temporarily unavailable.
        """
        ttl = ttl_seconds or self.DEFAULT_AOI_DATA_TTL_SECONDS
        for attempt in stamina.retry_context(on=redis.RedisError, attempts=5, wait_initial=1.0, wait_max=30, wait_jitter=3.0):
            with attempt:
                await self.db_client.setex(
                    f"integration_state.{integration_id}.aoi_data",
                    ttl,
                    json.dumps(aoi_data, default=str)
                )

    async def get_aoi_data(self, integration_id: str) -> dict | None:
        """
        Retrieve cached AOI data for an integration.
        
        Returns None if no cached data exists.
        """
        for attempt in stamina.retry_context(on=redis.RedisError, attempts=5, wait_initial=1.0, wait_max=30, wait_jitter=3.0):
            with attempt:
                data = await self.db_client.get(f"integration_state.{integration_id}.aoi_data")
        if data:
            return json.loads(data)
        return None

    # Default TTL for pending jobs: 24 hours
    # Jobs should complete well within this time, and expired jobs will be cleaned up automatically
    DEFAULT_JOB_TTL_SECONDS = 86400  # 24 hours

    def _pending_job_key(self, integration_id: str, action_id: str, job_id: str) -> str:
        """Get the Redis key for a pending job's data."""
        return f"integration_state.{integration_id}.{action_id}.pending_job.{job_id}"

    def _pending_jobs_set_key(self, integration_id: str, action_id: str) -> str:
        """Get the Redis key for the set of pending job IDs."""
        return f"integration_state.{integration_id}.{action_id}.pending_job_ids"

    async def add_pending_job(self, integration_id: str, action_id: str, job_data: dict, ttl_seconds: int = None):
        """
        Store a pending job in the cache.
        
        Stores job data in an individual key with TTL, and tracks the job ID in a set.
        job_data should include job_id, job_link, and any other relevant metadata.
        The job data will automatically expire after ttl_seconds (default 24 hours).
        """
        job_id = job_data.get("job_id")
        if not job_id:
            raise ValueError("job_data must include a 'job_id' field")
        
        ttl = ttl_seconds or self.DEFAULT_JOB_TTL_SECONDS
        job_key = self._pending_job_key(integration_id, action_id, job_id)
        set_key = self._pending_jobs_set_key(integration_id, action_id)
        
        for attempt in stamina.retry_context(on=redis.RedisError, attempts=5, wait_initial=1.0, wait_max=30, wait_jitter=3.0):
            with attempt:
                # Use a transactional pipeline so both operations succeed or fail together
                pipe = self.db_client.pipeline(transaction=True)
                # Store job data with TTL
                pipe.setex(
                    job_key,
                    ttl,
                    json.dumps(job_data, default=str)
                )
                # Add job ID to tracking set
                pipe.sadd(set_key, job_id)
                # Execute both commands atomically
                await pipe.execute()

    async def get_pending_jobs(self, integration_id: str, action_id: str) -> list:
        """
        Retrieve all pending jobs for an integration/action.
        
        Gets job IDs from the tracking set, then looks up each job's data.
        If a job's data has expired (key doesn't exist), removes it from the set.
        Returns a list of job_data dicts.
        """
        set_key = self._pending_jobs_set_key(integration_id, action_id)
        jobs = []
        expired_job_ids = []
        
        for attempt in stamina.retry_context(on=redis.RedisError, attempts=5, wait_initial=1.0, wait_max=30, wait_jitter=3.0):
            with attempt:
                # Get all job IDs from the set
                job_ids = await self.db_client.smembers(set_key)
        
        if not job_ids:
            return []
        
        # Look up each job's data
        for job_id in job_ids:
            job_id_str = job_id.decode('utf8') if isinstance(job_id, bytes) else job_id
            job_key = self._pending_job_key(integration_id, action_id, job_id_str)
            
            for attempt in stamina.retry_context(on=redis.RedisError, attempts=5, wait_initial=1.0, wait_max=30, wait_jitter=3.0):
                with attempt:
                    job_data = await self.db_client.get(job_key)
            
            if job_data:
                jobs.append(json.loads(job_data))
            else:
                # Job data expired, mark for removal from set
                expired_job_ids.append(job_id_str)
        
        # Clean up expired job IDs from the set
        if expired_job_ids:
            for attempt in stamina.retry_context(on=redis.RedisError, attempts=5, wait_initial=1.0, wait_max=30, wait_jitter=3.0):
                with attempt:
                    await self.db_client.srem(set_key, *expired_job_ids)
        
        return jobs

    async def remove_pending_job(self, integration_id: str, action_id: str, job_id: str):
        """
        Remove a completed or failed job from the pending jobs cache.
        
        Deletes the job data key and removes the job ID from the tracking set.
        """
        job_key = self._pending_job_key(integration_id, action_id, job_id)
        set_key = self._pending_jobs_set_key(integration_id, action_id)
        
        for attempt in stamina.retry_context(on=redis.RedisError, attempts=5, wait_initial=1.0, wait_max=30, wait_jitter=3.0):
            with attempt:
                pipeline = self.db_client.pipeline(transaction=True)
                pipeline.delete(job_key)
                pipeline.srem(set_key, job_id)
                await pipeline.execute()

    def __str__(self):
        return f"IntegrationStateManager(host={self.db_client.host}, port={self.db_client.port}, db={self.db_client.db})"

    def __repr__(self):
        return self.__str__()
