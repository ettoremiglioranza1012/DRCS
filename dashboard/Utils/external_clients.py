
# Utilities
from botocore.exceptions import ClientError, EndpointConnectionError
from typing import Optional, Dict, Any
import streamlit as st
import boto3
import redis
import time
import json
import os


class ExternalClientsConfig:
    """
    Centralized configuration for external services.

    Loads configuration values from environment variables with sensible defaults.
    These include:
    - MinIO endpoint and credentials
    - Redis host and port

    This class acts as a configuration holder passed to client managers.
    """
    MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT", "minio:9000")
    MINIO_ACCESS_KEY = os.getenv("AWS_ACCESS_KEY_ID", "minioadmin")
    MINIO_SECRET_KEY = os.getenv("AWS_SECRET_ACCESS_KEY", "minioadmin")
    MINIO_IMG_BUCKET = os.getenv("MINIO_IMG_BUCKET", "satellite-imgs")

    REDIS_HOST = os.getenv("REDIS_HOST", "redis")
    REDIS_PORT = int(os.getenv("REDIS_PORT", "6379"))


class MinIOClientManager:
    """
    Manager for connecting to and validating the availability of a MinIO client.

    It wraps a boto3 S3-compatible client and provides readiness checks.
    Automatically retries connection attempts until MinIO is confirmed ready or fails after max retries.
    """
    def __init__(self, config: ExternalClientsConfig):
        """
        Initialize the S3-compatible MinIO client using configuration settings.
        Sets endpoint, credentials, and static region name (us-east-1).
        """
        self.config = config
        endpoint_url = f"http://{config.MINIO_ENDPOINT}"
        self.client = boto3.client(
            's3',
            endpoint_url=endpoint_url,
            aws_access_key_id=config.MINIO_ACCESS_KEY,
            aws_secret_access_key=config.MINIO_SECRET_KEY,
            region_name='us-east-1'
        )

    def _test_minio_ready(self, retries: int = 5, delay: float = 2.0) -> None:
        """
        Internal method to ensure MinIO is online and ready by repeatedly calling `list_buckets()`.

        Args:
            retries (int): Number of attempts before giving up.
            delay (float): Seconds to wait between attempts.

        Raises:
            RuntimeError if MinIO remains unavailable after all retries.
        """
        last_exception = None
        for _ in range(retries):
            try:
                self.client.list_buckets()
                return
            except (ClientError, EndpointConnectionError, Exception) as e:
                last_exception = e
                time.sleep(delay)
        raise RuntimeError(f"MinIO is not ready after {retries} attempts: {last_exception}")

    def get_client(self) -> boto3.client:
        """
        Perform readiness check and return a valid MinIO client for downstream usage.

        Returns:
            boto3 S3 client connected to MinIO.
        """
        self._test_minio_ready()
        return self.client


class RedisClientManager:
    """
    Manager for connecting to and validating availability of a Redis client.

    It initializes a Redis connection with connection timeouts, performs a readiness check using ping,
    and includes a helper for safe JSON retrieval.
    """
    def __init__(self, config: ExternalClientsConfig):
        """
        Initialize the Redis client using host and port from config.
        Uses health checks and timeouts for robust connection management.
        """
        self.config = config
        self.client = redis.Redis(
            host=config.REDIS_HOST,
            port=config.REDIS_PORT,
            decode_responses=True,
            socket_timeout=5.0,
            socket_connect_timeout=5.0,
            health_check_interval=30
        )

    def _test_redis_ready(self, retries: int = 5, delay: float = 2.0) -> None:
        """
        Internal method to ensure Redis is responsive by calling `ping()` multiple times.

        Args:
            retries (int): Maximum number of ping attempts.
            delay (float): Seconds between retries.

        Raises:
            RuntimeError if Redis fails to respond after all retries.
        """
        last_exception = None
        for _ in range(retries):
            try:
                if not self.client.ping():
                    raise Exception("Redis ping failed")
                return
            except Exception as e:
                last_exception = e
                time.sleep(delay)
        raise RuntimeError(f"Redis is not ready after {retries} attempts: {last_exception}")

    def get_client(self) -> redis.Redis:
        """
        Ensure Redis is ready and return the client for general-purpose use.

        Returns:
            redis.Redis: Configured and live Redis client instance.
        """
        self._test_redis_ready()
        return self.client

    def get_json(self, key: str) -> Optional[Dict[str, Any]]:
        """
        Safely retrieve and parse a Redis value as JSON.

        Args:
            key (str): Redis key to retrieve.

        Returns:
            Parsed dictionary if valid JSON string exists, otherwise None.
        """
        val = self.client.get(key)
        try:
            return json.loads(val) if val else None
        except json.JSONDecodeError:
            return None 


class ExternalClientsFactory:
    """
    Factory class to create and store initialized clients for both MinIO and Redis.

    Provides centralized access to the two external services used by the dashboard.
    """
    def __init__(self):
        """
        Create configuration and initialize both MinIO and Redis managers.
        Ensures clients are ready before exposing them to the app.
        """
        self.config = ExternalClientsConfig()
        self.minio = MinIOClientManager(self.config)
        self.redis = RedisClientManager(self.config)

    def get_minio(self) -> MinIOClientManager:
        """
        Retrieve the MinIO client manager instance.

        Returns:
            MinIOClientManager
        """
        return self.minio

    def get_redis(self) -> RedisClientManager:
        """
        Retrieve the Redis client manager instance.

        Returns:
            RedisClientManager
        """
        return self.redis

@st.cache_resource
def initialize_external_connections() -> ExternalClientsFactory:
    """
    Entry point function used by `dashboard.py` to initialize external services.

    Returns:
        ExternalClientsFactory: Contains ready-to-use MinIO and Redis clients.
    
    Decorated with `st.cache_resource` to ensure initialization happens only once
    per Streamlit session lifecycle.
    """
    return ExternalClientsFactory()

