"""
    Connect to external clients.
"""

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
    MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT", "minio:9000")
    MINIO_ACCESS_KEY = os.getenv("AWS_ACCESS_KEY_ID", "minioadmin")
    MINIO_SECRET_KEY = os.getenv("AWS_SECRET_ACCESS_KEY", "minioadmin")
    MINIO_IMG_BUCKET = os.getenv("MINIO_IMG_BUCKET", "satellite-imgs")

    REDIS_HOST = os.getenv("REDIS_HOST", "redis")
    REDIS_PORT = int(os.getenv("REDIS_PORT", "6379"))


class MinIOClientManager:
    def __init__(self, config: ExternalClientsConfig):
        self.config = config
        endpoint_url = f"http://{config.MINIO_ENDPOINT}"
        self.client = boto3.client(
            's3',
            endpoint_url=endpoint_url,
            aws_access_key_id=config.MINIO_ACCESS_KEY,
            aws_secret_access_key=config.MINIO_SECRET_KEY,
            region_name='us-east-1'
        )

    def _test_minio_ready(self, retries: int = 5, delay: float = 2.0):
        """
        Try to call list_buckets() up to `retries` times with `delay` seconds between attempts.
        Raise RuntimeError if MinIO is not ready after all attempts.
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

    def get_client(self):
        self._test_minio_ready()
        return self.client


class RedisClientManager:
    def __init__(self, config: ExternalClientsConfig):
        self.config = config
        self.client = redis.Redis(
            host=config.REDIS_HOST,
            port=config.REDIS_PORT,
            decode_responses=True,
            socket_timeout=5.0,
            socket_connect_timeout=5.0,
            health_check_interval=30
        )

    def _test_redis_ready(self, retries: int = 5, delay: float = 2.0):
        """
        Try to ping Redis up to `retries` times with `delay` seconds between attempts.
        Raise RuntimeError if all attempts fail.
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

    def get_client(self):
        self._test_redis_ready()
        return self.client

    def get_json(self, key: str) -> Optional[Dict[str, Any]]:
        val = self.client.get(key)
        try:
            return json.loads(val) if val else None
        except json.JSONDecodeError:
            return None 


class ExternalClientsFactory:
    def __init__(self):
        self.config = ExternalClientsConfig()
        self.minio = MinIOClientManager(self.config)
        self.redis = RedisClientManager(self.config)

    def get_minio(self):
        return self.minio

    def get_redis(self):
        return self.redis

@st.cache_resource
def initialize_external_connections() -> ExternalClientsFactory:
    """
    This is the entry point used by dashboard.py
    """
    return ExternalClientsFactory()

