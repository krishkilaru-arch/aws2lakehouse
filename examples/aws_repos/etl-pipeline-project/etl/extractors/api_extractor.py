"""
API Extractor — Pull data from REST APIs (Shopify, Stripe, Segment).

Handles pagination, rate limiting, and retry logic.
"""
import requests
import time
import boto3
import json
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.types import StructType
from typing import List, Dict, Any, Optional
import logging

logger = logging.getLogger(__name__)


class APIExtractor:
    """Extract data from paginated REST APIs with rate limiting."""
    
    def __init__(self, spark: SparkSession, api_name: str):
        self.spark = spark
        self.api_name = api_name
        self.config = self._load_config()
        self.session = requests.Session()
        self.session.headers.update(self.config.get("headers", {}))
    
    def _load_config(self) -> dict:
        """Load API config from Secrets Manager."""
        client = boto3.client("secretsmanager", region_name="us-east-1")
        secret = client.get_secret_value(SecretId=f"prod/api/{self.api_name}")
        return json.loads(secret["SecretString"])
    
    def extract(self, endpoint: str, params: Dict[str, Any] = None,
               date_filter: str = None, max_pages: int = 100) -> DataFrame:
        """Extract all pages from an API endpoint."""
        all_records = []
        url = f"{self.config['base_url']}/{endpoint}"
        params = params or {}
        
        if date_filter:
            params["updated_at_min"] = date_filter
        
        page = 1
        while page <= max_pages:
            params["page"] = page
            response = self._request_with_retry(url, params)
            
            if not response:
                break
            
            data = response.json()
            records = data.get("data", data.get("results", data.get(endpoint, [])))
            
            if not records:
                break
            
            all_records.extend(records)
            logger.info(f"Page {page}: fetched {len(records)} records (total: {len(all_records)})")
            
            # Check for next page
            if not data.get("has_more", data.get("next_page")):
                break
            
            page += 1
            self._respect_rate_limit(response)
        
        # Convert to Spark DataFrame
        if all_records:
            return self.spark.createDataFrame(all_records)
        else:
            return self.spark.createDataFrame([], StructType([]))
    
    def _request_with_retry(self, url: str, params: dict, max_retries: int = 3):
        """HTTP request with exponential backoff."""
        for attempt in range(max_retries):
            try:
                response = self.session.get(url, params=params, timeout=30)
                if response.status_code == 200:
                    return response
                elif response.status_code == 429:  # Rate limited
                    wait = int(response.headers.get("Retry-After", 2 ** attempt))
                    logger.warning(f"Rate limited. Waiting {wait}s...")
                    time.sleep(wait)
                else:
                    logger.error(f"HTTP {response.status_code}: {response.text[:200]}")
            except requests.exceptions.RequestException as e:
                logger.error(f"Request failed (attempt {attempt+1}): {e}")
                time.sleep(2 ** attempt)
        return None
    
    def _respect_rate_limit(self, response):
        """Throttle based on rate limit headers."""
        remaining = int(response.headers.get("X-RateLimit-Remaining", 100))
        if remaining < 5:
            time.sleep(2)
