"""
Encryption utilities — field-level encryption for PII/MNPI data.

Uses AWS KMS for key management, AES-256 for field encryption.
"""
import boto3
import base64
from cryptography.fernet import Fernet
from pyspark.sql import DataFrame, functions as F
from pyspark.sql.types import StringType
from typing import List
import logging

logger = logging.getLogger(__name__)


class FieldEncryptor:
    """Encrypt/decrypt individual columns using AWS KMS-managed keys."""
    
    def __init__(self, kms_key_id: str, region: str = "us-east-1"):
        self.kms_key_id = kms_key_id
        self.region = region
        self._data_key = None
        self._fernet = None
    
    def _get_data_key(self):
        """Generate a data key from KMS for envelope encryption."""
        if self._data_key is None:
            kms = boto3.client("kms", region_name=self.region)
            response = kms.generate_data_key(
                KeyId=self.kms_key_id,
                KeySpec="AES_256"
            )
            self._data_key = response["Plaintext"]
            self._fernet = Fernet(base64.urlsafe_b64encode(self._data_key[:32]))
        return self._fernet
    
    def encrypt_columns(self, df: DataFrame, columns: List[str]) -> DataFrame:
        """Encrypt specified columns in-place."""
        fernet = self._get_data_key()
        
        @F.udf(StringType())
        def encrypt_value(value):
            if value is None:
                return None
            return fernet.encrypt(value.encode()).decode()
        
        for col in columns:
            df = df.withColumn(col, encrypt_value(F.col(col)))
            logger.info(f"Encrypted column: {col}")
        
        return df
    
    def decrypt_columns(self, df: DataFrame, columns: List[str]) -> DataFrame:
        """Decrypt specified columns in-place."""
        fernet = self._get_data_key()
        
        @F.udf(StringType())
        def decrypt_value(value):
            if value is None:
                return None
            return fernet.decrypt(value.encode()).decode()
        
        for col in columns:
            df = df.withColumn(col, decrypt_value(F.col(col)))
        
        return df
