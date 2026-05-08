"""
Cluster Mapper — Maps EMR instance types and configurations to Databricks compute.

Handles:
- Instance type mapping (EC2 → Databricks node types)
- Autoscaling translation
- Spot/On-Demand strategy
- Memory/CPU ratio optimization
- Cost comparison estimates
"""

import logging
from dataclasses import dataclass
from typing import Optional

logger = logging.getLogger(__name__)


@dataclass
class DatabricksClusterConfig:
    """Databricks cluster configuration."""
    cluster_name: str
    spark_version: str = "15.4.x-scala2.12"
    node_type_id: str = "i3.xlarge"
    driver_node_type_id: str = ""
    num_workers: int = 4
    autoscale_min: Optional[int] = None
    autoscale_max: Optional[int] = None

    # Compute type recommendation
    compute_type: str = "job_cluster"  # job_cluster, serverless, shared

    # Spark configuration
    spark_conf: dict[str, str] = None

    # AWS attributes
    availability: str = "SPOT_WITH_FALLBACK"
    first_on_demand: int = 1
    ebs_volume_type: str = "GENERAL_PURPOSE_SSD"
    ebs_volume_count: int = 1
    ebs_volume_size: int = 100

    # Init scripts (from EMR bootstrap actions)
    init_scripts: list[dict] = None

    # Cost estimate
    estimated_dbu_per_hour: float = 0.0
    estimated_monthly_cost: float = 0.0
    cost_vs_emr_pct: float = 0.0  # negative = savings

    def __post_init__(self):
        self.spark_conf = self.spark_conf or {}
        self.init_scripts = self.init_scripts or []
        if not self.driver_node_type_id:
            self.driver_node_type_id = self.node_type_id

    def to_api_payload(self) -> dict:
        """Convert to Databricks Jobs API cluster spec."""
        cluster = {
            "spark_version": self.spark_version,
            "node_type_id": self.node_type_id,
            "driver_node_type_id": self.driver_node_type_id,
            "spark_conf": self.spark_conf,
            "aws_attributes": {
                "availability": self.availability,
                "first_on_demand": self.first_on_demand,
                "ebs_volume_type": self.ebs_volume_type,
                "ebs_volume_count": self.ebs_volume_count,
                "ebs_volume_size": self.ebs_volume_size
            }
        }

        if self.autoscale_min is not None:
            cluster["autoscale"] = {
                "min_workers": self.autoscale_min,
                "max_workers": self.autoscale_max or self.num_workers
            }
        else:
            cluster["num_workers"] = self.num_workers

        if self.init_scripts:
            cluster["init_scripts"] = self.init_scripts

        return cluster


class ClusterMapper:
    """
    Maps EMR cluster configurations to optimal Databricks compute.

    Mapping Strategy:
    1. Match memory/CPU ratio to best Databricks node type
    2. Account for EMR overhead (YARN, node managers)
    3. Apply Photon acceleration factor (typically 2-3x for SQL/ETL)
    4. Recommend compute type based on workload pattern

    Usage:
        mapper = ClusterMapper()
        config = mapper.map_emr_cluster(
            instance_type="r5.2xlarge",
            instance_count=10,
            emr_release="emr-6.15.0",
            workload_type="batch_etl"
        )
    """

    # EMR instance type specifications (comprehensive)
    # Source: AWS EC2 instance type catalog as of 2025
    EMR_INSTANCE_SPECS = {
        # ═══════════════════════════════════════════════════════════════
        # GENERAL PURPOSE (M-series) — balanced compute/memory
        # Best for: mixed workloads, orchestration tasks, small ETL
        # ═══════════════════════════════════════════════════════════════
        # M5 (Intel Xeon Platinum 8175M, 3.1 GHz)
        "m5.xlarge":    {"vcpu": 4,   "memory_gb": 16,   "category": "general", "network_gbps": 10,  "storage": "EBS", "price_per_hr": 0.192},
        "m5.2xlarge":   {"vcpu": 8,   "memory_gb": 32,   "category": "general", "network_gbps": 10,  "storage": "EBS", "price_per_hr": 0.384},
        "m5.4xlarge":   {"vcpu": 16,  "memory_gb": 64,   "category": "general", "network_gbps": 10,  "storage": "EBS", "price_per_hr": 0.768},
        "m5.8xlarge":   {"vcpu": 32,  "memory_gb": 128,  "category": "general", "network_gbps": 10,  "storage": "EBS", "price_per_hr": 1.536},
        "m5.12xlarge":  {"vcpu": 48,  "memory_gb": 192,  "category": "general", "network_gbps": 12,  "storage": "EBS", "price_per_hr": 2.304},
        "m5.16xlarge":  {"vcpu": 64,  "memory_gb": 256,  "category": "general", "network_gbps": 20,  "storage": "EBS", "price_per_hr": 3.072},
        "m5.24xlarge":  {"vcpu": 96,  "memory_gb": 384,  "category": "general", "network_gbps": 25,  "storage": "EBS", "price_per_hr": 4.608},
        # M5a (AMD EPYC 7000, ~10% cheaper than M5)
        "m5a.xlarge":   {"vcpu": 4,   "memory_gb": 16,   "category": "general", "network_gbps": 10,  "storage": "EBS", "price_per_hr": 0.172},
        "m5a.2xlarge":  {"vcpu": 8,   "memory_gb": 32,   "category": "general", "network_gbps": 10,  "storage": "EBS", "price_per_hr": 0.344},
        "m5a.4xlarge":  {"vcpu": 16,  "memory_gb": 64,   "category": "general", "network_gbps": 10,  "storage": "EBS", "price_per_hr": 0.688},
        "m5a.8xlarge":  {"vcpu": 32,  "memory_gb": 128,  "category": "general", "network_gbps": 10,  "storage": "EBS", "price_per_hr": 1.376},
        "m5a.12xlarge": {"vcpu": 48,  "memory_gb": 192,  "category": "general", "network_gbps": 10,  "storage": "EBS", "price_per_hr": 2.064},
        "m5a.16xlarge": {"vcpu": 64,  "memory_gb": 256,  "category": "general", "network_gbps": 12,  "storage": "EBS", "price_per_hr": 2.752},
        "m5a.24xlarge": {"vcpu": 96,  "memory_gb": 384,  "category": "general", "network_gbps": 20,  "storage": "EBS", "price_per_hr": 4.128},
        # M5d (M5 + local NVMe SSD)
        "m5d.xlarge":   {"vcpu": 4,   "memory_gb": 16,   "category": "general", "network_gbps": 10,  "storage": "1x150GB NVMe", "price_per_hr": 0.226},
        "m5d.2xlarge":  {"vcpu": 8,   "memory_gb": 32,   "category": "general", "network_gbps": 10,  "storage": "1x300GB NVMe", "price_per_hr": 0.452},
        "m5d.4xlarge":  {"vcpu": 16,  "memory_gb": 64,   "category": "general", "network_gbps": 10,  "storage": "2x300GB NVMe", "price_per_hr": 0.904},
        "m5d.8xlarge":  {"vcpu": 32,  "memory_gb": 128,  "category": "general", "network_gbps": 10,  "storage": "2x600GB NVMe", "price_per_hr": 1.808},
        "m5d.12xlarge": {"vcpu": 48,  "memory_gb": 192,  "category": "general", "network_gbps": 12,  "storage": "2x900GB NVMe", "price_per_hr": 2.712},
        "m5d.16xlarge": {"vcpu": 64,  "memory_gb": 256,  "category": "general", "network_gbps": 20,  "storage": "4x600GB NVMe", "price_per_hr": 3.616},
        "m5d.24xlarge": {"vcpu": 96,  "memory_gb": 384,  "category": "general", "network_gbps": 25,  "storage": "4x900GB NVMe", "price_per_hr": 5.424},
        # M6i (Intel Xeon Scalable 3rd Gen, Ice Lake)
        "m6i.xlarge":   {"vcpu": 4,   "memory_gb": 16,   "category": "general", "network_gbps": 12.5, "storage": "EBS", "price_per_hr": 0.192},
        "m6i.2xlarge":  {"vcpu": 8,   "memory_gb": 32,   "category": "general", "network_gbps": 12.5, "storage": "EBS", "price_per_hr": 0.384},
        "m6i.4xlarge":  {"vcpu": 16,  "memory_gb": 64,   "category": "general", "network_gbps": 12.5, "storage": "EBS", "price_per_hr": 0.768},
        "m6i.8xlarge":  {"vcpu": 32,  "memory_gb": 128,  "category": "general", "network_gbps": 12.5, "storage": "EBS", "price_per_hr": 1.536},
        "m6i.12xlarge": {"vcpu": 48,  "memory_gb": 192,  "category": "general", "network_gbps": 18.75,"storage": "EBS", "price_per_hr": 2.304},
        "m6i.16xlarge": {"vcpu": 64,  "memory_gb": 256,  "category": "general", "network_gbps": 25,  "storage": "EBS", "price_per_hr": 3.072},
        "m6i.24xlarge": {"vcpu": 96,  "memory_gb": 384,  "category": "general", "network_gbps": 37.5, "storage": "EBS", "price_per_hr": 4.608},
        "m6i.32xlarge": {"vcpu": 128, "memory_gb": 512,  "category": "general", "network_gbps": 50,  "storage": "EBS", "price_per_hr": 6.144},
        # M6g (AWS Graviton2 ARM — not supported on Databricks, flag for migration)
        "m6g.xlarge":   {"vcpu": 4,   "memory_gb": 16,   "category": "general_arm", "network_gbps": 10, "storage": "EBS", "price_per_hr": 0.154},
        "m6g.2xlarge":  {"vcpu": 8,   "memory_gb": 32,   "category": "general_arm", "network_gbps": 10, "storage": "EBS", "price_per_hr": 0.308},
        "m6g.4xlarge":  {"vcpu": 16,  "memory_gb": 64,   "category": "general_arm", "network_gbps": 10, "storage": "EBS", "price_per_hr": 0.616},
        "m6g.8xlarge":  {"vcpu": 32,  "memory_gb": 128,  "category": "general_arm", "network_gbps": 12, "storage": "EBS", "price_per_hr": 1.232},
        "m6g.12xlarge": {"vcpu": 48,  "memory_gb": 192,  "category": "general_arm", "network_gbps": 20, "storage": "EBS", "price_per_hr": 1.848},
        "m6g.16xlarge": {"vcpu": 64,  "memory_gb": 256,  "category": "general_arm", "network_gbps": 25, "storage": "EBS", "price_per_hr": 2.464},
        # M7i (Intel Xeon Sapphire Rapids, latest gen)
        "m7i.xlarge":   {"vcpu": 4,   "memory_gb": 16,   "category": "general", "network_gbps": 12.5, "storage": "EBS", "price_per_hr": 0.202},
        "m7i.2xlarge":  {"vcpu": 8,   "memory_gb": 32,   "category": "general", "network_gbps": 12.5, "storage": "EBS", "price_per_hr": 0.403},
        "m7i.4xlarge":  {"vcpu": 16,  "memory_gb": 64,   "category": "general", "network_gbps": 12.5, "storage": "EBS", "price_per_hr": 0.806},
        "m7i.8xlarge":  {"vcpu": 32,  "memory_gb": 128,  "category": "general", "network_gbps": 12.5, "storage": "EBS", "price_per_hr": 1.613},
        "m7i.12xlarge": {"vcpu": 48,  "memory_gb": 192,  "category": "general", "network_gbps": 18.75,"storage": "EBS", "price_per_hr": 2.419},
        "m7i.16xlarge": {"vcpu": 64,  "memory_gb": 256,  "category": "general", "network_gbps": 25,  "storage": "EBS", "price_per_hr": 3.226},
        "m7i.24xlarge": {"vcpu": 96,  "memory_gb": 384,  "category": "general", "network_gbps": 37.5, "storage": "EBS", "price_per_hr": 4.838},
        "m7i.48xlarge": {"vcpu": 192, "memory_gb": 768,  "category": "general", "network_gbps": 50,  "storage": "EBS", "price_per_hr": 9.677},

        # ═══════════════════════════════════════════════════════════════
        # MEMORY OPTIMIZED (R-series) — high memory:CPU ratio
        # Best for: large shuffles, caching, broadcast joins, Spark SQL
        # ═══════════════════════════════════════════════════════════════
        # R5 (Intel Xeon Platinum 8175M)
        "r5.xlarge":    {"vcpu": 4,   "memory_gb": 32,   "category": "memory", "network_gbps": 10,  "storage": "EBS", "price_per_hr": 0.252},
        "r5.2xlarge":   {"vcpu": 8,   "memory_gb": 64,   "category": "memory", "network_gbps": 10,  "storage": "EBS", "price_per_hr": 0.504},
        "r5.4xlarge":   {"vcpu": 16,  "memory_gb": 128,  "category": "memory", "network_gbps": 10,  "storage": "EBS", "price_per_hr": 1.008},
        "r5.8xlarge":   {"vcpu": 32,  "memory_gb": 256,  "category": "memory", "network_gbps": 10,  "storage": "EBS", "price_per_hr": 2.016},
        "r5.12xlarge":  {"vcpu": 48,  "memory_gb": 384,  "category": "memory", "network_gbps": 12,  "storage": "EBS", "price_per_hr": 3.024},
        "r5.16xlarge":  {"vcpu": 64,  "memory_gb": 512,  "category": "memory", "network_gbps": 20,  "storage": "EBS", "price_per_hr": 4.032},
        "r5.24xlarge":  {"vcpu": 96,  "memory_gb": 768,  "category": "memory", "network_gbps": 25,  "storage": "EBS", "price_per_hr": 6.048},
        # R5a (AMD EPYC)
        "r5a.xlarge":   {"vcpu": 4,   "memory_gb": 32,   "category": "memory", "network_gbps": 10,  "storage": "EBS", "price_per_hr": 0.226},
        "r5a.2xlarge":  {"vcpu": 8,   "memory_gb": 64,   "category": "memory", "network_gbps": 10,  "storage": "EBS", "price_per_hr": 0.452},
        "r5a.4xlarge":  {"vcpu": 16,  "memory_gb": 128,  "category": "memory", "network_gbps": 10,  "storage": "EBS", "price_per_hr": 0.904},
        "r5a.8xlarge":  {"vcpu": 32,  "memory_gb": 256,  "category": "memory", "network_gbps": 10,  "storage": "EBS", "price_per_hr": 1.808},
        "r5a.12xlarge": {"vcpu": 48,  "memory_gb": 384,  "category": "memory", "network_gbps": 10,  "storage": "EBS", "price_per_hr": 2.712},
        "r5a.16xlarge": {"vcpu": 64,  "memory_gb": 512,  "category": "memory", "network_gbps": 12,  "storage": "EBS", "price_per_hr": 3.616},
        "r5a.24xlarge": {"vcpu": 96,  "memory_gb": 768,  "category": "memory", "network_gbps": 20,  "storage": "EBS", "price_per_hr": 5.424},
        # R5d (R5 + local NVMe)
        "r5d.xlarge":   {"vcpu": 4,   "memory_gb": 32,   "category": "memory", "network_gbps": 10,  "storage": "1x150GB NVMe", "price_per_hr": 0.288},
        "r5d.2xlarge":  {"vcpu": 8,   "memory_gb": 64,   "category": "memory", "network_gbps": 10,  "storage": "1x300GB NVMe", "price_per_hr": 0.576},
        "r5d.4xlarge":  {"vcpu": 16,  "memory_gb": 128,  "category": "memory", "network_gbps": 10,  "storage": "2x300GB NVMe", "price_per_hr": 1.152},
        "r5d.8xlarge":  {"vcpu": 32,  "memory_gb": 256,  "category": "memory", "network_gbps": 10,  "storage": "2x600GB NVMe", "price_per_hr": 2.304},
        "r5d.12xlarge": {"vcpu": 48,  "memory_gb": 384,  "category": "memory", "network_gbps": 12,  "storage": "2x900GB NVMe", "price_per_hr": 3.456},
        "r5d.16xlarge": {"vcpu": 64,  "memory_gb": 512,  "category": "memory", "network_gbps": 20,  "storage": "4x600GB NVMe", "price_per_hr": 4.608},
        "r5d.24xlarge": {"vcpu": 96,  "memory_gb": 768,  "category": "memory", "network_gbps": 25,  "storage": "4x900GB NVMe", "price_per_hr": 6.912},
        # R6i (Intel Ice Lake)
        "r6i.xlarge":   {"vcpu": 4,   "memory_gb": 32,   "category": "memory", "network_gbps": 12.5, "storage": "EBS", "price_per_hr": 0.252},
        "r6i.2xlarge":  {"vcpu": 8,   "memory_gb": 64,   "category": "memory", "network_gbps": 12.5, "storage": "EBS", "price_per_hr": 0.504},
        "r6i.4xlarge":  {"vcpu": 16,  "memory_gb": 128,  "category": "memory", "network_gbps": 12.5, "storage": "EBS", "price_per_hr": 1.008},
        "r6i.8xlarge":  {"vcpu": 32,  "memory_gb": 256,  "category": "memory", "network_gbps": 12.5, "storage": "EBS", "price_per_hr": 2.016},
        "r6i.12xlarge": {"vcpu": 48,  "memory_gb": 384,  "category": "memory", "network_gbps": 18.75,"storage": "EBS", "price_per_hr": 3.024},
        "r6i.16xlarge": {"vcpu": 64,  "memory_gb": 512,  "category": "memory", "network_gbps": 25,  "storage": "EBS", "price_per_hr": 4.032},
        "r6i.24xlarge": {"vcpu": 96,  "memory_gb": 768,  "category": "memory", "network_gbps": 37.5, "storage": "EBS", "price_per_hr": 6.048},
        "r6i.32xlarge": {"vcpu": 128, "memory_gb": 1024, "category": "memory", "network_gbps": 50,  "storage": "EBS", "price_per_hr": 8.064},
        # R6g (Graviton2 ARM — needs x86 equivalent mapping)
        "r6g.xlarge":   {"vcpu": 4,   "memory_gb": 32,   "category": "memory_arm", "network_gbps": 10, "storage": "EBS", "price_per_hr": 0.202},
        "r6g.2xlarge":  {"vcpu": 8,   "memory_gb": 64,   "category": "memory_arm", "network_gbps": 10, "storage": "EBS", "price_per_hr": 0.403},
        "r6g.4xlarge":  {"vcpu": 16,  "memory_gb": 128,  "category": "memory_arm", "network_gbps": 10, "storage": "EBS", "price_per_hr": 0.806},
        "r6g.8xlarge":  {"vcpu": 32,  "memory_gb": 256,  "category": "memory_arm", "network_gbps": 12, "storage": "EBS", "price_per_hr": 1.613},
        "r6g.12xlarge": {"vcpu": 48,  "memory_gb": 384,  "category": "memory_arm", "network_gbps": 20, "storage": "EBS", "price_per_hr": 2.419},
        "r6g.16xlarge": {"vcpu": 64,  "memory_gb": 512,  "category": "memory_arm", "network_gbps": 25, "storage": "EBS", "price_per_hr": 3.226},

        # ═══════════════════════════════════════════════════════════════
        # COMPUTE OPTIMIZED (C-series) — high CPU:memory ratio
        # Best for: CPU-bound transforms, ML inference, UDFs, compression
        # ═══════════════════════════════════════════════════════════════
        # C5 (Intel Xeon Platinum 8000, 3.4 GHz turbo)
        "c5.xlarge":    {"vcpu": 4,   "memory_gb": 8,    "category": "compute", "network_gbps": 10,  "storage": "EBS", "price_per_hr": 0.170},
        "c5.2xlarge":   {"vcpu": 8,   "memory_gb": 16,   "category": "compute", "network_gbps": 10,  "storage": "EBS", "price_per_hr": 0.340},
        "c5.4xlarge":   {"vcpu": 16,  "memory_gb": 32,   "category": "compute", "network_gbps": 10,  "storage": "EBS", "price_per_hr": 0.680},
        "c5.9xlarge":   {"vcpu": 36,  "memory_gb": 72,   "category": "compute", "network_gbps": 12,  "storage": "EBS", "price_per_hr": 1.530},
        "c5.12xlarge":  {"vcpu": 48,  "memory_gb": 96,   "category": "compute", "network_gbps": 12,  "storage": "EBS", "price_per_hr": 2.040},
        "c5.18xlarge":  {"vcpu": 72,  "memory_gb": 144,  "category": "compute", "network_gbps": 25,  "storage": "EBS", "price_per_hr": 3.060},
        "c5.24xlarge":  {"vcpu": 96,  "memory_gb": 192,  "category": "compute", "network_gbps": 25,  "storage": "EBS", "price_per_hr": 4.080},
        # C5d (C5 + local NVMe)
        "c5d.xlarge":   {"vcpu": 4,   "memory_gb": 8,    "category": "compute", "network_gbps": 10,  "storage": "1x50GB NVMe",  "price_per_hr": 0.192},
        "c5d.2xlarge":  {"vcpu": 8,   "memory_gb": 16,   "category": "compute", "network_gbps": 10,  "storage": "1x200GB NVMe", "price_per_hr": 0.384},
        "c5d.4xlarge":  {"vcpu": 16,  "memory_gb": 32,   "category": "compute", "network_gbps": 10,  "storage": "1x400GB NVMe", "price_per_hr": 0.768},
        "c5d.9xlarge":  {"vcpu": 36,  "memory_gb": 72,   "category": "compute", "network_gbps": 12,  "storage": "1x900GB NVMe", "price_per_hr": 1.728},
        # C6i (Intel Ice Lake)
        "c6i.xlarge":   {"vcpu": 4,   "memory_gb": 8,    "category": "compute", "network_gbps": 12.5, "storage": "EBS", "price_per_hr": 0.170},
        "c6i.2xlarge":  {"vcpu": 8,   "memory_gb": 16,   "category": "compute", "network_gbps": 12.5, "storage": "EBS", "price_per_hr": 0.340},
        "c6i.4xlarge":  {"vcpu": 16,  "memory_gb": 32,   "category": "compute", "network_gbps": 12.5, "storage": "EBS", "price_per_hr": 0.680},
        "c6i.8xlarge":  {"vcpu": 32,  "memory_gb": 64,   "category": "compute", "network_gbps": 12.5, "storage": "EBS", "price_per_hr": 1.360},
        "c6i.12xlarge": {"vcpu": 48,  "memory_gb": 96,   "category": "compute", "network_gbps": 18.75,"storage": "EBS", "price_per_hr": 2.040},
        "c6i.16xlarge": {"vcpu": 64,  "memory_gb": 128,  "category": "compute", "network_gbps": 25,  "storage": "EBS", "price_per_hr": 2.720},
        "c6i.24xlarge": {"vcpu": 96,  "memory_gb": 192,  "category": "compute", "network_gbps": 37.5, "storage": "EBS", "price_per_hr": 4.080},
        "c6i.32xlarge": {"vcpu": 128, "memory_gb": 256,  "category": "compute", "network_gbps": 50,  "storage": "EBS", "price_per_hr": 5.440},

        # ═══════════════════════════════════════════════════════════════
        # STORAGE OPTIMIZED (I/D-series) — high I/O, local NVMe
        # Best for: shuffle-heavy, large datasets, Delta Lake compaction
        # ═══════════════════════════════════════════════════════════════
        # I3 (Intel Xeon E5-2686 v4, high sequential I/O)
        "i3.xlarge":    {"vcpu": 4,   "memory_gb": 30.5,  "category": "storage", "network_gbps": 10,  "storage": "1x950GB NVMe",  "price_per_hr": 0.312},
        "i3.2xlarge":   {"vcpu": 8,   "memory_gb": 61,    "category": "storage", "network_gbps": 10,  "storage": "1x1900GB NVMe", "price_per_hr": 0.624},
        "i3.4xlarge":   {"vcpu": 16,  "memory_gb": 122,   "category": "storage", "network_gbps": 10,  "storage": "2x1900GB NVMe", "price_per_hr": 1.248},
        "i3.8xlarge":   {"vcpu": 32,  "memory_gb": 244,   "category": "storage", "network_gbps": 10,  "storage": "4x1900GB NVMe", "price_per_hr": 2.496},
        "i3.16xlarge":  {"vcpu": 64,  "memory_gb": 488,   "category": "storage", "network_gbps": 25,  "storage": "8x1900GB NVMe", "price_per_hr": 4.992},
        # I3en (enhanced networking, more NVMe)
        "i3en.xlarge":  {"vcpu": 4,   "memory_gb": 32,    "category": "storage", "network_gbps": 25,  "storage": "1x2500GB NVMe", "price_per_hr": 0.452},
        "i3en.2xlarge": {"vcpu": 8,   "memory_gb": 64,    "category": "storage", "network_gbps": 25,  "storage": "2x2500GB NVMe", "price_per_hr": 0.904},
        "i3en.3xlarge": {"vcpu": 12,  "memory_gb": 96,    "category": "storage", "network_gbps": 25,  "storage": "1x7500GB NVMe", "price_per_hr": 1.356},
        "i3en.6xlarge": {"vcpu": 24,  "memory_gb": 192,   "category": "storage", "network_gbps": 25,  "storage": "2x7500GB NVMe", "price_per_hr": 2.712},
        "i3en.12xlarge":{"vcpu": 48,  "memory_gb": 384,   "category": "storage", "network_gbps": 50,  "storage": "4x7500GB NVMe", "price_per_hr": 5.424},
        "i3en.24xlarge":{"vcpu": 96,  "memory_gb": 768,   "category": "storage", "network_gbps": 100, "storage": "8x7500GB NVMe", "price_per_hr": 10.848},
        # D2 (dense storage, HDD — legacy)
        "d2.xlarge":    {"vcpu": 4,   "memory_gb": 30.5,  "category": "storage", "network_gbps": 10,  "storage": "3x2TB HDD",    "price_per_hr": 0.690},
        "d2.2xlarge":   {"vcpu": 8,   "memory_gb": 61,    "category": "storage", "network_gbps": 10,  "storage": "6x2TB HDD",    "price_per_hr": 1.380},
        "d2.4xlarge":   {"vcpu": 16,  "memory_gb": 122,   "category": "storage", "network_gbps": 10,  "storage": "12x2TB HDD",   "price_per_hr": 2.760},
        "d2.8xlarge":   {"vcpu": 36,  "memory_gb": 244,   "category": "storage", "network_gbps": 10,  "storage": "24x2TB HDD",   "price_per_hr": 5.520},

        # ═══════════════════════════════════════════════════════════════
        # GPU INSTANCES — ML training, deep learning, GPU-accelerated ETL
        # Best for: ML model training, inference, GPU UDFs
        # ═══════════════════════════════════════════════════════════════
        # P3 (NVIDIA V100 16GB)
        "p3.2xlarge":   {"vcpu": 8,   "memory_gb": 61,    "category": "gpu", "gpus": 1, "gpu_type": "V100", "gpu_mem_gb": 16,  "price_per_hr": 3.060},
        "p3.8xlarge":   {"vcpu": 32,  "memory_gb": 244,   "category": "gpu", "gpus": 4, "gpu_type": "V100", "gpu_mem_gb": 64,  "price_per_hr": 12.240},
        "p3.16xlarge":  {"vcpu": 64,  "memory_gb": 488,   "category": "gpu", "gpus": 8, "gpu_type": "V100", "gpu_mem_gb": 128, "price_per_hr": 24.480},
        # P4d (NVIDIA A100 40GB — latest high-end)
        "p4d.24xlarge": {"vcpu": 96,  "memory_gb": 1152,  "category": "gpu", "gpus": 8, "gpu_type": "A100", "gpu_mem_gb": 320, "price_per_hr": 32.773},
        # P5 (NVIDIA H100 80GB — newest)
        "p5.48xlarge":  {"vcpu": 192, "memory_gb": 2048,  "category": "gpu", "gpus": 8, "gpu_type": "H100", "gpu_mem_gb": 640, "price_per_hr": 98.32},
        # G4dn (NVIDIA T4 — inference/light training)
        "g4dn.xlarge":  {"vcpu": 4,   "memory_gb": 16,    "category": "gpu", "gpus": 1, "gpu_type": "T4",   "gpu_mem_gb": 16,  "price_per_hr": 0.526},
        "g4dn.2xlarge": {"vcpu": 8,   "memory_gb": 32,    "category": "gpu", "gpus": 1, "gpu_type": "T4",   "gpu_mem_gb": 16,  "price_per_hr": 0.752},
        "g4dn.4xlarge": {"vcpu": 16,  "memory_gb": 64,    "category": "gpu", "gpus": 1, "gpu_type": "T4",   "gpu_mem_gb": 16,  "price_per_hr": 1.204},
        "g4dn.8xlarge": {"vcpu": 32,  "memory_gb": 128,   "category": "gpu", "gpus": 1, "gpu_type": "T4",   "gpu_mem_gb": 16,  "price_per_hr": 2.176},
        "g4dn.12xlarge":{"vcpu": 48,  "memory_gb": 192,   "category": "gpu", "gpus": 4, "gpu_type": "T4",   "gpu_mem_gb": 64,  "price_per_hr": 3.912},
        "g4dn.16xlarge":{"vcpu": 64,  "memory_gb": 256,   "category": "gpu", "gpus": 1, "gpu_type": "T4",   "gpu_mem_gb": 16,  "price_per_hr": 4.352},
        # G5 (NVIDIA A10G — best price/performance for ML)
        "g5.xlarge":    {"vcpu": 4,   "memory_gb": 16,    "category": "gpu", "gpus": 1, "gpu_type": "A10G", "gpu_mem_gb": 24,  "price_per_hr": 1.006},
        "g5.2xlarge":   {"vcpu": 8,   "memory_gb": 32,    "category": "gpu", "gpus": 1, "gpu_type": "A10G", "gpu_mem_gb": 24,  "price_per_hr": 1.212},
        "g5.4xlarge":   {"vcpu": 16,  "memory_gb": 64,    "category": "gpu", "gpus": 1, "gpu_type": "A10G", "gpu_mem_gb": 24,  "price_per_hr": 1.624},
        "g5.8xlarge":   {"vcpu": 32,  "memory_gb": 128,   "category": "gpu", "gpus": 1, "gpu_type": "A10G", "gpu_mem_gb": 24,  "price_per_hr": 2.448},
        "g5.12xlarge":  {"vcpu": 48,  "memory_gb": 192,   "category": "gpu", "gpus": 4, "gpu_type": "A10G", "gpu_mem_gb": 96,  "price_per_hr": 5.672},
        "g5.24xlarge":  {"vcpu": 96,  "memory_gb": 384,   "category": "gpu", "gpus": 4, "gpu_type": "A10G", "gpu_mem_gb": 96,  "price_per_hr": 8.144},
        "g5.48xlarge":  {"vcpu": 192, "memory_gb": 768,   "category": "gpu", "gpus": 8, "gpu_type": "A10G", "gpu_mem_gb": 192, "price_per_hr": 16.288},

        # ═══════════════════════════════════════════════════════════════
        # HIGH MEMORY (X/U-series) — very large memory footprint
        # Best for: in-memory analytics, very large broadcasts, graph
        # ═══════════════════════════════════════════════════════════════
        "x1.16xlarge":  {"vcpu": 64,  "memory_gb": 976,   "category": "highmem", "network_gbps": 10,  "storage": "1x1920GB SSD", "price_per_hr": 6.669},
        "x1.32xlarge":  {"vcpu": 128, "memory_gb": 1952,  "category": "highmem", "network_gbps": 25,  "storage": "2x1920GB SSD", "price_per_hr": 13.338},
        "x1e.xlarge":   {"vcpu": 4,   "memory_gb": 122,   "category": "highmem", "network_gbps": 10,  "storage": "1x120GB SSD",  "price_per_hr": 0.834},
        "x1e.2xlarge":  {"vcpu": 8,   "memory_gb": 244,   "category": "highmem", "network_gbps": 10,  "storage": "1x240GB SSD",  "price_per_hr": 1.668},
        "x1e.4xlarge":  {"vcpu": 16,  "memory_gb": 488,   "category": "highmem", "network_gbps": 10,  "storage": "1x480GB SSD",  "price_per_hr": 3.336},
        "x1e.8xlarge":  {"vcpu": 32,  "memory_gb": 976,   "category": "highmem", "network_gbps": 10,  "storage": "1x960GB SSD",  "price_per_hr": 6.672},
        "x1e.16xlarge": {"vcpu": 64,  "memory_gb": 1952,  "category": "highmem", "network_gbps": 10,  "storage": "1x1920GB SSD", "price_per_hr": 13.344},
        "x1e.32xlarge": {"vcpu": 128, "memory_gb": 3904,  "category": "highmem", "network_gbps": 25,  "storage": "2x1920GB SSD", "price_per_hr": 26.688},
        "x2idn.16xlarge":{"vcpu": 64, "memory_gb": 1024,  "category": "highmem", "network_gbps": 50,  "storage": "1x1900GB NVMe","price_per_hr": 6.669},
        "x2idn.24xlarge":{"vcpu": 96, "memory_gb": 1536,  "category": "highmem", "network_gbps": 75,  "storage": "2x1900GB NVMe","price_per_hr": 10.003},
        "x2idn.32xlarge":{"vcpu": 128,"memory_gb": 2048,  "category": "highmem", "network_gbps": 100, "storage": "2x1900GB NVMe","price_per_hr": 13.338},
    }

    # Databricks node type mapping (best match by use case)
    DATABRICKS_NODE_MAP = {
        # category -> {size_class -> databricks_node_type}
        "general": {
            "small":  "m5.xlarge",
            "medium": "m5.2xlarge",
            "large":  "m5.4xlarge",
            "xlarge": "m5.8xlarge",
            "2xlarge": "m5.16xlarge",
        },
        "general_arm": {  # Graviton → map to x86 equivalent on Databricks
            "small":  "m5.xlarge",
            "medium": "m5.2xlarge",
            "large":  "m5.4xlarge",
            "xlarge": "m5.8xlarge",
            "2xlarge": "m5.16xlarge",
        },
        "memory": {
            "small":  "r5.xlarge",
            "medium": "r5.2xlarge",
            "large":  "r5.4xlarge",
            "xlarge": "r5.8xlarge",
            "2xlarge": "r5.16xlarge",
        },
        "memory_arm": {  # Graviton → map to x86 equivalent on Databricks
            "small":  "r5.xlarge",
            "medium": "r5.2xlarge",
            "large":  "r5.4xlarge",
            "xlarge": "r5.8xlarge",
            "2xlarge": "r5.16xlarge",
        },
        "compute": {
            "small":  "c5.xlarge",
            "medium": "c5.2xlarge",
            "large":  "c5.4xlarge",
            "xlarge": "c5.9xlarge",
            "2xlarge": "c5.18xlarge",
        },
        "storage": {
            "small":  "i3.xlarge",
            "medium": "i3.2xlarge",
            "large":  "i3.4xlarge",
            "xlarge": "i3.8xlarge",
            "2xlarge": "i3.16xlarge",
        },
        "gpu": {
            "small":  "g4dn.xlarge",      # T4 — inference, light training
            "medium": "g5.xlarge",         # A10G — best price/perf for ML
            "large":  "p3.2xlarge",        # V100 — heavy training
            "xlarge": "p3.8xlarge",        # 4x V100
            "2xlarge": "p4d.24xlarge",     # 8x A100 — distributed training
        },
        "highmem": {
            "small":  "r5.8xlarge",        # No direct equiv, use large memory
            "medium": "r5.16xlarge",
            "large":  "r5.24xlarge",
            "xlarge": "x1.16xlarge",       # Only if truly needed
            "2xlarge": "x1.32xlarge",
        },
    }

    # Photon acceleration factors by workload type
    PHOTON_FACTORS = {
        "batch_etl": 2.5,       # ETL benefits heavily from Photon
        "sql_analytics": 3.0,   # SQL gets best Photon speedup
        "ml_training": 1.0,     # ML doesn't benefit from Photon
        "streaming": 1.5,       # Moderate benefit
        "general": 2.0,
    }

    def __init__(self, photon_enabled: bool = True, target_runtime: str = "15.4.x-scala2.12"):
        self.photon_enabled = photon_enabled
        self.target_runtime = target_runtime
        if photon_enabled and "photon" not in target_runtime:
            self.target_runtime = target_runtime.replace("-scala2.12", "-photon-scala2.12")

    def map_emr_cluster(
        self,
        instance_type: str,
        instance_count: int,
        emr_release: str = "emr-6.15.0",
        workload_type: str = "batch_etl",
        avg_utilization: float = 0.7,
        has_autoscaling: bool = False,
        min_instances: int = None,
        max_instances: int = None,
        bootstrap_actions: list[dict] = None,
        spark_config: dict[str, str] = None
    ) -> DatabricksClusterConfig:
        """
        Map an EMR cluster configuration to Databricks.

        Args:
            instance_type: EC2 instance type (e.g., "r5.2xlarge")
            instance_count: Number of core/task nodes
            emr_release: EMR release label
            workload_type: "batch_etl", "sql_analytics", "ml_training", "streaming"
            avg_utilization: Average cluster utilization (0-1)
            has_autoscaling: Whether EMR autoscaling is enabled
            min_instances: Minimum instances (if autoscaling)
            max_instances: Maximum instances (if autoscaling)
            bootstrap_actions: EMR bootstrap actions to convert to init scripts
            spark_config: EMR Spark configurations
        """
        bootstrap_actions = bootstrap_actions or []
        spark_config = spark_config or {}

        # Get EMR instance specs
        specs = self.EMR_INSTANCE_SPECS.get(instance_type, {})
        if not specs:
            logger.warning(f"Unknown instance type: {instance_type}, using defaults")
            specs = {"vcpu": 4, "memory_gb": 16, "category": "general"}

        # Calculate total cluster resources
        specs["vcpu"] * instance_count
        specs["memory_gb"] * instance_count
        category = specs["category"]

        # Apply Photon factor — Databricks needs fewer nodes
        photon_factor = self.PHOTON_FACTORS.get(workload_type, 2.0) if self.photon_enabled else 1.0

        # Calculate right-sized Databricks cluster
        # Factor in: Photon speedup + better resource management + avg utilization
        effective_factor = photon_factor * (1 / max(avg_utilization, 0.3))
        target_nodes = max(1, int(instance_count / effective_factor))

        # Map to Databricks node type
        databricks_node = self._select_node_type(specs, category)

        # Determine compute type
        compute_type = self._recommend_compute_type(
            workload_type, instance_count, spark_config
        )

        # Convert Spark configs (remove EMR-specific, add Databricks-specific)
        db_spark_conf = self._convert_spark_config(spark_config, emr_release)

        # Convert bootstrap actions to init scripts
        init_scripts = self._convert_bootstrap_actions(bootstrap_actions)

        # Build cluster config
        config = DatabricksClusterConfig(
            cluster_name=f"migrated_{instance_type}_{instance_count}n",
            spark_version=self.target_runtime,
            node_type_id=databricks_node,
            num_workers=target_nodes,
            compute_type=compute_type,
            spark_conf=db_spark_conf,
            init_scripts=init_scripts
        )

        # Handle autoscaling
        if has_autoscaling and min_instances and max_instances:
            config.autoscale_min = max(1, int(min_instances / effective_factor))
            config.autoscale_max = max(config.autoscale_min + 1, int(max_instances / effective_factor))

        # Cost estimation
        config.estimated_dbu_per_hour = target_nodes * self._dbu_rate(databricks_node)
        config.estimated_monthly_cost = config.estimated_dbu_per_hour * 720 * 0.15  # ~$0.15/DBU

        logger.info(
            f"Mapped EMR {instance_type}x{instance_count} → "
            f"Databricks {databricks_node}x{target_nodes} "
            f"({compute_type}, photon={self.photon_enabled})"
        )

        return config

    def _select_node_type(self, specs: dict, category: str) -> str:
        """Select best Databricks node type based on EMR instance specs."""
        memory_gb = specs["memory_gb"]

        # Size classification based on memory
        if memory_gb <= 16:
            size = "small"
        elif memory_gb <= 64:
            size = "medium"
        elif memory_gb <= 128:
            size = "large"
        elif memory_gb <= 512:
            size = "xlarge"
        else:
            size = "2xlarge"

        node_map = self.DATABRICKS_NODE_MAP.get(category, self.DATABRICKS_NODE_MAP["general"])
        return node_map.get(size, "i3.xlarge")

    def _recommend_compute_type(
        self, workload_type: str, instance_count: int, spark_config: dict
    ) -> str:
        """Recommend Databricks compute type based on workload."""
        # Serverless: best for short-running, intermittent batch jobs
        if workload_type == "batch_etl" and instance_count <= 10:
            return "serverless"

        # Streaming always needs dedicated job clusters
        if workload_type == "streaming":
            return "job_cluster"

        # Large ML training needs job clusters with specific configs
        if workload_type == "ml_training":
            return "job_cluster"

        # SQL analytics → SQL Warehouse
        if workload_type == "sql_analytics":
            return "sql_warehouse"

        # Default: job cluster for isolation
        return "job_cluster"

    def _convert_spark_config(self, emr_config: dict[str, str], emr_release: str) -> dict[str, str]:
        """Convert EMR Spark configs to Databricks equivalents."""
        db_config = {}

        # Configs to remove (EMR-specific or Databricks defaults)
        skip_prefixes = [
            "spark.yarn.",
            "spark.hadoop.yarn.",
            "spark.emr.",
            "spark.hadoop.fs.s3.",  # EMRFS-specific
            "spark.decommission.",
        ]

        # Configs to transform
        transforms = {
            "spark.hadoop.fs.s3a.endpoint": None,  # Remove (not needed in Databricks)
            "spark.hadoop.fs.s3a.impl": None,
            "spark.driver.maxResultSize": "spark.driver.maxResultSize",  # Keep as-is
            "spark.sql.shuffle.partitions": "spark.sql.shuffle.partitions",
            "spark.sql.adaptive.enabled": "spark.sql.adaptive.enabled",
            "spark.default.parallelism": "spark.default.parallelism",
            "spark.executor.memory": None,  # Managed by Databricks
            "spark.executor.cores": None,   # Managed by Databricks
        }

        for key, value in emr_config.items():
            # Skip EMR-specific
            if any(key.startswith(p) for p in skip_prefixes):
                continue

            # Apply transforms
            if key in transforms:
                new_key = transforms[key]
                if new_key:
                    db_config[new_key] = value
            else:
                # Keep other configs as-is
                db_config[key] = value

        # Add Databricks-recommended defaults
        db_config.setdefault("spark.sql.adaptive.enabled", "true")
        db_config.setdefault("spark.sql.adaptive.coalescePartitions.enabled", "true")
        db_config.setdefault("spark.databricks.delta.optimizeWrite.enabled", "true")
        db_config.setdefault("spark.databricks.delta.autoCompact.enabled", "true")

        return db_config

    def _convert_bootstrap_actions(self, bootstrap_actions: list[dict]) -> list[dict]:
        """Convert EMR bootstrap actions to Databricks init scripts."""
        init_scripts = []

        for action in bootstrap_actions:
            script_path = action.get("ScriptBootstrapAction", {}).get("Path", "")
            if script_path and script_path.startswith("s3://"):
                # Convert S3 path to Databricks volume or DBFS path
                volume_path = script_path.replace("s3://", "/Volumes/external/init_scripts/")
                init_scripts.append({
                    "workspace": {"destination": volume_path}
                })

        return init_scripts

    def _dbu_rate(self, node_type: str) -> float:
        """Estimate DBU consumption rate per node per hour."""
        dbu_rates = {
            "i3.xlarge": 1.0, "i3.2xlarge": 2.0, "i3.4xlarge": 4.0, "i3.8xlarge": 8.0,
            "m5.xlarge": 0.75, "m5.2xlarge": 1.5, "m5.4xlarge": 3.0, "m5.8xlarge": 6.0,
            "r5.xlarge": 1.0, "r5.2xlarge": 2.0, "r5.4xlarge": 4.0, "r5.8xlarge": 8.0,
            "c5.xlarge": 0.5, "c5.2xlarge": 1.0, "c5.4xlarge": 2.0, "c5.9xlarge": 4.5,
        }
        return dbu_rates.get(node_type, 1.5)

    def generate_comparison_report(
        self, emr_configs: list[dict], monthly_emr_cost: float
    ) -> dict:
        """Generate cost/performance comparison between EMR and Databricks."""
        total_dbu = 0
        mappings = []

        for config in emr_configs:
            db_config = self.map_emr_cluster(**config)
            mappings.append({
                "emr": f"{config['instance_type']}x{config['instance_count']}",
                "databricks": f"{db_config.node_type_id}x{db_config.num_workers}",
                "compute_type": db_config.compute_type,
                "monthly_cost": db_config.estimated_monthly_cost
            })
            total_dbu += db_config.estimated_dbu_per_hour

        total_db_cost = sum(m["monthly_cost"] for m in mappings)

        return {
            "emr_monthly_cost": monthly_emr_cost,
            "databricks_estimated_monthly_cost": total_db_cost,
            "savings_pct": ((monthly_emr_cost - total_db_cost) / monthly_emr_cost) * 100,
            "total_dbu_per_hour": total_dbu,
            "mappings": mappings
        }
