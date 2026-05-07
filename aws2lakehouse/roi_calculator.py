"""
ROI Calculator — Quantifies migration savings and time-to-value.

Compares AWS (EMR/Glue/Step Functions) costs vs Databricks Lakehouse.
"""

from typing import Dict, List
from dataclasses import dataclass, field


@dataclass
class AWSCurrentState:
    """Current AWS infrastructure costs."""
    # EMR
    emr_clusters: int = 0
    emr_monthly_cost: float = 0.0
    emr_avg_utilization: float = 0.4  # Most EMR clusters are 30-50% utilized
    
    # Glue
    glue_jobs: int = 0
    glue_monthly_dpu_hours: float = 0.0
    glue_monthly_cost: float = 0.0
    
    # Step Functions
    step_function_executions: int = 0
    step_function_monthly_cost: float = 0.0
    
    # S3 storage
    s3_storage_tb: float = 0.0
    s3_monthly_cost: float = 0.0
    
    # Other (Kinesis, Lambda, CloudWatch, etc.)
    other_monthly_cost: float = 0.0
    
    # People cost
    engineers_maintaining: int = 0
    avg_engineer_monthly_cost: float = 15000.0  # Fully loaded
    
    @property
    def total_monthly_infrastructure(self) -> float:
        return (self.emr_monthly_cost + self.glue_monthly_cost + 
                self.step_function_monthly_cost + self.s3_monthly_cost + 
                self.other_monthly_cost)
    
    @property
    def total_monthly_people(self) -> float:
        return self.engineers_maintaining * self.avg_engineer_monthly_cost
    
    @property
    def total_monthly_cost(self) -> float:
        return self.total_monthly_infrastructure + self.total_monthly_people


@dataclass
class DatabricksProjectedState:
    """Projected Databricks costs after migration."""
    # Compute
    jobs_dbu_monthly: float = 0.0
    sql_dbu_monthly: float = 0.0
    serverless_monthly: float = 0.0
    
    # Storage (typically cheaper due to Delta optimization)
    storage_tb: float = 0.0
    storage_monthly_cost: float = 0.0
    
    # Platform
    unity_catalog_cost: float = 0.0
    
    # People (typically fewer needed due to automation)
    engineers_needed: int = 0
    avg_engineer_monthly_cost: float = 15000.0
    
    @property
    def total_monthly_infrastructure(self) -> float:
        return (self.jobs_dbu_monthly + self.sql_dbu_monthly + 
                self.serverless_monthly + self.storage_monthly_cost +
                self.unity_catalog_cost)
    
    @property
    def total_monthly_cost(self) -> float:
        return self.total_monthly_infrastructure + (self.engineers_needed * self.avg_engineer_monthly_cost)


class ROICalculator:
    """Calculate migration ROI and generate executive summary."""
    
    def __init__(self, current: AWSCurrentState, projected: DatabricksProjectedState,
                 migration_cost: float = 0.0, migration_months: int = 6):
        self.current = current
        self.projected = projected
        self.migration_cost = migration_cost
        self.migration_months = migration_months
    
    def calculate(self) -> Dict:
        """Calculate comprehensive ROI metrics."""
        monthly_savings = self.current.total_monthly_cost - self.projected.total_monthly_cost
        annual_savings = monthly_savings * 12
        
        # Infrastructure savings
        infra_savings = self.current.total_monthly_infrastructure - self.projected.total_monthly_infrastructure
        
        # Photon acceleration (typical 2-3x performance = cost reduction)
        photon_savings = self.current.emr_monthly_cost * 0.4  # ~40% compute reduction with Photon
        
        # Delta Lake storage optimization (typically 30-50% less storage)
        storage_savings = self.current.s3_monthly_cost * 0.35
        
        # People efficiency (automation reduces ops burden)
        people_savings = self.current.total_monthly_people - (self.projected.engineers_needed * self.projected.avg_engineer_monthly_cost)
        
        # Payback period
        if monthly_savings > 0:
            payback_months = self.migration_cost / monthly_savings
        else:
            payback_months = float('inf')
        
        # 3-year TCO
        tco_current_3yr = self.current.total_monthly_cost * 36
        tco_databricks_3yr = (self.projected.total_monthly_cost * 36) + self.migration_cost
        
        return {
            "summary": {
                "monthly_savings": monthly_savings,
                "annual_savings": annual_savings,
                "savings_pct": (monthly_savings / self.current.total_monthly_cost * 100) if self.current.total_monthly_cost > 0 else 0,
                "payback_months": round(payback_months, 1),
                "migration_cost": self.migration_cost,
            },
            "breakdown": {
                "infrastructure_savings_monthly": infra_savings,
                "photon_acceleration_savings": photon_savings,
                "storage_optimization_savings": storage_savings,
                "people_efficiency_savings": people_savings,
            },
            "tco_comparison": {
                "aws_3yr_tco": tco_current_3yr,
                "databricks_3yr_tco": tco_databricks_3yr,
                "net_savings_3yr": tco_current_3yr - tco_databricks_3yr,
            },
            "non_financial_benefits": [
                "Unified platform (no ETL tool sprawl)",
                "Built-in governance (Unity Catalog)",
                "Faster time-to-insight (Photon + serverless)",
                "Reduced operational complexity",
                "Auto-scaling eliminates over-provisioning",
                "Delta Lake: ACID, time travel, schema evolution",
                "Single security model (MNPI, RBAC, column masking)",
            ],
            "risk_factors": [
                "Migration execution risk (mitigated by accelerator)",
                "Learning curve for teams (mitigated by patterns/templates)",
                "Vendor lock-in (mitigated by Delta Lake open format)",
            ],
        }
    
    def generate_executive_summary(self) -> str:
        """Generate text-based executive summary."""
        roi = self.calculate()
        s = roi["summary"]
        b = roi["breakdown"]
        t = roi["tco_comparison"]
        
        return f"""
═══════════════════════════════════════════════════════════════
MIGRATION ROI EXECUTIVE SUMMARY
═══════════════════════════════════════════════════════════════

INVESTMENT:
  Migration cost:           ${self.migration_cost:,.0f}
  Timeline:                 {self.migration_months} months
  Pipelines:                {self.current.emr_clusters + self.current.glue_jobs}

MONTHLY SAVINGS:            ${s['monthly_savings']:,.0f}/mo ({s['savings_pct']:.0f}%)
ANNUAL SAVINGS:             ${s['annual_savings']:,.0f}/yr
PAYBACK PERIOD:             {s['payback_months']} months

SAVINGS BREAKDOWN (Monthly):
  Infrastructure:           ${b['infrastructure_savings_monthly']:,.0f}
  Photon acceleration:      ${b['photon_acceleration_savings']:,.0f}
  Storage optimization:     ${b['storage_optimization_savings']:,.0f}
  People efficiency:        ${b['people_efficiency_savings']:,.0f}

3-YEAR TCO COMPARISON:
  AWS (current path):       ${t['aws_3yr_tco']:,.0f}
  Databricks (projected):   ${t['databricks_3yr_tco']:,.0f}
  NET SAVINGS:              ${t['net_savings_3yr']:,.0f}

═══════════════════════════════════════════════════════════════
"""
