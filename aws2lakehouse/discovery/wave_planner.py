"""
Wave Planner — Generates migration waves based on complexity, risk, and dependencies.

Strategies:
- risk_first: Migrate low-risk pipelines first to build confidence
- impact_first: Migrate high-impact pipelines first for maximum ROI  
- domain_based: Group by business domain for team alignment
- dependency_based: Respect data dependencies between pipelines
"""

from typing import Dict, List, Optional
from dataclasses import dataclass, field
import logging

logger = logging.getLogger(__name__)


@dataclass
class MigrationWave:
    """A migration wave containing grouped pipelines."""
    wave_number: int
    name: str
    description: str
    pipelines: List[str] = field(default_factory=list)  # pipeline_ids
    estimated_duration_weeks: float = 2.0
    estimated_effort_hours: float = 0.0
    dependencies: List[int] = field(default_factory=list)  # wave numbers this depends on
    validation_criteria: List[str] = field(default_factory=list)
    rollback_plan: str = ""


class WavePlanner:
    """
    Generates wave-based migration roadmap.
    
    Usage:
        from aws2lakehouse.discovery import PipelineInventory, WavePlanner
        
        inventory = PipelineInventory()
        inventory.scan_emr_clusters()
        
        planner = WavePlanner(inventory)
        waves = planner.generate_waves(strategy="risk_first", max_per_wave=8)
        planner.export_roadmap("migration_roadmap.json")
    """
    
    def __init__(self, inventory):
        self.inventory = inventory
        self.waves: List[MigrationWave] = []
    
    def generate_waves(
        self, 
        strategy: str = "risk_first",
        max_per_wave: int = 8,
        wave_duration_weeks: float = 2.0
    ) -> List[MigrationWave]:
        """
        Generate migration waves.
        
        Args:
            strategy: "risk_first", "impact_first", "domain_based", "dependency_based"
            max_per_wave: Maximum pipelines per wave
            wave_duration_weeks: Target duration per wave
        """
        pipelines = self.inventory.pipelines
        
        if strategy == "risk_first":
            sorted_pipelines = sorted(pipelines, key=lambda p: p.risk_score)
        elif strategy == "impact_first":
            impact_order = {"critical": 0, "high": 1, "medium": 2, "low": 3}
            sorted_pipelines = sorted(
                pipelines, 
                key=lambda p: impact_order.get(p.business_impact.value, 3)
            )
        elif strategy == "domain_based":
            sorted_pipelines = sorted(pipelines, key=lambda p: p.domain)
        else:
            sorted_pipelines = pipelines
        
        # Always start with Wave 0: Pilot
        self.waves = [MigrationWave(
            wave_number=0,
            name="Pilot Wave",
            description="3 representative pipelines (simple, medium, complex) to validate patterns",
            estimated_duration_weeks=3.0,
            validation_criteria=[
                "Data accuracy validated against source",
                "Performance within 10% of EMR baseline",
                "Monitoring and alerting operational",
                "Rollback procedure tested"
            ]
        )]
        
        # Select pilot pipelines (one of each complexity)
        pilot_ids = set()
        for complexity in ["simple", "medium", "complex"]:
            for p in sorted_pipelines:
                if p.complexity.value == complexity and p.pipeline_id not in pilot_ids:
                    self.waves[0].pipelines.append(p.pipeline_id)
                    pilot_ids.add(p.pipeline_id)
                    break
        
        # Generate remaining waves
        remaining = [p for p in sorted_pipelines if p.pipeline_id not in pilot_ids]
        wave_num = 1
        
        for i in range(0, len(remaining), max_per_wave):
            batch = remaining[i:i + max_per_wave]
            wave = MigrationWave(
                wave_number=wave_num,
                name=f"Wave {wave_num}",
                description=f"Migration batch {wave_num} ({len(batch)} pipelines)",
                pipelines=[p.pipeline_id for p in batch],
                estimated_duration_weeks=wave_duration_weeks,
                estimated_effort_hours=sum(
                    {"simple": 4, "medium": 16, "complex": 40, "critical": 80}.get(
                        p.complexity.value, 16
                    ) for p in batch
                ),
                dependencies=[0] if wave_num == 1 else [wave_num - 1],
                validation_criteria=[
                    "All pipelines producing correct output",
                    "SLAs met for 3 consecutive days",
                    "No data quality regressions"
                ]
            )
            self.waves.append(wave)
            wave_num += 1
        
        logger.info(f"Generated {len(self.waves)} waves for {len(pipelines)} pipelines")
        return self.waves
    
    def export_roadmap(self, path: str):
        """Export roadmap to JSON."""
        import json
        
        roadmap = {
            "total_waves": len(self.waves),
            "total_pipelines": sum(len(w.pipelines) for w in self.waves),
            "estimated_total_weeks": sum(w.estimated_duration_weeks for w in self.waves),
            "waves": [
                {
                    "wave_number": w.wave_number,
                    "name": w.name,
                    "description": w.description,
                    "pipeline_count": len(w.pipelines),
                    "duration_weeks": w.estimated_duration_weeks,
                    "effort_hours": w.estimated_effort_hours,
                    "dependencies": w.dependencies,
                    "validation_criteria": w.validation_criteria
                }
                for w in self.waves
            ]
        }
        
        with open(path, "w") as f:
            json.dump(roadmap, f, indent=2)
