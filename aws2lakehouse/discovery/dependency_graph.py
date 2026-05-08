"""
Dependency Graph — Automated dependency extraction and visualization.

Extracts pipeline dependencies from:
- Airflow DAG definitions (>> operator)
- Step Function state machine definitions
- Table lineage (reads/writes)
- Pipeline YAML specs (depends_on field)

Outputs:
- Adjacency list
- Topological sort (migration order)
- Critical path identification
- Cycle detection
"""

from collections import defaultdict, deque
from dataclasses import dataclass, field


@dataclass
class PipelineNode:
    """A node in the dependency graph."""
    name: str
    domain: str = ""
    pipeline_type: str = ""
    complexity: str = "medium"
    wave: int = 0
    tables_read: list[str] = field(default_factory=list)
    tables_written: list[str] = field(default_factory=list)


class DependencyGraph:
    """
    Builds and analyzes pipeline dependency graphs.

    Usage:
        graph = DependencyGraph()
        graph.add_edge("pipeline_a", "pipeline_b")  # B depends on A
        graph.add_from_table_lineage(pipelines)      # Auto-detect from tables

        order = graph.topological_sort()             # Migration order
        critical = graph.critical_path()             # Longest dependency chain
        cycles = graph.detect_cycles()               # Circular dependencies
    """

    def __init__(self):
        self.nodes: dict[str, PipelineNode] = {}
        self.edges: dict[str, set[str]] = defaultdict(set)  # parent -> children
        self.reverse_edges: dict[str, set[str]] = defaultdict(set)  # child -> parents

    def add_node(self, node: PipelineNode):
        """Add a pipeline node."""
        self.nodes[node.name] = node

    def add_edge(self, parent: str, child: str):
        """Add dependency: child depends on parent (parent must complete first)."""
        self.edges[parent].add(child)
        self.reverse_edges[child].add(parent)

    def add_from_table_lineage(self, pipelines: list[PipelineNode]):
        """
        Auto-detect dependencies from table read/write patterns.
        If pipeline A writes table X and pipeline B reads table X,
        then B depends on A.
        """
        # Build write map: table -> pipeline that writes it
        write_map: dict[str, str] = {}
        for p in pipelines:
            self.add_node(p)
            for table in p.tables_written:
                write_map[table] = p.name

        # Detect dependencies
        for p in pipelines:
            for table in p.tables_read:
                if table in write_map and write_map[table] != p.name:
                    self.add_edge(write_map[table], p.name)

    def add_from_yaml_specs(self, specs: list[dict]):
        """Add dependencies from pipeline YAML specs (depends_on field)."""
        for spec in specs:
            name = spec.get("name", "")
            depends_on = spec.get("depends_on", [])
            for dep in depends_on:
                self.add_edge(dep, name)

    def topological_sort(self) -> list[str]:
        """
        Return pipelines in dependency order (safe migration sequence).
        Raises ValueError if cycles detected.
        """
        in_degree = defaultdict(int)
        for parent, children in self.edges.items():
            if parent not in in_degree:
                in_degree[parent] = 0
            for child in children:
                in_degree[child] = in_degree.get(child, 0) + 1

        # Add nodes with no edges
        for node in self.nodes:
            if node not in in_degree:
                in_degree[node] = 0

        queue = deque([n for n, d in in_degree.items() if d == 0])
        result = []

        while queue:
            node = queue.popleft()
            result.append(node)
            for child in self.edges.get(node, []):
                in_degree[child] -= 1
                if in_degree[child] == 0:
                    queue.append(child)

        if len(result) < len(in_degree):
            raise ValueError(f"Cycle detected! Cannot topologically sort. "
                           f"Sorted {len(result)}/{len(in_degree)} nodes.")

        return result

    def detect_cycles(self) -> list[list[str]]:
        """Detect circular dependencies (these block migration)."""
        cycles = []
        visited = set()
        rec_stack = set()
        path = []

        def dfs(node):
            visited.add(node)
            rec_stack.add(node)
            path.append(node)

            for child in self.edges.get(node, []):
                if child not in visited:
                    dfs(child)
                elif child in rec_stack:
                    # Found cycle
                    cycle_start = path.index(child)
                    cycles.append(path[cycle_start:] + [child])

            path.pop()
            rec_stack.discard(node)

        for node in list(self.nodes.keys()) + list(self.edges.keys()):
            if node not in visited:
                dfs(node)

        return cycles

    def critical_path(self) -> tuple[list[str], int]:
        """
        Find the longest dependency chain (critical path).
        This determines minimum migration duration.
        """
        try:
            order = self.topological_sort()
        except ValueError:
            return ([], -1)

        # Longest path using DP
        dist = {n: 0 for n in order}
        predecessor = {n: None for n in order}

        for node in order:
            for child in self.edges.get(node, []):
                if dist[child] < dist[node] + 1:
                    dist[child] = dist[node] + 1
                    predecessor[child] = node

        # Find the end of the longest path
        end_node = max(dist, key=dist.get)
        length = dist[end_node]

        # Reconstruct path
        path = []
        current = end_node
        while current is not None:
            path.append(current)
            current = predecessor[current]
        path.reverse()

        return (path, length)

    def migration_waves(self, max_per_wave: int = 50) -> dict[int, list[str]]:
        """
        Assign pipelines to waves respecting dependencies.
        A pipeline can only be in a wave after all its dependencies.
        """
        try:
            order = self.topological_sort()
        except ValueError:
            return {}

        # Calculate depth (wave number) for each node
        depth = {n: 0 for n in order}
        for node in order:
            for child in self.edges.get(node, []):
                depth[child] = max(depth[child], depth[node] + 1)

        # Group by depth
        waves = defaultdict(list)
        for node, wave in depth.items():
            waves[wave].append(node)

        # Split large waves
        final_waves = {}
        wave_num = 1
        for w in sorted(waves.keys()):
            nodes = waves[w]
            for i in range(0, len(nodes), max_per_wave):
                final_waves[wave_num] = nodes[i:i + max_per_wave]
                wave_num += 1

        return final_waves

    def to_mermaid(self) -> str:
        """Export as Mermaid diagram (for documentation)."""
        lines = ["graph LR"]
        for parent, children in sorted(self.edges.items()):
            for child in sorted(children):
                lines.append(f"    {parent} --> {child}")
        return "\n".join(lines)

    def summary(self) -> str:
        """Human-readable summary of the dependency graph."""
        total_nodes = len(set(list(self.nodes.keys()) + list(self.edges.keys()) +
                              [c for children in self.edges.values() for c in children]))
        total_edges = sum(len(children) for children in self.edges.values())
        roots = [n for n in self.nodes if not self.reverse_edges.get(n)]
        leaves = [n for n in self.nodes if not self.edges.get(n)]

        path, length = self.critical_path()
        cycles = self.detect_cycles()

        return f"""Dependency Graph Summary:
  Nodes: {total_nodes}
  Edges: {total_edges}
  Root pipelines (no upstream): {len(roots)}
  Leaf pipelines (no downstream): {len(leaves)}
  Critical path length: {length} hops
  Critical path: {' -> '.join(path[:10])}{'...' if len(path) > 10 else ''}
  Cycles detected: {len(cycles)}
  {'  ⚠️  CYCLES MUST BE RESOLVED BEFORE MIGRATION' if cycles else '  ✅ No cycles (clean DAG)'}"""
