"""
Graph visualization data preparation utility.

Loads cleaned edges and entities from dbt, prepares optimized JSON structure
for interactive network visualization in Streamlit.
"""

import duckdb
import json
import networkx as nx
from pathlib import Path
from datetime import datetime
from typing import Dict, List, Any


class GraphVisualizer:
    """Prepare graph data for interactive visualization."""
    
    def __init__(self, warehouse_path: str = "warehouse/dev.duckdb"):
        """
        Initialize with path to DuckDB warehouse.
        
        Args:
            warehouse_path: Path to dev.duckdb file
        """
        self.warehouse_path = warehouse_path
        self.conn = duckdb.connect(warehouse_path, read_only=True)
    
    def load_edges(self) -> List[Dict[str, Any]]:
        """Load clean edges from dbt model."""
        query = """
            SELECT 
                source_qid,
                target_qid,
                relationship_type,
                source_label,
                target_label
            FROM fct_edges_clean
        """
        return self.conn.execute(query).fetch_df().to_dict('records')
    
    def load_entities(self) -> Dict[str, Dict[str, Any]]:
        """Load clean entities from dbt model, keyed by QID."""
        query = """
            SELECT 
                qid,
                label,
                description,
                instance_of,
                subclass_of,
                first_seen_ingestion,
                last_updated
            FROM dim_entities_clean
        """
        df = self.conn.execute(query).fetch_df()
        entities = {}
        for _, row in df.iterrows():
            entities[row['qid']] = {
                'label': row['label'],
                'description': row['description'],
                'instance_of': row['instance_of'],
                'subclass_of': row['subclass_of'],
                'first_seen_ingestion': row['first_seen_ingestion'],
                'last_updated': row['last_updated']
            }
        return entities
    
    def build_graph(self, edges: List[Dict]) -> nx.DiGraph:
        """Build NetworkX directed graph from edges."""
        G = nx.DiGraph()
        for edge in edges:
            G.add_edge(
                edge['source_qid'],
                edge['target_qid'],
                relationship=edge['relationship_type'],
                source_label=edge['source_label'],
                target_label=edge['target_label']
            )
        return G
    
    def compute_metrics(self, G: nx.DiGraph) -> Dict[str, Dict[str, float]]:
        """
        Compute graph metrics: in-degree, out-degree, betweenness centrality.
        
        Returns:
            Dict mapping node QID to metrics dict
        """
        in_degree = dict(G.in_degree())
        out_degree = dict(G.out_degree())
        betweenness = nx.betweenness_centrality(G)
        
        metrics = {}
        for node in G.nodes():
            metrics[node] = {
                'in_degree': in_degree.get(node, 0),
                'out_degree': out_degree.get(node, 0),
                'betweenness_centrality': betweenness.get(node, 0.0)
            }
        return metrics
    
    def prepare_visualization_data(self) -> Dict[str, Any]:
        """
        Prepare complete visualization data structure.
        
        Returns:
            Dict with nodes, edges, and metadata for interactive visualization
        """
        edges = self.load_edges()
        entities = self.load_entities()
        
        G = self.build_graph(edges)
        metrics = self.compute_metrics(G)
        
        # Prepare nodes with all metadata
        nodes = []
        for qid, entity in entities.items():
            if qid in G.nodes():
                nodes.append({
                    'id': qid,
                    'label': entity['label'],
                    'description': entity['description'],
                    'first_seen': entity['first_seen_ingestion'],
                    'instance_of': entity['instance_of'],
                    'subclass_of': entity['subclass_of'],
                    **metrics[qid]  # Add computed metrics
                })
        
        # Prepare edges
        edges_output = []
        for edge in edges:
            edges_output.append({
                'source': edge['source_qid'],
                'target': edge['target_qid'],
                'relationship': edge['relationship_type'],
                'source_label': edge['source_label'],
                'target_label': edge['target_label']
            })
        
        return {
            'nodes': nodes,
            'edges': edges_output,
            'metadata': {
                'generated_at': datetime.now().isoformat(),
                'total_nodes': len(nodes),
                'total_edges': len(edges_output),
                'density': nx.density(G) if G.number_of_nodes() > 0 else 0.0
            }
        }
    
    def export_json(self, output_path: str = "data/graph_visualization.json"):
        """Export visualization data to JSON file."""
        data = self.prepare_visualization_data()
        
        output_file = Path(output_path)
        output_file.parent.mkdir(parents=True, exist_ok=True)
        
        with open(output_file, 'w') as f:
            json.dump(data, f, indent=2, default=str)
        
        print(f"Visualization data exported → {output_file}")
        print(f"  Nodes: {data['metadata']['total_nodes']}")
        print(f"  Edges: {data['metadata']['total_edges']}")
        print(f"  Density: {data['metadata']['density']:.4f}")
        
        return data
    
    def close(self):
        """Close database connection."""
        self.conn.close()


def main():
    """Generate visualization data."""
    import sys
    from pathlib import Path
    
    # Try to find warehouse from project root
    project_root = Path(__file__).resolve().parents[3]
    warehouse_path = str(project_root / "warehouse" / "dev.duckdb")
    
    viz = GraphVisualizer(warehouse_path)
    try:
        viz.export_json(str(project_root / "data" / "graph_visualization.json"))
    finally:
        viz.close()


if __name__ == "__main__":
    main()
