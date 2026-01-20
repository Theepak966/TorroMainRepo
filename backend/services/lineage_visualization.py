"""
Automatic lineage diagram generation service.
Supports multiple output formats: ReactFlow, Mermaid, Graphviz.
"""

from typing import Dict, List, Optional
import sys
import os
import logging

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from services.lineage_traversal import LineageTraversalService
from database import SessionLocal
from models_lineage.models_lineage import ColumnLineage

logger = logging.getLogger(__name__)


class LineageDiagramGenerator:
    """Generate lineage diagrams automatically from lineage data"""
    
    def __init__(self):
        self.traversal_service = LineageTraversalService()
    
    def generate_lineage_diagram(
        self,
        root_dataset_urn: str,
        direction: str = 'both',  # 'upstream', 'downstream', 'both'
        depth: int = 3,
        include_column_lineage: bool = False,
        format: str = 'reactflow'  # 'reactflow', 'graphviz', 'mermaid', 'cytoscape'
    ) -> Dict:
        """
        Generate automatic lineage diagram.
        Returns diagram data in specified format.
        """
        # Get lineage data
        upstream_data = {}
        downstream_data = {}
        
        if direction in ['upstream', 'both']:
            upstream_data = self.traversal_service.get_upstream_lineage(
                root_dataset_urn, depth=depth
            )
        
        if direction in ['downstream', 'both']:
            downstream_data = self.traversal_service.get_downstream_lineage(
                root_dataset_urn, depth=depth
            )
        
        # Generate diagram based on format
        if format == 'reactflow':
            return self._generate_reactflow_diagram(
                root_dataset_urn, upstream_data, downstream_data, include_column_lineage
            )
        elif format == 'mermaid':
            return self._generate_mermaid_diagram(
                root_dataset_urn, upstream_data, downstream_data
            )
        elif format == 'graphviz':
            return self._generate_graphviz_diagram(
                root_dataset_urn, upstream_data, downstream_data
            )
        else:
            raise ValueError(f"Unsupported format: {format}")
    
    def _generate_reactflow_diagram(
        self,
        root_urn: str,
        upstream_data: Dict,
        downstream_data: Dict,
        include_column_lineage: bool
    ) -> Dict:
        """Generate ReactFlow-compatible diagram"""
        nodes = []
        edges = []
        node_positions = {}
        
        # Add root node
        root_node = {
            'id': root_urn,
            'type': 'custom',
            'data': {
                'label': self._extract_name(root_urn),
                'urn': root_urn,
                'is_root': True
            },
            'position': {'x': 400, 'y': 300}
        }
        nodes.append(root_node)
        node_positions[root_urn] = (400, 300)
        
        # Add upstream nodes and edges
        self._add_nodes_and_edges(
            nodes, edges, upstream_data, root_urn, 
            direction='upstream', node_positions=node_positions
        )
        
        # Add downstream nodes and edges
        self._add_nodes_and_edges(
            nodes, edges, downstream_data, root_urn,
            direction='downstream', node_positions=node_positions
        )
        
        # Add column lineage if requested
        if include_column_lineage:
            self._enrich_with_column_lineage(edges)
        
        return {
            'nodes': nodes,
            'edges': edges,
            'metadata': {
                'root_dataset': root_urn,
                'total_nodes': len(nodes),
                'total_edges': len(edges),
                'upstream_depth': len(upstream_data.get('edges', [])),
                'downstream_depth': len(downstream_data.get('edges', []))
            }
        }
    
    def _add_nodes_and_edges(
        self,
        nodes: List[Dict],
        edges: List[Dict],
        lineage_data: Dict,
        root_urn: str,
        direction: str,
        node_positions: Dict
    ):
        """Add nodes and edges from lineage data"""
        datasets = lineage_data.get('datasets', [])
        processes = lineage_data.get('processes', [])
        edges_data = lineage_data.get('edges', [])
        
        # Add dataset nodes
        for dataset in datasets:
            if dataset['urn'] not in node_positions:
                position = self._calculate_position(
                    dataset['urn'], direction, len(nodes), node_positions
                )
                nodes.append({
                    'id': dataset['urn'],
                    'type': 'custom',
                    'data': {
                        'label': dataset['name'],
                        'urn': dataset['urn'],
                        'type': dataset['type'],
                        'catalog': dataset.get('catalog')
                    },
                    'position': position
                })
                node_positions[dataset['urn']] = (position['x'], position['y'])
        
        # Add process nodes
        for process in processes:
            process_id = f"process_{process['urn']}"
            if process_id not in node_positions:
                position = self._calculate_position(
                    process_id, direction, len(nodes), node_positions
                )
                nodes.append({
                    'id': process_id,
                    'type': 'custom',
                    'data': {
                        'label': process['name'],
                        'urn': process['urn'],
                        'type': 'process',
                        'source_system': process.get('source_system')
                    },
                    'position': position
                })
                node_positions[process_id] = (position['x'], position['y'])
        
        # Add edges
        for edge_data in edges_data:
            if direction == 'downstream':
                source_id = edge_data['source_urn']
                target_id = f"process_{edge_data['process_urn']}"
            else:
                source_id = f"process_{edge_data['process_urn']}"
                target_id = edge_data['target_urn']
            
            edges.append({
                'id': f"edge_{edge_data['id']}",
                'source': source_id,
                'target': target_id,
                'type': 'smoothstep',
                'data': {
                    'relationship_type': edge_data.get('relationship_type'),
                    'depth': edge_data.get('depth', 1)
                }
            })
    
    def _calculate_position(self, node_id: str, direction: str, index: int, existing_positions: Dict) -> Dict:
        """Calculate node position for layout"""
        base_x = 400
        base_y = 300
        
        if direction == 'upstream':
            x = base_x - 300 - (index % 5) * 200
            y = base_y - 200 + (index // 5) * 150
        else:
            x = base_x + 300 + (index % 5) * 200
            y = base_y - 200 + (index // 5) * 150
        
        return {'x': x, 'y': y}
    
    def _enrich_with_column_lineage(self, edges: List[Dict]):
        """Enrich edges with column-level lineage information"""
        db = SessionLocal()
        try:
            for edge in edges:
                edge_id = edge['id'].replace('edge_', '')
                try:
                    column_lineage = db.query(ColumnLineage).filter(
                        ColumnLineage.edge_id == edge_id
                    ).all()
                    
                    if column_lineage:
                        edge['data']['column_lineage'] = [
                            {
                                'source_column': cl.source_column,
                                'target_column': cl.target_column,
                                'transformation_type': cl.transformation_type
                            }
                            for cl in column_lineage
                        ]
                except Exception as e:
                    logger.debug(f"Could not load column lineage for edge {edge_id}: {e}")
        finally:
            db.close()
    
    def _generate_mermaid_diagram(self, root_urn: str, upstream_data: Dict, downstream_data: Dict) -> Dict:
        """Generate Mermaid diagram syntax"""
        mermaid_lines = ["graph TD"]
        
        # Add nodes and edges
        for edge in upstream_data.get('edges', []):
            source = self._sanitize_name(self._extract_name(edge['source_urn']))
            target = self._sanitize_name(self._extract_name(edge['target_urn']))
            mermaid_lines.append(f"    {source} --> {target}")
        
        for edge in downstream_data.get('edges', []):
            source = self._sanitize_name(self._extract_name(edge['source_urn']))
            target = self._sanitize_name(self._extract_name(edge['target_urn']))
            mermaid_lines.append(f"    {source} --> {target}")
        
        return {
            'format': 'mermaid',
            'diagram': '\n'.join(mermaid_lines)
        }
    
    def _generate_graphviz_diagram(self, root_urn: str, upstream_data: Dict, downstream_data: Dict) -> Dict:
        """Generate Graphviz DOT format"""
        dot_lines = ["digraph Lineage {"]
        dot_lines.append("    rankdir=LR;")
        dot_lines.append("    node [shape=box];")
        
        # Add edges
        for edge in upstream_data.get('edges', []):
            source = self._sanitize_name(self._extract_name(edge['source_urn']))
            target = self._sanitize_name(self._extract_name(edge['target_urn']))
            dot_lines.append(f'    "{source}" -> "{target}";')
        
        for edge in downstream_data.get('edges', []):
            source = self._sanitize_name(self._extract_name(edge['source_urn']))
            target = self._sanitize_name(self._extract_name(edge['target_urn']))
            dot_lines.append(f'    "{source}" -> "{target}";')
        
        dot_lines.append("}")
        
        return {
            'format': 'graphviz',
            'diagram': '\n'.join(dot_lines)
        }
    
    def _extract_name(self, urn: str) -> str:
        """Extract name from URN"""
        if ':' in urn:
            return urn.split(':')[-1].split('.')[-1]
        return urn
    
    def _sanitize_name(self, name: str) -> str:
        """Sanitize name for diagram formats"""
        return name.replace(' ', '_').replace('-', '_').replace('.', '_')










