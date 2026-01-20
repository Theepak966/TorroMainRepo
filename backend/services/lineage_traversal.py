"""
Optimized graph traversal service with depth limits and caching.
No runtime inference - all edges are precomputed at ingestion time.
"""

from typing import List, Dict, Optional, Set
from collections import deque
from datetime import datetime
import sys
import os

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from database import SessionLocal
from models_lineage.models_lineage import Dataset, Process, LineageEdge
import logging

logger = logging.getLogger(__name__)


class LineageTraversalService:
    """Optimized graph traversal with depth limits and caching"""
    
    def __init__(self, max_depth: int = 3, cache_enabled: bool = True):
        self.max_depth = min(max_depth, 5)  # Hard limit
        self.cache_enabled = cache_enabled
        self._cache = {}  # In-memory cache for hot paths
    
    def get_upstream_lineage(
        self, 
        dataset_urn: str, 
        depth: int = 3,
        as_of: Optional[datetime] = None
    ) -> Dict:
        """
        Get upstream lineage (sources) for a dataset.
        Returns: {datasets: [...], processes: [...], edges: [...]}
        """
        depth = min(depth, self.max_depth)
        cache_key = f"upstream:{dataset_urn}:{depth}:{as_of}"
        
        if self.cache_enabled and cache_key in self._cache:
            return self._cache[cache_key]
        
        db = SessionLocal()
        try:
            visited_datasets = set()
            visited_processes = set()
            edges = []
            
            # BFS traversal with depth limit
            queue = deque([(dataset_urn, 0)])
            visited_datasets.add(dataset_urn)
            
            while queue:
                current_urn, current_depth = queue.popleft()
                
                if current_depth >= depth:
                    continue
                
                # Find edges where current dataset is target
                query = db.query(LineageEdge).filter(
                    LineageEdge.target_urn == current_urn
                )
                
                if as_of:
                    query = query.filter(
                        LineageEdge.valid_from <= as_of,
                        (LineageEdge.valid_to.is_(None) | (LineageEdge.valid_to >= as_of))
                    )
                else:
                    # Only active edges (no valid_to or valid_to in future)
                    query = query.filter(
                        (LineageEdge.valid_to.is_(None) | (LineageEdge.valid_to >= datetime.utcnow()))
                    )
                
                upstream_edges = query.all()
                
                for edge in upstream_edges:
                    edges.append({
                        'id': edge.id,
                        'source_urn': edge.source_urn,
                        'process_urn': edge.process_urn,
                        'target_urn': edge.target_urn,
                        'relationship_type': edge.relationship_type,
                        'depth': current_depth + 1
                    })
                    
                    # Add source dataset to queue if not visited
                    if edge.source_urn not in visited_datasets:
                        visited_datasets.add(edge.source_urn)
                        queue.append((edge.source_urn, current_depth + 1))
                    
                    # Track process
                    if edge.process_urn not in visited_processes:
                        visited_processes.add(edge.process_urn)
            
            # Fetch dataset and process details
            datasets = db.query(Dataset).filter(Dataset.urn.in_(visited_datasets)).all()
            processes = db.query(Process).filter(Process.urn.in_(visited_processes)).all()
            
            result = {
                'datasets': [self._dataset_to_dict(d) for d in datasets],
                'processes': [self._process_to_dict(p) for p in processes],
                'edges': edges,
                'depth': depth,
                'total_datasets': len(datasets),
                'total_processes': len(processes)
            }
            
            if self.cache_enabled:
                self._cache[cache_key] = result
            
            return result
            
        finally:
            db.close()
    
    def get_downstream_lineage(
        self, 
        dataset_urn: str, 
        depth: int = 3,
        as_of: Optional[datetime] = None
    ) -> Dict:
        """Get downstream lineage (dependencies) for a dataset"""
        depth = min(depth, self.max_depth)
        cache_key = f"downstream:{dataset_urn}:{depth}:{as_of}"
        
        if self.cache_enabled and cache_key in self._cache:
            return self._cache[cache_key]
        
        db = SessionLocal()
        try:
            visited_datasets = set()
            visited_processes = set()
            edges = []
            
            queue = deque([(dataset_urn, 0)])
            visited_datasets.add(dataset_urn)
            
            while queue:
                current_urn, current_depth = queue.popleft()
                
                if current_depth >= depth:
                    continue
                
                # Find edges where current dataset is source
                query = db.query(LineageEdge).filter(
                    LineageEdge.source_urn == current_urn
                )
                
                if as_of:
                    query = query.filter(
                        LineageEdge.valid_from <= as_of,
                        (LineageEdge.valid_to.is_(None) | (LineageEdge.valid_to >= as_of))
                    )
                else:
                    # Only active edges
                    query = query.filter(
                        (LineageEdge.valid_to.is_(None) | (LineageEdge.valid_to >= datetime.utcnow()))
                    )
                
                downstream_edges = query.all()
                
                for edge in downstream_edges:
                    edges.append({
                        'id': edge.id,
                        'source_urn': edge.source_urn,
                        'process_urn': edge.process_urn,
                        'target_urn': edge.target_urn,
                        'relationship_type': edge.relationship_type,
                        'depth': current_depth + 1
                    })
                    
                    if edge.target_urn not in visited_datasets:
                        visited_datasets.add(edge.target_urn)
                        queue.append((edge.target_urn, current_depth + 1))
                    
                    if edge.process_urn not in visited_processes:
                        visited_processes.add(edge.process_urn)
            
            datasets = db.query(Dataset).filter(Dataset.urn.in_(visited_datasets)).all()
            processes = db.query(Process).filter(Process.urn.in_(visited_processes)).all()
            
            result = {
                'datasets': [self._dataset_to_dict(d) for d in datasets],
                'processes': [self._process_to_dict(p) for p in processes],
                'edges': edges,
                'depth': depth,
                'total_datasets': len(datasets),
                'total_processes': len(processes)
            }
            
            if self.cache_enabled:
                self._cache[cache_key] = result
            
            return result
            
        finally:
            db.close()
    
    def _dataset_to_dict(self, dataset: Dataset) -> Dict:
        return {
            'urn': dataset.urn,
            'name': dataset.name,
            'type': dataset.type,
            'catalog': dataset.catalog,
            'schema_name': dataset.schema_name,
            'storage_type': dataset.storage_type,
            'table_lineage_enabled': dataset.table_lineage_enabled
        }
    
    def _process_to_dict(self, process: Process) -> Dict:
        return {
            'urn': process.urn,
            'name': process.name,
            'type': process.type,
            'source_system': process.source_system,
            'job_id': process.job_id,
            'job_name': process.job_name
        }










