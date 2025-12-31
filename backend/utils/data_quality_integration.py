import logging
from typing import Dict, List, Optional
from datetime import datetime

logger = logging.getLogger(__name__)


def calculate_asset_quality_score(asset: Dict) -> Dict:
    quality_metrics = {
        'completeness': 1.0,
        'uniqueness': 1.0,
        'validity': 1.0,
        'consistency': 1.0,
        'timeliness': 1.0
    }
    
    quality_issues = []
    columns = asset.get('columns', [])
    
    if not columns:
        quality_metrics['completeness'] = 0.0
        quality_issues.append('No columns found')
        return {
            'quality_score': 0.0,
            'quality_metrics': quality_metrics,
            'quality_issues': quality_issues
        }
    

    total_columns = len(columns)
    nullable_columns = sum(1 for col in columns if col.get('nullable', False))
    completeness = 1.0 - (nullable_columns / total_columns) if total_columns > 0 else 0.0
    quality_metrics['completeness'] = max(0.0, completeness)
    
    if nullable_columns > total_columns * 0.5:
        quality_issues.append(f'High number of nullable columns ({nullable_columns}/{total_columns})')
    

    unique_columns = sum(1 for col in columns if col.get('unique', False) or col.get('primary_key', False))
    uniqueness = unique_columns / total_columns if total_columns > 0 else 0.0
    quality_metrics['uniqueness'] = uniqueness
    
    if unique_columns == 0:
        quality_issues.append('No unique constraints or primary keys found')
    

    pii_columns = sum(1 for col in columns if col.get('pii_detected', False))
    if pii_columns > 0:

        validity = 1.0
    else:
        validity = 1.0
    quality_metrics['validity'] = validity
    

    naming_patterns = {}
    for col in columns:
        col_name = col.get('name', '')
        if col_name:

            if '_' in col_name:
                naming_patterns['snake_case'] = naming_patterns.get('snake_case', 0) + 1
            elif col_name[0].islower() and any(c.isupper() for c in col_name[1:]):
                naming_patterns['camelCase'] = naming_patterns.get('camelCase', 0) + 1
    
    if len(naming_patterns) > 1:
        quality_issues.append('Inconsistent column naming patterns')
        quality_metrics['consistency'] = 0.7
    else:
        quality_metrics['consistency'] = 1.0
    

    last_modified = asset.get('last_modified')
    if last_modified:
        try:
            if isinstance(last_modified, str):
                from dateutil import parser
                last_modified_dt = parser.parse(last_modified)
            else:
                last_modified_dt = last_modified
            
            days_since_update = (datetime.utcnow() - last_modified_dt.replace(tzinfo=None)).days
            if days_since_update > 30:
                quality_issues.append(f'Data is {days_since_update} days old')
                quality_metrics['timeliness'] = max(0.0, 1.0 - (days_since_update / 365))
            else:
                quality_metrics['timeliness'] = 1.0
        except Exception:
            quality_metrics['timeliness'] = 0.5
    else:
        quality_metrics['timeliness'] = 0.5
        quality_issues.append('No last_modified date available')
    

    weights = {
        'completeness': 0.3,
        'uniqueness': 0.2,
        'validity': 0.2,
        'consistency': 0.15,
        'timeliness': 0.15
    }
    
    quality_score = sum(
        quality_metrics[metric] * weights[metric]
        for metric in quality_metrics
    )
    
    return {
        'quality_score': round(quality_score, 2),
        'quality_metrics': quality_metrics,
        'quality_issues': quality_issues,
        'quality_status': 'good' if quality_score >= 0.8 else 'warning' if quality_score >= 0.6 else 'poor'
    }


def propagate_quality_through_lineage(
    source_quality: Dict,
    target_quality: Dict,
    relationship: Dict
) -> Dict:
    source_score = source_quality.get('quality_score', 1.0)
    target_score = target_quality.get('quality_score', 1.0)
    

    transformation_type = relationship.get('transformation_type', 'pass_through')
    degradation_factors = {
        'pass_through': 0.95,
        'aggregate': 0.90,
        'transform': 0.85,
        'join': 0.80,
    }
    
    degradation = degradation_factors.get(transformation_type, 0.90)
    expected_target_score = source_score * degradation
    

    quality_impact = {
        'source_quality': source_score,
        'expected_target_quality': expected_target_score,
        'actual_target_quality': target_score,
        'quality_delta': target_score - expected_target_score,
        'quality_risk': 'high' if target_score < expected_target_score * 0.9 else 'medium' if target_score < expected_target_score else 'low'
    }
    
    return {
        'propagated_quality': expected_target_score,
        'impact_analysis': quality_impact
    }


