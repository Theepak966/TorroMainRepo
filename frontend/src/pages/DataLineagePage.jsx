import React, { useState, useEffect, useCallback, useRef } from 'react';
import ReactFlow, {
  MiniMap,
  Controls,
  Background,
  useNodesState,
  useEdgesState,
  MarkerType,
  Panel,
  Handle,
  Position,
} from 'reactflow';
import 'reactflow/dist/style.css';
import {
  Box,
  Typography,
  Card,
  CardContent,
  Chip,
  Button,
  CircularProgress,
  Alert,
  Dialog,
  DialogTitle,
  DialogContent,
  DialogActions,
  Grid,
  Paper,
  Table,
  TableBody,
  TableCell,
  TableContainer,
  TableHead,
  TableRow,
  FormControl,
  InputLabel,
  Select,
  MenuItem,
  TextField,
  InputAdornment,
  IconButton,
  Tooltip,
  Autocomplete,
  Tabs,
  Tab,
  Menu,
  Divider,
  List,
  ListItem,
  ListItemText,
  LinearProgress,
  Badge,
} from '@mui/material';
import {
  Refresh,
  ZoomIn,
  ZoomOut,
  FitScreen,
  Info,
  DataObject,
  Search,
  Close,
  AccountTree,
  FilterList,
  TableChart,
  Download,
  Analytics,
  Warning,
  TrendingUp,
  MoreVert,
  CloudDownload,
  Share,
  Add,
  Description,
  ArrowForward,
} from '@mui/icons-material';
import ManualLineageDialog from '../components/ManualLineageDialog';

const CustomNode = ({ data }) => {
  const isSelected = data.isSelected;
  const isPipelineNode = data.isPipelineNode || false;
  const pipelineColor = data.pipelineColor || '#666';
  const pipelineLabel = data.pipelineLabel || '';
  const isUpstream = data.isUpstream || false;
  const isDownstream = data.isDownstream || false;
  
  // Determine border color: pipeline > upstream/downstream > default
  let borderColor = '#ddd';
  let borderWidth = '1px';
  if (isPipelineNode) {
    borderColor = pipelineColor;
    borderWidth = '3px';
  } else if (isUpstream) {
    borderColor = '#2196f3'; // Blue for upstream
    borderWidth = '2px';
  } else if (isDownstream) {
    borderColor = '#4caf50'; // Green for downstream
    borderWidth = '2px';
  }
  
  return (
    <>
      <Handle type="target" position={Position.Left} style={{ background: isPipelineNode ? pipelineColor : (isUpstream ? '#2196f3' : isDownstream ? '#4caf50' : '#666'), width: 10, height: 10 }} />
      <Handle type="source" position={Position.Right} style={{ background: isPipelineNode ? pipelineColor : (isUpstream ? '#2196f3' : isDownstream ? '#4caf50' : '#666'), width: 10, height: 10 }} />
      
      <Box
        sx={{
          px: 3,
          py: 2,
          borderRadius: 1,
          border: `${borderWidth} solid ${borderColor}`,
          backgroundColor: isPipelineNode ? `${pipelineColor}15` : (isUpstream ? '#2196f315' : isDownstream ? '#4caf5015' : (isSelected ? '#f5f5f5' : '#ffffff')),
          minWidth: 200,
          cursor: 'pointer',
          transition: 'all 0.2s ease',
          boxShadow: isSelected ? '0 0 0 2px #666' : (isPipelineNode ? `0 2px 8px ${pipelineColor}40` : (isUpstream || isDownstream) ? `0 2px 6px ${borderColor}40` : '0 1px 3px rgba(0,0,0,0.1)'),
          '&:hover': {
            boxShadow: isSelected ? '0 0 0 2px #666' : (isPipelineNode ? `0 4px 12px ${pipelineColor}60` : (isUpstream || isDownstream) ? `0 3px 8px ${borderColor}60` : '0 2px 6px rgba(0,0,0,0.15)'),
            transform: 'translateY(-1px)',
          },
        }}
        onClick={() => data.onNodeClick && data.onNodeClick(data.id)}
      >
      {isPipelineNode && pipelineLabel && (
        <Box sx={{ mb: 1 }}>
          <Chip 
            label={pipelineLabel} 
            size="small" 
            sx={{ 
              height: 20, 
              fontSize: 9,
              fontWeight: 700,
              backgroundColor: pipelineColor,
              color: '#ffffff',
              border: 'none'
            }} 
          />
        </Box>
      )}
      <Box sx={{ display: 'flex', alignItems: 'center', gap: 1, mb: 1 }}>
        <DataObject sx={{ fontSize: 18, color: isPipelineNode ? pipelineColor : '#666' }} />
        <Typography variant="body2" sx={{ fontWeight: 600, fontSize: 14, color: '#333' }}>
          {data.name}
        </Typography>
      </Box>
      <Box sx={{ display: 'flex', gap: 1, flexWrap: 'wrap', alignItems: 'center' }}>
      <Chip 
          label={data.type || 'Azure Blob Storage'} 
        size="small" 
          variant="outlined"
        sx={{ 
            height: 24, 
          fontSize: 11,
            borderColor: '#ccc',
            color: '#666',
            fontWeight: 500,
            minWidth: '50px'
          }} 
        />
      </Box>
    </Box>
    </>
  );
};

const nodeTypes = {
  custom: CustomNode,
};

const detectPII = (columnName, description) => {
  const piiPatterns = [
    
    /email/i, /e-mail/i, /mail/i,
    
    /phone/i, /mobile/i, /cell/i, /telephone/i,
    
    /firstname/i, /lastname/i, /fullname/i, /name/i,
    
    /address/i, /street/i, /city/i, /zip/i, /postal/i,
    
    /ssn/i, /social/i, /passport/i, /license/i, /id/i,
    
    /credit/i, /card/i, /account/i, /bank/i,
    
    /birth/i, /age/i, /gender/i, /race/i, /ethnicity/i,
    
    /location/i, /gps/i, /coordinate/i, /lat/i, /lng/i,
    
    /password/i, /secret/i, /private/i, /confidential/i
  ];
  
  const combinedText = `${columnName} ${description || ''}`.toLowerCase();
  return piiPatterns.some(pattern => pattern.test(combinedText));
};

const DataLineagePage = () => {
  const [nodes, setNodes, onNodesChange] = useNodesState([]);
  const [edges, setEdges, onEdgesChange] = useEdgesState([]);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState(null);
  const [assets, setAssets] = useState([]);
  const [searchTerm, setSearchTerm] = useState('');
  const [filterType, setFilterType] = useState('all');
  const [filterSource, setFilterSource] = useState('all');
  const [columnRelationships, setColumnRelationships] = useState(0);
  const [selectedEdge, setSelectedEdge] = useState(null);
  const [edgeDetailsOpen, setEdgeDetailsOpen] = useState(false);
  const [selectedAssetForLineage, setSelectedAssetForLineage] = useState(null);
  const [fullLineageData, setFullLineageData] = useState({ nodes: [], edges: [] });
  const [showAssetDetails, setShowAssetDetails] = useState(false);
  const [selectedAssetDetails, setSelectedAssetDetails] = useState(null);
  const [activeDetailTab, setActiveDetailTab] = useState('basic');
  const [selectedNode, setSelectedNode] = useState(null);
  const [detailsDialogOpen, setDetailsDialogOpen] = useState(false);
  const [avgConfidence, setAvgConfidence] = useState(null);
  const [columnPage, setColumnPage] = useState(0);
  const [lineageViewMode, setLineageViewMode] = useState('hierarchical'); // 'hierarchical' or 'actual'
  const [columnsPerPage] = useState(10);
  const [manualLineageOpen, setManualLineageOpen] = useState(false);
  const [impactAnalysis, setImpactAnalysis] = useState(null);
  const [showImpactAnalysis, setShowImpactAnalysis] = useState(false);
  // Lineage extraction status removed - runs silently in background
  // Prevent request storms: only trigger extraction once per page load
  const extractionTriggeredRef = useRef(false);

  const fetchAllAssets = async () => {
    try {
      const API_BASE_URL = import.meta.env.VITE_API_BASE_URL;

      // Prefer paginated endpoint; fall back to old array response.
      // NOTE: /api/assets?minimal=1 intentionally omits total/total_pages for performance,
      // so we must paginate until has_next is false (or until fewer than per_page assets).
      const perPage = 1000;
      const maxPages = 10000; // safety guard

      const all = [];
      for (let page = 1; page <= maxPages; page++) {
        let resp;
        try {
          resp = await fetch(`${API_BASE_URL}/api/assets?page=${page}&per_page=${perPage}&minimal=1`);
        } catch (pageError) {
          console.error(`Error fetching page ${page}:`, pageError);
          break;
        }

        if (!resp.ok) {
          console.warn(`Failed to fetch page ${page}:`, resp.status, resp.statusText);
          break;
        }

        let data;
        try {
          data = await resp.json();
        } catch (jsonError) {
          console.error(`Failed to parse JSON for page ${page}:`, jsonError);
          break;
        }

        // Backward compatibility (old API returned an array)
        if (Array.isArray(data)) {
          all.push(...data);
          break;
        }

        if (!data || !Array.isArray(data.assets)) {
          console.warn('Invalid response format from assets API');
          break;
        }

        all.push(...data.assets);

        const hasNext = Boolean(data.pagination && data.pagination.has_next);
        if (!hasNext || data.assets.length < perPage) {
          break;
        }
      }

      return all;
    } catch (error) {
      console.error('Error in fetchAllAssets:', error);
      return [];
    }
  };

  const fetchLineage = async () => {
      setLoading(true);
    try {
      const API_BASE_URL = import.meta.env.VITE_API_BASE_URL;
      
      // Step 1: Fetch first page of assets immediately (don't wait for all)
      let assets = [];
      try {
        const firstResp = await fetch(`${API_BASE_URL}/api/assets?page=1&per_page=1000&minimal=1`);
        if (firstResp.ok) {
          const firstData = await firstResp.json();
          if (Array.isArray(firstData)) {
            assets = firstData;
          } else if (firstData && Array.isArray(firstData.assets)) {
            assets = firstData.assets;
          }
        }
      } catch (err) {
        console.warn('Failed to fetch first page of assets:', err);
      }
      // Note: full asset loading is handled by fetchAssets() (single place) to avoid duplicate paging storms.
      
      if (assets && assets.length > 0) {
        // Step 2: Check for Oracle and Azure Blob connections and trigger lineage extraction
        const oracleConnections = new Set();
        const azureBlobConnections = new Set();
        
        assets.forEach(asset => {
          if (asset.connector_id && asset.connector_id.startsWith('oracle_db_')) {
            // Extract connection name from connector_id (format: oracle_db_ConnectionName)
            const parts = asset.connector_id.split('_');
            if (parts.length >= 3) {
              const connectionName = parts.slice(2).join('_');
              oracleConnections.add(connectionName);
            }
          } else if (asset.connector_id && asset.connector_id.startsWith('azure_blob_')) {
            // Extract connection name from connector_id (format: azure_blob_ConnectionName)
            const parts = asset.connector_id.split('_');
            if (parts.length >= 3) {
              const connectionName = parts.slice(2).join('_');
              azureBlobConnections.add(connectionName);
            }
          }
        });
        
        const shouldTriggerExtraction = !extractionTriggeredRef.current;
        if (shouldTriggerExtraction) {
          extractionTriggeredRef.current = true;
        }

        // Step 3: Trigger lineage extraction for Oracle connections (only once, silently)
        if (shouldTriggerExtraction && oracleConnections.size > 0) {
          try {
            const connectionsResp = await fetch(`${API_BASE_URL}/api/connections`);
            if (connectionsResp.ok) {
              const connections = await connectionsResp.json();
              
              const oracleExtractionPromises = [];
              for (const connection of connections) {
                if (connection.connector_type === 'oracle_db' && 
                    oracleConnections.has(connection.name)) {
                  // Trigger lineage extraction
                  const extractionPromise = fetch(`${API_BASE_URL}/api/connections/${connection.id}/extract-lineage`, {
                    method: 'POST',
                    headers: { 'Content-Type': 'application/json' }
                  }).then(resp => {
                    if (!resp.ok) {
                      throw new Error(`HTTP ${resp.status}: ${resp.statusText}`);
                    }
                    return resp.json();
                  }).then(data => {
                    // Silently log - no UI update
                    console.log(`Oracle lineage extraction started for connection ${connection.id}`);
                  }).catch(err => {
                    // Silently log error - no UI update
                    console.error(`Failed to trigger Oracle lineage extraction for connection ${connection.id}:`, err);
                  });
                  oracleExtractionPromises.push(extractionPromise);
                }
              }
              // Wait for all Oracle extractions to complete (but don't block UI)
              Promise.all(oracleExtractionPromises).catch(() => {});
            }
          } catch (err) {
            // Silently log error - no UI update
            console.error('Failed to trigger Oracle lineage extraction:', err);
          }
        }
        
        // Step 4: Trigger lineage extraction for Azure Blob connections (only once, silently)
        if (shouldTriggerExtraction && azureBlobConnections.size > 0) {
          try {
            const connectionsResp = await fetch(`${API_BASE_URL}/api/connections`);
            if (connectionsResp.ok) {
              const connections = await connectionsResp.json();
              
              const azureExtractionPromises = [];
              for (const connection of connections) {
                if (connection.connector_type === 'azure_blob' && 
                    azureBlobConnections.has(connection.name)) {
                  // Trigger lineage extraction
                  const extractionPromise = fetch(`${API_BASE_URL}/api/connections/${connection.id}/extract-azure-lineage`, {
                    method: 'POST',
                    headers: { 'Content-Type': 'application/json' }
                  }).then(resp => {
                    if (!resp.ok) {
                      throw new Error(`HTTP ${resp.status}: ${resp.statusText}`);
                    }
                    return resp.json();
                  }).then(data => {
                    // Silently log - no UI update
                    console.log(`Azure Blob lineage extraction started for connection ${connection.id}`);
                  }).catch(err => {
                    // Silently log error - no UI update
                    console.error(`Failed to trigger Azure Blob lineage extraction for connection ${connection.id}:`, err);
                  });
                  azureExtractionPromises.push(extractionPromise);
                }
              }
              // Wait for all Azure extractions to complete (but don't block UI)
              Promise.all(azureExtractionPromises).catch(() => {});
            }
          } catch (err) {
            // Silently log error - no UI update
            console.error('Failed to trigger Azure Blob lineage extraction:', err);
          }
        }
        
        // Step 5: Cross-platform lineage extraction removed
        
        // Step 6: Fetch lineage relationships (fetch immediately, no wait)
        let relationships = [];
        try {
          const relationshipsResp = await fetch(`${API_BASE_URL}/api/lineage/relationships`);
          if (relationshipsResp.ok) {
            const relationshipsData = await relationshipsResp.json();
            relationships = relationshipsData.relationships || [];
          } else {
            console.warn(`Failed to fetch lineage relationships: HTTP ${relationshipsResp.status}`);
          }
        } catch (err) {
          console.warn('Failed to fetch lineage relationships:', err);
        }
        
        
        const assetMap = new Map();
        assets.forEach(asset => {
          assetMap.set(asset.id, asset);
        });
        
        
        
        const lineageNodes = assets.map(asset => {
          
          let sourceSystem = 'Unknown';
          if (asset.connector_id) {
            const parts = asset.connector_id.split('_');
            if (parts[0] === 'azure' && parts[1] === 'blob') {
              sourceSystem = 'Azure Blob Storage';
            } else if (parts[0]) {
              sourceSystem = parts[0].charAt(0).toUpperCase() + parts[0].slice(1);
            }
          }
          
          return {
            id: asset.id,
            name: asset.name,
            type: asset.type || 'file',
            catalog: asset.catalog || 'default',
            schema: asset.schema || '',
            connector_id: asset.connector_id,
            columns: asset.columns || [],
            source_system: sourceSystem,
          };
        });
        
        
        
        const realRelationships = relationships.filter(rel => {
          const method = (rel.extraction_method || '').toLowerCase();
          
          return method === 'sql_parsing' || 
                 method === 'sql_column_parsing' ||
                 method === 'join_analysis' ||
                 method === 'ml_inference' ||
                 method === 'procedure_parsing' ||
                 method === 'trigger_analysis' ||
                 method === 'column_matching' ||
                 method === 'sql_column_analysis' ||
                 method === 'folder_hierarchy' ||
                 method === 'naming_pattern' ||
                 method === 'schema_matching' ||
                 method === 'sql_reference' ||
                 method === 'etl_pattern' ||
                 method === 'name_similarity' ||
                 method === 'cross_platform_matching' ||
                 method === 'name_matching' ||
                 method === 'manual' || 
                 method === 'api' ||
                 method === 'etl' ||
                 method === 'dbt' ||
                 method === 'databricks' ||
                 method === 'regex_fallback' ||
                 method === 'oracle_metadata';
          
        });
        
        const edges = realRelationships.map(rel => ({
          id: `${rel.source_asset_id}-${rel.target_asset_id}`,
          source: rel.source_asset_id,
          target: rel.target_asset_id,
          type: rel.relationship_type || 'transformation',
          column_lineage: rel.column_lineage || [],
          relationship: rel.column_lineage && rel.column_lineage.length > 0 
            ? `${rel.column_lineage.length} columns` 
            : 'feeds into',
          confidence_score: rel.confidence_score || 0.5,
          transformation_type: rel.transformation_type,
          source_system: rel.source_system,
          extraction_method: rel.extraction_method || 'unknown'
        }));
        
        const lineageData = {
          nodes: lineageNodes,
          edges: edges,
          rawData: { nodes: lineageNodes, edges: edges },
        };
        
        setFullLineageData(lineageData);
        setNodes([]);
        setEdges([]);
      } else {
        
        setFullLineageData({ nodes: [], edges: [], rawData: { nodes: [], edges: [] } });
        setNodes([]);
        setEdges([]);
      }
    } catch (error) {
      console.error('Error fetching lineage:', error);
      setFullLineageData({ nodes: [], edges: [], rawData: { nodes: [], edges: [] } });
      setNodes([]);
      setEdges([]);
    } finally {
      setLoading(false);
    }
  };

  const fetchAssets = async () => {
    try {
      const allAssets = await fetchAllAssets();
      setAssets(allAssets);
    } catch (error) {
      if (import.meta.env.DEV) {
      console.error('Error fetching assets:', error);
      }
      setAssets([]);
    }
  };

  useEffect(() => {
    fetchLineage();
    fetchAssets();
  }, []);

  // Re-apply view mode filter when lineageViewMode changes (only refresh display, don't re-fetch)
  useEffect(() => {
    if (selectedAssetForLineage && fullLineageData.rawData?.nodes?.length > 0) {
      // Re-apply the view mode filter using existing data - reuse handleAssetSelection logic
      const assetId = selectedAssetForLineage;
      const rawNodes = fullLineageData.rawData.nodes;
      const rawEdges = fullLineageData.rawData.edges;
      
      // Apply lineage view mode filter
      let filteredNodes = rawNodes;
      let filteredEdges = rawEdges;
      
      if (lineageViewMode === 'actual') {
        filteredNodes = rawNodes.filter(node => {
          const nodeId = node.id || '';
          return !nodeId.startsWith('container_') && !nodeId.startsWith('folder_');
        });
        
        filteredEdges = rawEdges.filter(edge => {
          if (edge.type === 'contains' || edge.folder_based) {
            return false;
          }
          const sourceIsAsset = !edge.source.startsWith('container_') && !edge.source.startsWith('folder_');
          const targetIsAsset = !edge.target.startsWith('container_') && !edge.target.startsWith('folder_');
          return sourceIsAsset && targetIsAsset;
        });
      }
      
      // Re-layout and update nodes/edges using the same logic as handleAssetSelection
      const layoutedNodes = layoutNodes(filteredNodes, filteredEdges);
      
      // Get pipeline stage function (same as in handleAssetSelection)
      const getPipelineStageForFiltered = (node) => {
        const nameLower = (node.name || '').toLowerCase();
        const catalogLower = (node.catalog || '').toLowerCase();
        const schemaLower = (node.schema || '').toLowerCase();
        const nodeIdLower = (node.id || '').toLowerCase();
        const connectorId = (node.connector_id || '').toLowerCase();
        const nodeType = (node.type || '').toUpperCase();
        
        const isETLPipeline = 
          connectorId === 'demo_etl_pipeline' || 
          connectorId === 'sample_connector' ||
          catalogLower.includes('demo') ||
          catalogLower === 'etl' ||
          nodeIdLower.includes(':etl:') ||
          nodeIdLower.includes('sample:etl:') ||
          schemaLower === 'source' ||
          schemaLower === 'staging' ||
          schemaLower === 'destination' ||
          nodeType === 'API';
        
        if (isETLPipeline) {
          if (catalogLower.includes('crm') || 
              catalogLower.includes('source') || 
              schemaLower === 'source' ||
              nodeIdLower.includes(':source:') ||
              nodeIdLower.includes('sample:etl:source:') ||
              nodeType === 'API'
              ) {
            return { stage: 'source', color: '#9c27b0', label: 'SOURCE' };
          } 
          else if (catalogLower.includes('staging') || 
                   schemaLower === 'staging' ||
                   nameLower.includes('raw') ||
                   nameLower.includes('staging') ||
                   nameLower.includes('transformed') ||
                   nodeIdLower.includes(':staging:') ||
                   nodeIdLower.includes('sample:etl:staging:')) {
            return { stage: 'extract', color: '#ff9800', label: 'EXTRACT' };
          } 
          else if (catalogLower.includes('warehouse') || 
                   nameLower.includes('dim') || 
                   nameLower.includes('fact') ||
                   nameLower.includes('transform')) {
            return { stage: 'transform', color: '#2196f3', label: 'TRANSFORM' };
          } 
          else if (catalogLower.includes('analytics') || 
                   catalogLower.includes('report') ||
                   schemaLower === 'destination' ||
                   nameLower.includes('destination') ||
                   nameLower.includes('warehouse') ||
                   nodeIdLower.includes(':destination:') ||
                   nodeIdLower.includes('sample:etl:destination:')) {
            return { stage: 'load', color: '#4caf50', label: 'LOAD' };
          }
        }
        return null;
      };
      
      // Determine upstream and downstream nodes
      const upstreamNodeIds = new Set();
      const downstreamNodeIds = new Set();
      
      if (assetId) {
        const findUpstream = (nodeId, visited = new Set()) => {
          if (visited.has(nodeId)) return;
          visited.add(nodeId);
          filteredEdges.forEach(edge => {
            if (edge.target === nodeId) {
              upstreamNodeIds.add(edge.source);
              findUpstream(edge.source, visited);
            }
          });
        };
        
        const findDownstream = (nodeId, visited = new Set()) => {
          if (visited.has(nodeId)) return;
          visited.add(nodeId);
          filteredEdges.forEach(edge => {
            if (edge.source === nodeId) {
              downstreamNodeIds.add(edge.target);
              findDownstream(edge.target, visited);
            }
          });
        };
        
        findUpstream(assetId);
        findDownstream(assetId);
      }
      
      const flowNodes = layoutedNodes.map((node) => {
        const pipelineInfo = getPipelineStageForFiltered(node);
        const isPipelineNode = pipelineInfo !== null;
        const isUpstream = upstreamNodeIds.has(node.id);
        const isDownstream = downstreamNodeIds.has(node.id);
        
        return {
          id: node.id,
          type: 'custom',
          position: node.position,
          style: isPipelineNode ? {
            border: `3px solid ${pipelineInfo.color}`,
            backgroundColor: `${pipelineInfo.color}15`,
          } : {},
          data: {
            label: node.name,
            name: node.name,
            type: node.type,
            catalog: node.catalog,
            schema: node.schema,
            connector_id: node.connector_id,
            source_system: node.source_system,
            id: node.id,
            isSelected: node.id === assetId,
            onNodeClick: handleNodeClick,
            pipelineStage: pipelineInfo?.stage,
            pipelineLabel: pipelineInfo?.label,
            pipelineColor: pipelineInfo?.color,
            isPipelineNode: isPipelineNode,
            isUpstream: isUpstream,
            isDownstream: isDownstream,
          },
        };
      });
      
      const getPipelineRelationshipForFiltered = (edge) => {
        const relationship = (edge.relationship || '').toLowerCase();
        const sourceId = (edge.source || '').toLowerCase();
        const targetId = (edge.target || '').toLowerCase();
        
        if (relationship.includes('extract') || relationship.includes('etl_extract')) {
          return { type: 'extract', color: '#ff9800', label: 'EXTRACT' };
        } else if (relationship.includes('transform') || relationship.includes('etl_transform')) {
          return { type: 'transform', color: '#2196f3', label: 'TRANSFORM' };
        } else if (relationship.includes('load') || relationship.includes('etl_load')) {
          return { type: 'load', color: '#4caf50', label: 'LOAD' };
        }
        
        if (sourceId.includes('sample:etl:') || targetId.includes('sample:etl:')) {
          if (sourceId.includes(':source:') && targetId.includes(':staging:')) {
            return { type: 'extract', color: '#ff9800', label: 'EXTRACT' };
          }
          if (sourceId.includes(':staging:') && targetId.includes(':destination:')) {
            return { type: 'load', color: '#4caf50', label: 'LOAD' };
          }
        }
        
        return null;
      };
      
      const flowEdges = filteredEdges.map((edge) => {
        const columnCount = edge.column_lineage ? edge.column_lineage.length : 0;
        const pipelineRel = getPipelineRelationshipForFiltered(edge);
        const isPipelineEdge = pipelineRel !== null;
        
        let edgeColor = '#64b5f6';
        let edgeWidth = 1;
        if (isPipelineEdge) {
          edgeColor = pipelineRel.color;
          edgeWidth = 2.5;
        } else if (columnCount > 0) {
          edgeColor = '#1976d2';
          edgeWidth = 1.5;
        }
        
        const label = isPipelineEdge 
          ? `${pipelineRel.label} (${columnCount} cols)`
          : columnCount > 0 
            ? `${columnCount} columns` 
            : edge.relationship || 'feeds into';
        
        return {
          id: `${edge.source}->${edge.target}`,
          source: edge.source,
          target: edge.target,
          sourceHandle: null,
          targetHandle: null,
          type: 'smoothstep',
          animated: isPipelineEdge || columnCount > 0,
          markerEnd: {
            type: MarkerType.ArrowClosed,
            width: isPipelineEdge ? 16 : 12,
            height: isPipelineEdge ? 16 : 12,
            color: edgeColor,
          },
          style: {
            strokeWidth: edgeWidth,
            stroke: edgeColor,
            strokeDasharray: isPipelineEdge ? '0' : (columnCount > 0 ? '0' : '5,5'),
            opacity: isPipelineEdge ? 1 : 0.8,
          },
          label: label,
          labelStyle: { 
            fill: '#ffffff', 
            fontWeight: 600, 
            fontSize: isPipelineEdge ? 12 : 11,
            textShadow: '0 1px 2px rgba(0,0,0,0.3)'
          },
          labelBgStyle: { 
            fill: edgeColor, 
            fillOpacity: 0.9,
            padding: isPipelineEdge ? '6px 10px' : '4px 8px',
            borderRadius: '12px',
            stroke: '#ffffff',
            strokeWidth: 1
          },
          data: {
            column_lineage: edge.column_lineage || [],
            relationship: edge.relationship,
            extraction_method: edge.extraction_method,
            transformation_type: edge.transformation_type,
            onEdgeClick: handleEdgeClick,
          },
        };
      });
      
      setNodes(flowNodes);
      setEdges(flowEdges);
    }
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [lineageViewMode]); // Only trigger when view mode changes (not when clicking nodes)

  
  const layoutNodes = (nodes, edges) => {
    
    const adjacencyMap = new Map();
    nodes.forEach(node => adjacencyMap.set(node.id, { node, children: [], parents: [] }));
    
    edges.forEach(edge => {
      const sourceNode = adjacencyMap.get(edge.source);
      const targetNode = adjacencyMap.get(edge.target);
      if (sourceNode && targetNode) {
        sourceNode.children.push(edge.target);
        targetNode.parents.push(edge.source);
      }
    });

    
    const rootNodes = nodes.filter(node => {
      const nodeData = adjacencyMap.get(node.id);
      return nodeData.parents.length === 0;
    });

    
    const startNodes = rootNodes.length > 0 ? rootNodes : nodes;

    
    const levels = new Map();
    const visited = new Set();
    const queue = startNodes.map(node => ({ id: node.id, level: 0 }));

    while (queue.length > 0) {
      const { id, level } = queue.shift();
      if (visited.has(id)) continue;
      
      visited.add(id);
      levels.set(id, level);

      const nodeData = adjacencyMap.get(id);
      if (nodeData) {
        nodeData.children.forEach(childId => {
          if (!visited.has(childId)) {
            queue.push({ id: childId, level: level + 1 });
          }
        });
      }
    }

    
    nodes.forEach(node => {
      if (!levels.has(node.id)) {
        levels.set(node.id, 0);
      }
    });

    
    const nodesByCatalog = new Map();
    nodes.forEach(node => {
      const catalog = node.catalog || 'default';
      const connector = node.connector_id ? node.connector_id.split('_')[0] : 'unknown';
      const groupKey = `${catalog}_${connector}`;
      
      if (!nodesByCatalog.has(groupKey)) {
        nodesByCatalog.set(groupKey, []);
      }
      nodesByCatalog.get(groupKey).push(node);
    });

    
    const nodesByLevel = new Map();
    levels.forEach((level, nodeId) => {
      if (!nodesByLevel.has(level)) {
        nodesByLevel.set(level, []);
      }
      nodesByLevel.get(level).push(nodeId);
    });

    
    const levelSpacing = 300;
    const nodeSpacing = 120;
    const catalogSpacing = 200;
    const layoutedNodes = [];

    
    nodesByLevel.forEach((nodeIds, level) => {
      
      const nodesInLevelByCatalog = new Map();
      nodeIds.forEach(nodeId => {
        const node = nodes.find(n => n.id === nodeId);
        if (node) {
          const catalog = node.catalog || 'default';
          const connector = node.connector_id ? node.connector_id.split('_')[0] : 'unknown';
          const groupKey = `${catalog}_${connector}`;
          
          if (!nodesInLevelByCatalog.has(groupKey)) {
            nodesInLevelByCatalog.set(groupKey, []);
          }
          nodesInLevelByCatalog.get(groupKey).push(node);
        }
      });

      
      let catalogYOffset = 0;
      nodesInLevelByCatalog.forEach((catalogNodes, groupKey) => {
        catalogNodes.forEach((node, index) => {
      const x = level * levelSpacing;
          const y = catalogYOffset + index * nodeSpacing + 300;

      layoutedNodes.push({
        ...node,
        position: { x, y },
          });
        });
        catalogYOffset += catalogNodes.length * nodeSpacing + catalogSpacing;
      });
    });

    
    nodes.forEach(node => {
      if (!layoutedNodes.find(n => n.id === node.id)) {
        const catalog = node.catalog || 'default';
        const connector = node.connector_id ? node.connector_id.split('_')[0] : 'unknown';
        const groupKey = `${catalog}_${connector}`;
        const catalogNodes = nodesByCatalog.get(groupKey) || [];
        const indexInCatalog = catalogNodes.findIndex(n => n.id === node.id);
        
        layoutedNodes.push({
          ...node,
          position: { 
            x: 0, 
            y: indexInCatalog * nodeSpacing + 300 
          },
        });
      }
    });

    return layoutedNodes;
  };

  
  const handleNodeClick = async (nodeId) => {
    try {
      setColumnPage(0);
      
      // Check node ID patterns FIRST (most reliable method)
      if (nodeId.startsWith('container_')) {
        const containerName = nodeId.replace('container_', '');
        setSelectedNode({
          id: nodeId,
          name: containerName,
          type: 'Container',
          node_type: 'container',
          description: `Azure Blob Storage Container: ${containerName}`,
          isContainer: true,
          source_system: 'Azure Blob Storage',
          connector_id: 'azure_blob_storage'
        });
        setDetailsDialogOpen(true);
        return;
      }
      
      if (nodeId.startsWith('folder_')) {
        const folderPath = nodeId.replace('folder_', '').replace(/_/g, '/');
        const folderName = folderPath.split('/').pop() || folderPath;
        setSelectedNode({
          id: nodeId,
          name: folderName,
          type: 'Folder',
          node_type: 'folder',
          full_path: folderPath,
          description: `Folder in storage hierarchy: ${folderPath}`,
          isFolder: true,
          source_system: 'Azure Blob Storage',
          connector_id: 'azure_blob_storage'
        });
        setDetailsDialogOpen(true);
        return;
      }
      
      // Check if this is a container or folder node from the lineage
      const lineageNode = fullLineageData.rawData?.nodes?.find(n => n.id === nodeId);
      
      if (lineageNode) {
        const nodeType = lineageNode.node_type || lineageNode.type;
        
        // Handle container nodes
        if (nodeType === 'container') {
          setSelectedNode({
            id: nodeId,
            name: lineageNode.name || nodeId,
            type: 'Container',
            node_type: 'container',
            description: `Azure Blob Storage Container: ${lineageNode.name}`,
            isContainer: true,
            source_system: 'Azure Blob Storage',
            connector_id: 'azure_blob_storage'
          });
          setDetailsDialogOpen(true);
          return;
        }
        
        // Handle folder nodes
        if (nodeType === 'folder') {
          setSelectedNode({
            id: nodeId,
            name: lineageNode.name || nodeId,
            type: 'Folder',
            node_type: 'folder',
            full_path: lineageNode.full_path || lineageNode.name,
            description: `Folder in storage hierarchy: ${lineageNode.full_path || lineageNode.name}`,
            isFolder: true,
            source_system: 'Azure Blob Storage',
            connector_id: 'azure_blob_storage'
          });
          setDetailsDialogOpen(true);
          return;
        }
      }
      
      // For regular assets, just open details dialog - DO NOT reload lineage
      const asset = assets.find(a => a.id === nodeId);
      
      if (asset) {
        // Just open details dialog without reloading lineage
        setSelectedNode(asset);
        setDetailsDialogOpen(true);
      } else {
        // If not found in assets, try to get from lineage node
        if (lineageNode) {
          // Just open details dialog - DO NOT reload lineage
          setSelectedNode({
            id: nodeId,
            name: lineageNode.name || nodeId,
            type: lineageNode.type || 'Azure Blob Storage',
            catalog: lineageNode.catalog,
            error: 'Asset details not available'
          });
          setDetailsDialogOpen(true);
        } else {
          // If it looks like an asset ID but not found, just show error - DO NOT reload
          setSelectedNode({ id: nodeId, name: nodeId, error: 'Asset details not found' });
          setDetailsDialogOpen(true);
        }
      }
    } catch (error) {
      if (import.meta.env.DEV) {
        console.error('Error fetching node details:', error);
      }
      setSelectedNode({ id: nodeId, name: nodeId, error: error.message });
      setDetailsDialogOpen(true);
    }
  };

  
  const handleCloseDialog = () => {
    setDetailsDialogOpen(false);
    setSelectedNode(null);
    setColumnPage(0);  
  };

  
  const handleAssetDetailsSelection = async (assetId) => {
    if (!assetId) {
      setShowAssetDetails(false);
      setSelectedAssetDetails(null);
      return;
    }

    try {
      const API_BASE_URL = import.meta.env.VITE_API_BASE_URL;
      
      // First check if asset is in the assets list (already fetched)
      const cachedAsset = assets.find(a => a.id === assetId);
      if (cachedAsset) {
        setSelectedAssetDetails(cachedAsset);
        setShowAssetDetails(true);
        return;
      }
      
      // Fetch full asset details from API
      const response = await fetch(`${API_BASE_URL}/api/assets/${assetId}`);
      if (response.ok) {
        const asset = await response.json();
        setSelectedAssetDetails(asset);
        setShowAssetDetails(true);
      } else {
        // Fallback to lineage data if API fails
        const asset = fullLineageData.rawData?.nodes?.find(n => n.id === assetId);
        if (asset) {
          setSelectedAssetDetails(asset);
          setShowAssetDetails(true);
        } else {
          if (import.meta.env.DEV) {
            console.error('Asset not found:', assetId);
          }
        }
      }
    } catch (error) {
      if (import.meta.env.DEV) {
        console.error('Error fetching asset details:', error);
      }
      // Fallback to lineage data on error
      const asset = fullLineageData.rawData?.nodes?.find(n => n.id === assetId);
      if (asset) {
        setSelectedAssetDetails(asset);
        setShowAssetDetails(true);
      }
    }
  };

  const handleEdgeClick = (edgeData) => {
    setSelectedEdge(edgeData);
    setEdgeDetailsOpen(true);
  };

  const handleCloseEdgeDialog = () => {
    setEdgeDetailsOpen(false);
    setSelectedEdge(null);
  };

  const onEdgeClick = (event, edge) => {
    if (edge.data) {
      handleEdgeClick({
        ...edge.data,
        source: edge.source,
        target: edge.target,
        relationship: edge.data.relationship || 'feeds into',
        confidence_score: edge.data.confidence_score,
        extraction_method: edge.data.extraction_method,
        transformation_type: edge.data.transformation_type
      });
      setEdgeDetailsOpen(true);
    }
  };

  
  const handleAssetSelection = async (assetId) => {
    if (!assetId) {
      setSelectedAssetForLineage(null);
      setNodes([]);
      setEdges([]);
      return;
    }

    setSelectedAssetForLineage(assetId);
    setLoading(true);

    try {
      const API_BASE_URL = import.meta.env.VITE_API_BASE_URL;
      
      // Use new lineage system endpoint
      const lineageResponse = await fetch(`${API_BASE_URL}/api/lineage/asset/${assetId}`);
      
      if (lineageResponse.ok) {
        const lineageData = await lineageResponse.json();
        const { nodes, edges, upstream_count, downstream_count } = lineageData.lineage;
        
        // Generate impact analysis from lineage data (old endpoint removed)
        if (lineageData.lineage) {
          const impactData = {
            asset_id: assetId,
            impact_summary: {
              direct_impact: downstream_count || 0,
              total_impacted_assets: downstream_count || 0,
              total_dependencies: upstream_count || 0,
              impact_depth: 1
            },
            impacted_assets: nodes.filter(n => n.id !== assetId && edges.some(e => e.source === assetId && e.target === n.id)),
            impact_paths: edges.filter(e => e.source === assetId).map(e => ({
              path: [assetId, e.target],
              relationship: { type: e.type, columns: e.column_lineage || [] },
              depth: 1
            })),
            dependencies: nodes
              .filter(n => n.id !== assetId && edges.some(e => e.target === assetId && e.source === n.id))
              .map(n => {
                const relatedEdge = edges.find(e => e.target === assetId && e.source === n.id);
                return {
                  ...n,
                  relationship: relatedEdge ? { 
                    type: relatedEdge.type || 'related', 
                    columns: relatedEdge.column_lineage || [] 
                  } : { type: 'related', columns: [] }
                };
              })
          };
          setImpactAnalysis(impactData);
        }
        
        
        // Filter based on view mode
        let filteredNodes = nodes;
        let filteredEdges = edges;
        
        if (lineageViewMode === 'actual') {
          // Actual lineage: Remove hierarchy nodes (containers, folders) and their edges
          // Keep only asset nodes and real lineage relationships
          filteredNodes = nodes.filter(node => {
            const nodeId = node.id || '';
            // Exclude container and folder nodes
            return !nodeId.startsWith('container_') && !nodeId.startsWith('folder_');
          });
          
          filteredEdges = edges.filter(edge => {
            // Exclude hierarchy edges (contains relationships)
            if (edge.type === 'contains' || edge.folder_based) {
              return false;
            }
            // Only include edges between actual assets (not hierarchy nodes)
            const sourceIsAsset = !edge.source.startsWith('container_') && !edge.source.startsWith('folder_');
            const targetIsAsset = !edge.target.startsWith('container_') && !edge.target.startsWith('folder_');
            return sourceIsAsset && targetIsAsset;
          });
        } else {
          // Hierarchical view: Show everything (containers, folders, assets, hierarchy edges)
          filteredNodes = nodes;
          filteredEdges = edges;
        }
    
    const layoutedNodes = layoutNodes(filteredNodes, filteredEdges);
    
    const getPipelineStageForFiltered = (node) => {
      const nameLower = (node.name || '').toLowerCase();
      const catalogLower = (node.catalog || '').toLowerCase();
      const schemaLower = (node.schema || '').toLowerCase();
      const nodeIdLower = (node.id || '').toLowerCase();
      const connectorId = (node.connector_id || '').toLowerCase();
      const nodeType = (node.type || '').toUpperCase();
      
      const isETLPipeline = 
        connectorId === 'demo_etl_pipeline' || 
        connectorId === 'sample_connector' ||
        catalogLower.includes('demo') ||
        catalogLower === 'etl' ||
        nodeIdLower.includes(':etl:') ||
        nodeIdLower.includes('sample:etl:') ||
        schemaLower === 'source' ||
        schemaLower === 'staging' ||
        schemaLower === 'destination' ||
        nodeType === 'API';
      
      if (isETLPipeline) {
        if (catalogLower.includes('crm') || 
            catalogLower.includes('source') || 
            schemaLower === 'source' ||
            nodeIdLower.includes(':source:') ||
            nodeIdLower.includes('sample:etl:source:') ||
            nodeType === 'API'
            ) {
          return { stage: 'source', color: '#9c27b0', label: 'SOURCE' };
        } 
        else if (catalogLower.includes('staging') || 
                 schemaLower === 'staging' ||
                 nameLower.includes('raw') ||
                 nameLower.includes('staging') ||
                 nameLower.includes('transformed') ||
                 nodeIdLower.includes(':staging:') ||
                 nodeIdLower.includes('sample:etl:staging:')) {
          return { stage: 'extract', color: '#ff9800', label: 'EXTRACT' };
        } 
        else if (catalogLower.includes('warehouse') || 
                 nameLower.includes('dim') || 
                 nameLower.includes('fact') ||
                 nameLower.includes('transform')) {
          return { stage: 'transform', color: '#2196f3', label: 'TRANSFORM' };
        } 
        else if (catalogLower.includes('analytics') || 
                 catalogLower.includes('report') ||
                 schemaLower === 'destination' ||
                 nameLower.includes('destination') ||
                 nameLower.includes('warehouse') ||
                 nodeIdLower.includes(':destination:') ||
                 nodeIdLower.includes('sample:etl:destination:')) {
          return { stage: 'load', color: '#4caf50', label: 'LOAD' };
        }
      }
      return null;
    };
    
    // Determine upstream and downstream nodes relative to selected asset
    const upstreamNodeIds = new Set();
    const downstreamNodeIds = new Set();
    
    if (assetId) {
      // Find all upstream nodes (nodes that feed into the selected asset)
      const findUpstream = (nodeId, visited = new Set()) => {
        if (visited.has(nodeId)) return;
        visited.add(nodeId);
        filteredEdges.forEach(edge => {
          if (edge.target === nodeId) {
            upstreamNodeIds.add(edge.source);
            findUpstream(edge.source, visited);
          }
        });
      };
      
      // Find all downstream nodes (nodes that the selected asset feeds into)
      const findDownstream = (nodeId, visited = new Set()) => {
        if (visited.has(nodeId)) return;
        visited.add(nodeId);
        filteredEdges.forEach(edge => {
          if (edge.source === nodeId) {
            downstreamNodeIds.add(edge.target);
            findDownstream(edge.target, visited);
          }
        });
      };
      
      findUpstream(assetId);
      findDownstream(assetId);
    }
    
    const flowNodes = layoutedNodes.map((node) => {
      const originalNode = filteredNodes.find(n => n.id === node.id);
      const pipelineInfo = getPipelineStageForFiltered(node);
      const isPipelineNode = pipelineInfo !== null;
      const isUpstream = upstreamNodeIds.has(node.id);
      const isDownstream = downstreamNodeIds.has(node.id);
      
      return {
        id: node.id,
        type: 'custom',
        position: node.position,
        sourcePosition: 'right',
        targetPosition: 'left',
        style: isPipelineNode ? {
          border: `3px solid ${pipelineInfo.color}`,
          backgroundColor: `${pipelineInfo.color}15`,
        } : {},
        data: {
          label: node.name,
          name: node.name,
          type: node.type,
          catalog: node.catalog,
          schema: node.schema,
          connector_id: node.connector_id,
          source_system: node.source_system,
          id: node.id,
          isSelected: node.id === assetId,
          onNodeClick: handleNodeClick,
          pipelineStage: pipelineInfo?.stage,
          pipelineLabel: pipelineInfo?.label,
          pipelineColor: pipelineInfo?.color,
          isPipelineNode: isPipelineNode,
          isUpstream: isUpstream,
          isDownstream: isDownstream,
        },
      };
    });
    
    const getPipelineRelationshipForFiltered = (edge) => {
      const relationship = (edge.relationship || '').toLowerCase();
      const sourceId = (edge.source || '').toLowerCase();
      const targetId = (edge.target || '').toLowerCase();
      
      if (relationship.includes('extract') || relationship.includes('etl_extract')) {
        return { type: 'extract', color: '#ff9800', label: 'EXTRACT' };
      } else if (relationship.includes('transform') || relationship.includes('etl_transform')) {
        return { type: 'transform', color: '#2196f3', label: 'TRANSFORM' };
      } else if (relationship.includes('load') || relationship.includes('etl_load')) {
        return { type: 'load', color: '#4caf50', label: 'LOAD' };
      }
      
      if (sourceId.includes('sample:etl:') || targetId.includes('sample:etl:')) {
        if (sourceId.includes(':source:') && targetId.includes(':staging:')) {
          return { type: 'extract', color: '#ff9800', label: 'EXTRACT' };
        }
        if (sourceId.includes(':staging:') && targetId.includes(':destination:')) {
          return { type: 'load', color: '#4caf50', label: 'LOAD' };
        }
      }
      
      return null;
    };
    
    const flowEdges = filteredEdges.map((edge, index) => {
      const columnCount = edge.column_lineage ? edge.column_lineage.length : 0;
      const pipelineRel = getPipelineRelationshipForFiltered(edge);
      const isPipelineEdge = pipelineRel !== null;
      
      
      let edgeColor = '#64b5f6';
      let edgeWidth = 1;
      if (isPipelineEdge) {
        edgeColor = pipelineRel.color;
        edgeWidth = 2.5;
      } else if (columnCount > 0) {
        edgeColor = '#1976d2';
        edgeWidth = 1.5;
      }
      
      const label = isPipelineEdge 
        ? `${pipelineRel.label} (${columnCount} cols)`
        : columnCount > 0 
          ? `${columnCount} columns` 
          : edge.relationship || 'feeds into';
      
      return {
        id: `${edge.source}->${edge.target}`,
        source: edge.source,
        target: edge.target,
        sourceHandle: null,
        targetHandle: null,
        type: 'smoothstep',
        animated: isPipelineEdge || columnCount > 0,
        markerEnd: {
          type: MarkerType.ArrowClosed,
          width: isPipelineEdge ? 16 : 12,
          height: isPipelineEdge ? 16 : 12,
          color: edgeColor,
        },
        style: {
          strokeWidth: edgeWidth,
          stroke: edgeColor,
          strokeDasharray: isPipelineEdge ? '0' : (columnCount > 0 ? '0' : '5,5'),
          opacity: isPipelineEdge ? 1 : 0.8,
        },
        label: label,
        labelStyle: { 
          fill: '#ffffff', 
          fontWeight: 600, 
          fontSize: isPipelineEdge ? 12 : 11,
          textShadow: '0 1px 2px rgba(0,0,0,0.3)'
        },
        labelBgStyle: { 
          fill: edgeColor, 
          fillOpacity: 0.9,
          padding: isPipelineEdge ? '6px 10px' : '4px 8px',
          borderRadius: '12px',
          stroke: '#ffffff',
          strokeWidth: 1
        },
        data: {
          column_lineage: edge.column_lineage || [],
          relationship: edge.relationship,
            extraction_method: edge.extraction_method,
            transformation_type: edge.transformation_type,
          onEdgeClick: handleEdgeClick,
        },
      };
    });

    setNodes(flowNodes);
    setEdges(flowEdges);
    
    // Store raw data for view mode switching (preserve existing rawData if it exists)
    setFullLineageData(prev => ({
      ...prev,
      nodes: flowNodes,
      edges: flowEdges,
      rawData: { nodes, edges }
    }));
      } else {
        
        const rawNodes = fullLineageData.rawData?.nodes || [];
        const rawEdges = fullLineageData.rawData?.edges || [];
        
        const relatedNodeIds = new Set([assetId]);
        const upstreamEdges = rawEdges.filter(e => e.target === assetId);
        upstreamEdges.forEach(edge => {
          relatedNodeIds.add(edge.source);
          const secondLevelUp = rawEdges.filter(e => e.target === edge.source);
          secondLevelUp.forEach(e2 => relatedNodeIds.add(e2.source));
        });
        const downstreamEdges = rawEdges.filter(e => e.source === assetId);
        downstreamEdges.forEach(edge => {
          relatedNodeIds.add(edge.target);
          const secondLevelDown = rawEdges.filter(e => e.source === edge.target);
          secondLevelDown.forEach(e2 => relatedNodeIds.add(e2.target));
        });
        let filteredNodes = rawNodes.filter(n => relatedNodeIds.has(n.id));
        let filteredEdges = rawEdges.filter(e => 
          relatedNodeIds.has(e.source) && relatedNodeIds.has(e.target)
        );
        
        // Apply lineage view mode filter
        if (lineageViewMode === 'actual') {
          filteredNodes = filteredNodes.filter(node => {
            const nodeId = node.id || '';
            return !nodeId.startsWith('container_') && !nodeId.startsWith('folder_');
          });
          
          filteredEdges = filteredEdges.filter(edge => {
            if (edge.type === 'contains' || edge.folder_based) {
              return false;
            }
            const sourceIsAsset = !edge.source.startsWith('container_') && !edge.source.startsWith('folder_');
            const targetIsAsset = !edge.target.startsWith('container_') && !edge.target.startsWith('folder_');
            return sourceIsAsset && targetIsAsset;
          });
        }
        
        const layoutedNodes = layoutNodes(filteredNodes, filteredEdges);
        
        // Determine upstream and downstream nodes relative to selected asset
        const upstreamNodeIds = new Set();
        const downstreamNodeIds = new Set();
        
        if (assetId) {
          // Find all upstream nodes (nodes that feed into the selected asset)
          const findUpstream = (nodeId, visited = new Set()) => {
            if (visited.has(nodeId)) return;
            visited.add(nodeId);
            filteredEdges.forEach(edge => {
              if (edge.target === nodeId) {
                upstreamNodeIds.add(edge.source);
                findUpstream(edge.source, visited);
              }
            });
          };
          
          // Find all downstream nodes (nodes that the selected asset feeds into)
          const findDownstream = (nodeId, visited = new Set()) => {
            if (visited.has(nodeId)) return;
            visited.add(nodeId);
            filteredEdges.forEach(edge => {
              if (edge.source === nodeId) {
                downstreamNodeIds.add(edge.target);
                findDownstream(edge.target, visited);
              }
            });
          };
          
          findUpstream(assetId);
          findDownstream(assetId);
        }
        
        const flowNodes = layoutedNodes.map((node) => ({
          id: node.id,
          type: 'custom',
          position: node.position,
          data: { 
            label: node.name, 
            name: node.name, 
            type: node.type, 
            catalog: node.catalog,
            isSelected: node.id === assetId,
            isUpstream: upstreamNodeIds.has(node.id),
            isDownstream: downstreamNodeIds.has(node.id),
            onNodeClick: handleNodeClick,
          }
        }));
        setNodes(flowNodes);
        setEdges(filteredEdges);
        
        // Store raw data for view mode switching
        setFullLineageData(prev => ({
          ...prev,
          nodes: flowNodes,
          edges: filteredEdges,
          rawData: { nodes: rawNodes, edges: rawEdges }
        }));
      }
    } catch (error) {
      console.error('Error fetching asset lineage:', error);
      setNodes([]);
      setEdges([]);
      setFullLineageData(prev => ({
        ...prev,
        nodes: [],
        edges: [],
        rawData: { nodes: [], edges: [] }
      }));
    } finally {
      setLoading(false);
    }
  };

  
  // Apply view mode filter first, then search/filter filters
  let baseFilteredNodes = selectedAssetForLineage ? nodes : [];
  let baseFilteredEdges = selectedAssetForLineage ? edges : [];
  
  // Apply lineage view mode filter
  if (selectedAssetForLineage && lineageViewMode === 'actual') {
    baseFilteredNodes = baseFilteredNodes.filter(node => {
      const nodeId = node.id || '';
      return !nodeId.startsWith('container_') && !nodeId.startsWith('folder_');
    });
    
    baseFilteredEdges = baseFilteredEdges.filter(edge => {
      if (edge.type === 'contains' || edge.folder_based) {
        return false;
      }
      const sourceIsAsset = !edge.source.startsWith('container_') && !edge.source.startsWith('folder_');
      const targetIsAsset = !edge.target.startsWith('container_') && !edge.target.startsWith('folder_');
      return sourceIsAsset && targetIsAsset;
    });
  }
  
  // Apply search and filter filters
  const filteredNodes = baseFilteredNodes.filter(node => {
    const matchesSearch = !searchTerm || 
      node.data.name.toLowerCase().includes(searchTerm.toLowerCase()) ||
      node.data.catalog.toLowerCase().includes(searchTerm.toLowerCase());
    const matchesType = filterType === 'all' || node.data.type === filterType;
    const matchesSource = filterSource === 'all' || node.data.source_system === filterSource;
    return matchesSearch && matchesType && matchesSource;
  });

  const filteredEdges = baseFilteredEdges.filter(edge => {
    const sourceExists = filteredNodes.find(n => n.id === edge.source);
    const targetExists = filteredNodes.find(n => n.id === edge.target);
    return sourceExists && targetExists;
  });

  
  const uniqueTypes = [...new Set(fullLineageData.rawData?.nodes?.map(n => n.type) || [])];
  const uniqueSources = [...new Set(fullLineageData.rawData?.nodes?.map(n => n.source_system) || [])];

  
  
  // Dropdown should reflect *all* loaded assets (not just the currently-rendered lineage nodes).
  const dropdownAssets = (assets || []).filter(asset => {
    if (!searchTerm) return true;
    const name = (asset.name || '').toLowerCase();
    const catalog = (asset.catalog || '').toLowerCase();
    const term = searchTerm.toLowerCase();
    return name.includes(term) || catalog.includes(term);
  });

  
  const filteredRawNodes = fullLineageData.rawData?.nodes?.filter(node => {
    const matchesSearch = !searchTerm || 
      node.name.toLowerCase().includes(searchTerm.toLowerCase()) ||
      node.catalog.toLowerCase().includes(searchTerm.toLowerCase());
    const matchesType = filterType === 'all' || node.type === filterType;
    const matchesSource = filterSource === 'all' || node.source_system === filterSource;
    return matchesSearch && matchesType && matchesSource;
  }) || [];



  return (
    <Box sx={{ minHeight: '120vh', p: 4, pb: 8 }}>
      {}
      <Box sx={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center', mb: 3 }}>
        <Box>
          <Typography variant="h4" component="h1" sx={{ fontWeight: 600, fontFamily: 'Comfortaa', display: 'flex', alignItems: 'center', gap: 1 }}>
            <AccountTree sx={{ fontSize: 40, color: '#8FA0F5' }} />
            Data Lineage
          </Typography>
          <Typography variant="body2" color="text.secondary" sx={{ mt: 0.5 }}>
            Visualize data flow and dependencies across your discovered assets
          </Typography>
          
          {/* Lineage extraction runs silently in background - no UI messages */}
        </Box>
        <Box sx={{ display: 'flex', gap: 2 }}>
          <Button
            variant="outlined"
            startIcon={<Add />}
            onClick={() => setManualLineageOpen(true)}
            sx={{ height: 40 }}
          >
            Manual Lineage
          </Button>
          <Button
            variant="contained"
            startIcon={<Refresh />}
            onClick={() => {
              fetchLineage();
            }}
            disabled={loading}
            sx={{ height: 40 }}
          >
            Refresh
          </Button>
        </Box>
      </Box>

      {}
      <Card sx={{ mb: 3 }}>
        <CardContent sx={{ py: 2 }}>
          <Grid container spacing={2} alignItems="center">
            <Grid item xs={12} md={5}>
              <Autocomplete
                options={dropdownAssets.map(a => {
                  let sourceSystem = 'Unknown';
                  if (a.connector_id) {
                    const parts = a.connector_id.split('_');
                    if (parts[0] === 'azure' && parts[1] === 'blob') {
                      sourceSystem = 'Azure Blob Storage';
                    } else if (parts[0]) {
                      sourceSystem = parts[0].charAt(0).toUpperCase() + parts[0].slice(1);
                    }
                  }
                  return ({
                    id: a.id,
                    label: `${a.name} (${a.type})`,
                    name: a.name,
                    type: a.type,
                    source: sourceSystem,
                  });
                })}
                value={dropdownAssets.find(n => n.id === selectedAssetForLineage) ? {
                  id: selectedAssetForLineage,
                  label: dropdownAssets.find(n => n.id === selectedAssetForLineage)?.name,
                } : null}
                onChange={(event, newValue) => {
                  handleAssetSelection(newValue ? newValue.id : null);
                  handleAssetDetailsSelection(newValue ? newValue.id : null);
                }}
                disabled={!dropdownAssets || dropdownAssets.length === 0}
                renderInput={(params) => (
                  <TextField
                    {...params}
                    size="small"
                    label={`Focus on Asset${(assets?.length || 0) > 0 ? (searchTerm ? ` (${dropdownAssets.length} / ${assets.length} match)` : ` (${assets.length} total)`) : ''}`}
                    placeholder="Select an asset to view its lineage..."
                    InputProps={{
                      ...params.InputProps,
                      startAdornment: (
                        <>
                          <InputAdornment position="start">
                            <AccountTree />
                          </InputAdornment>
                          {params.InputProps.startAdornment}
                        </>
                      ),
                    }}
                  />
                )}
                renderOption={(props, option) => (
                  <li {...props}>
                    <Box sx={{ display: 'flex', alignItems: 'center', gap: 1, width: '100%' }}>
                      <DataObject sx={{ fontSize: 18, color: '#666' }} />
                      <Box sx={{ flex: 1 }}>
                        <Typography variant="body2" sx={{ fontWeight: 600 }}>
                          {option.name}
                        </Typography>
                        <Typography variant="caption" color="text.secondary">
                          {option.type} • {option.source}
                        </Typography>
                      </Box>
                    </Box>
                  </li>
                )}
              />
            </Grid>
            <Grid item xs={12} md={2}>
              <TextField
                fullWidth
                size="small"
                placeholder="Search..."
                value={searchTerm}
                onChange={(e) => setSearchTerm(e.target.value)}
                InputProps={{
                  startAdornment: (
                    <InputAdornment position="start">
                      <Search />
                    </InputAdornment>
                  ),
                }}
              />
            </Grid>
            <Grid item xs={12} md={3}>
              <FormControl fullWidth size="small">
                <InputLabel>Type</InputLabel>
                <Select
                  value={filterType}
                  label="Type"
                  onChange={(e) => setFilterType(e.target.value)}
                >
                  <MenuItem value="all">All Types</MenuItem>
                  {uniqueTypes.map(type => (
                    <MenuItem key={type} value={type}>{type}</MenuItem>
                  ))}
                </Select>
              </FormControl>
            </Grid>
            <Grid item xs={12} md={3}>
              <FormControl fullWidth size="small">
                <InputLabel>Source</InputLabel>
                <Select
                  value={filterSource}
                  label="Source"
                  onChange={(e) => setFilterSource(e.target.value)}
                >
                  <MenuItem value="all">All Sources</MenuItem>
                  {uniqueSources.map(source => (
                    <MenuItem key={source} value={source}>{source}</MenuItem>
                  ))}
                </Select>
              </FormControl>
            </Grid>
            <Grid item xs={12} md={2}>
              <Button
                fullWidth
                variant="outlined"
                startIcon={<FilterList />}
                onClick={() => {
                  setSearchTerm('');
                  setFilterType('all');
                  setFilterSource('all');
                  handleAssetSelection(null);
                }}
              >
                Clear All
              </Button>
            </Grid>
            {selectedAssetForLineage && (
              <>
                <Grid item xs={12} md={2}>
                  <Button
                    fullWidth
                    variant={lineageViewMode === 'hierarchical' ? 'contained' : 'outlined'}
                    color={lineageViewMode === 'hierarchical' ? 'primary' : 'default'}
                    onClick={async () => {
                      setLineageViewMode('hierarchical');
                      // Reload lineage when switching modes
                      if (selectedAssetForLineage) {
                        await handleAssetSelection(selectedAssetForLineage);
                      }
                    }}
                    size="small"
                  >
                    Hierarchical
                  </Button>
                </Grid>
                <Grid item xs={12} md={2}>
                  <Button
                    fullWidth
                    variant={lineageViewMode === 'actual' ? 'contained' : 'outlined'}
                    color={lineageViewMode === 'actual' ? 'primary' : 'default'}
                    onClick={async () => {
                      setLineageViewMode('actual');
                      // Reload lineage when switching modes
                      if (selectedAssetForLineage) {
                        await handleAssetSelection(selectedAssetForLineage);
                      }
                    }}
                    size="small"
                  >
                    Dependency Lineage
                  </Button>
                </Grid>
              </>
            )}
          </Grid>
        </CardContent>
      </Card>

      {}
      <Card sx={{ position: 'relative', height: '700px', mb: 4 }}>
        {loading ? (
          <Box sx={{ display: 'flex', justifyContent: 'center', alignItems: 'center', height: '100%' }}>
            <CircularProgress />
          </Box>
        ) : !selectedAssetForLineage ? (
          <Box sx={{ 
            display: 'flex', 
            flexDirection: 'column', 
            alignItems: 'center', 
            justifyContent: 'center', 
            height: '100%',
            textAlign: 'center',
            color: 'text.secondary'
          }}>
            <AccountTree sx={{ fontSize: 80, color: '#ddd', mb: 2 }} />
            <Typography variant="h5" sx={{ mb: 1, fontWeight: 500 }}>
              Select asset to see lineage
                  </Typography>
                  <Typography variant="body2">
              Choose an asset from the dropdown above to view its data lineage graph
                  </Typography>
                  </Box>
        ) : filteredNodes.length === 0 ? (
          <Box sx={{ display: 'flex', justifyContent: 'center', alignItems: 'center', height: '100%', p: 4 }}>
            <Alert severity="info">
                  <Typography variant="body2">
                No lineage relationships found for this asset.
                  </Typography>
            </Alert>
          </Box>
        ) : (
          <ReactFlow
            nodes={filteredNodes}
            edges={filteredEdges}
            onNodesChange={onNodesChange}
            onEdgesChange={onEdgesChange}
            onEdgeClick={onEdgeClick}
            nodeTypes={nodeTypes}
            fitView
            attributionPosition="bottom-left"
          >
            <Background color="#aaa" gap={16} />
            <Controls />
            <MiniMap
              nodeColor={(node) => node.data.type === 'View' ? '#8FA0F5' : '#4caf50'}
              maskColor="rgba(0, 0, 0, 0.1)"
            />
            <Panel position="top-right">
              <Card sx={{ p: 2, backgroundColor: 'rgba(255, 255, 255, 0.95)', maxWidth: 250 }}>
                <Typography variant="body2" sx={{ fontWeight: 600, mb: 1.5 }}>
                  Legend
                </Typography>
                <Box sx={{ display: 'flex', flexDirection: 'column', gap: 1.5 }}>
                  <Box>
                    <Typography variant="caption" sx={{ fontWeight: 600, mb: 0.5, display: 'block' }}>
                      Pipeline Stages
                    </Typography>
                    <Box sx={{ display: 'flex', flexDirection: 'column', gap: 0.5, ml: 1 }}>
                      <Box sx={{ display: 'flex', alignItems: 'center', gap: 1 }}>
                        <Box sx={{ width: 16, height: 16, backgroundColor: '#9c27b015', border: '2px solid #9c27b0', borderRadius: 0.5 }} />
                        <Typography variant="caption">SOURCE</Typography>
                      </Box>
                      <Box sx={{ display: 'flex', alignItems: 'center', gap: 1 }}>
                        <Box sx={{ width: 16, height: 16, backgroundColor: '#ff980015', border: '2px solid #ff9800', borderRadius: 0.5 }} />
                        <Typography variant="caption">EXTRACT</Typography>
                      </Box>
                      <Box sx={{ display: 'flex', alignItems: 'center', gap: 1 }}>
                        <Box sx={{ width: 16, height: 16, backgroundColor: '#2196f315', border: '2px solid #2196f3', borderRadius: 0.5 }} />
                        <Typography variant="caption">TRANSFORM</Typography>
                      </Box>
                      <Box sx={{ display: 'flex', alignItems: 'center', gap: 1 }}>
                        <Box sx={{ width: 16, height: 16, backgroundColor: '#4caf5015', border: '2px solid #4caf50', borderRadius: 0.5 }} />
                        <Typography variant="caption">LOAD</Typography>
                      </Box>
                    </Box>
                  </Box>
                  {selectedAssetForLineage && (
                    <Box sx={{ borderTop: '1px solid #ddd', pt: 1 }}>
                      <Typography variant="caption" sx={{ fontWeight: 600, mb: 0.5, display: 'block' }}>
                        Lineage Direction
                      </Typography>
                      <Box sx={{ display: 'flex', flexDirection: 'column', gap: 0.5, ml: 1 }}>
                        <Box sx={{ display: 'flex', alignItems: 'center', gap: 1 }}>
                          <Box sx={{ width: 16, height: 16, backgroundColor: '#2196f315', border: '2px solid #2196f3', borderRadius: 0.5 }} />
                          <Typography variant="caption">Upstream (Source)</Typography>
                        </Box>
                        <Box sx={{ display: 'flex', alignItems: 'center', gap: 1 }}>
                          <Box sx={{ width: 16, height: 16, backgroundColor: '#4caf5015', border: '2px solid #4caf50', borderRadius: 0.5 }} />
                          <Typography variant="caption">Downstream (Target)</Typography>
                        </Box>
                      </Box>
                    </Box>
                  )}
                  <Box sx={{ borderTop: '1px solid #ddd', pt: 1 }}>
                    <Typography variant="caption" sx={{ fontWeight: 600, mb: 0.5, display: 'block' }}>
                      Node Types
                    </Typography>
                    <Box sx={{ display: 'flex', flexDirection: 'column', gap: 0.5, ml: 1 }}>
                      <Box sx={{ display: 'flex', alignItems: 'center', gap: 1 }}>
                        <Box sx={{ width: 16, height: 16, backgroundColor: '#ffffff', border: '1px solid #ddd', borderRadius: 0.5 }} />
                        <Typography variant="caption">Table</Typography>
                      </Box>
                      <Box sx={{ display: 'flex', alignItems: 'center', gap: 1 }}>
                        <Box sx={{ width: 16, height: 16, backgroundColor: '#ffffff', border: '1px solid #ddd', borderRadius: 0.5 }} />
                        <Typography variant="caption">View</Typography>
                      </Box>
                    </Box>
                  </Box>
                  <Box sx={{ borderTop: '1px solid #ddd', pt: 1 }}>
                    <Typography variant="caption" sx={{ fontWeight: 600, mb: 0.5, display: 'block' }}>
                      Edge Types
                    </Typography>
                    <Box sx={{ display: 'flex', flexDirection: 'column', gap: 0.5, ml: 1 }}>
                      <Box sx={{ display: 'flex', alignItems: 'center', gap: 1 }}>
                        <Box sx={{ width: 20, height: 2, backgroundColor: '#ff9800' }} />
                        <Typography variant="caption">EXTRACT Flow</Typography>
                      </Box>
                      <Box sx={{ display: 'flex', alignItems: 'center', gap: 1 }}>
                        <Box sx={{ width: 20, height: 2, backgroundColor: '#2196f3' }} />
                        <Typography variant="caption">TRANSFORM Flow</Typography>
                      </Box>
                      <Box sx={{ display: 'flex', alignItems: 'center', gap: 1 }}>
                        <Box sx={{ width: 20, height: 2, backgroundColor: '#4caf50' }} />
                        <Typography variant="caption">LOAD Flow</Typography>
                      </Box>
                      <Box sx={{ display: 'flex', alignItems: 'center', gap: 1 }}>
                        <Box sx={{ width: 20, height: 2, backgroundColor: '#1976d2' }} />
                        <Typography variant="caption">Column Flow</Typography>
                      </Box>
                      <Box sx={{ display: 'flex', alignItems: 'center', gap: 1 }}>
                        <Box sx={{ width: 20, height: 2, backgroundColor: '#64b5f6', backgroundImage: 'repeating-linear-gradient(90deg, #64b5f6 0px, #64b5f6 5px, transparent 5px, transparent 10px)' }} />
                        <Typography variant="caption">Data Flow</Typography>
                      </Box>
                    </Box>
                  </Box>
                </Box>
              </Card>
            </Panel>
          </ReactFlow>
        )}
      </Card>

      {}
      {selectedAssetDetails && (
        <Box sx={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center', mb: 3, mt: 4 }}>
          <Typography variant="h5" sx={{ fontWeight: 600, display: 'flex', alignItems: 'center', gap: 1 }}>
            <DataObject sx={{ fontSize: 28, color: '#666' }} />
            Asset Details
          </Typography>
          <Button
            variant="outlined"
            size="small"
            onClick={() => {
              setSelectedAssetDetails(null);
              setSelectedAssetForLineage(null);
              setNodes([]);
              setEdges([]);
              setActiveDetailTab('basic');
            }}
            startIcon={<Close />}
          >
            Clear Selection
          </Button>
        </Box>
      )}

      {}
      {!selectedAssetDetails && (
        <Box sx={{ 
          display: 'flex', 
          flexDirection: 'column', 
          alignItems: 'center', 
          justifyContent: 'center', 
          py: 8,
          textAlign: 'center',
          mt: 4
        }}>
          <DataObject sx={{ fontSize: 64, color: '#ddd', mb: 2 }} />
          <Typography variant="h5" color="text.secondary" sx={{ mb: 1 }}>
            No Asset Selected
                  </Typography>
          <Typography variant="body1" color="text.secondary">
            Select an asset from the dropdown above to view its details and lineage
          </Typography>
                  </Box>
      )}

      {}
      {selectedAssetDetails && (
        <Card sx={{ mb: 4, minHeight: '400px' }}>
          <Box sx={{ borderBottom: 1, borderColor: 'divider' }}>
            <Tabs 
              value={activeDetailTab} 
              onChange={(e, newValue) => setActiveDetailTab(newValue)}
              variant="scrollable"
              scrollButtons="auto"
            >
              <Tab 
                label="Basic Information" 
                value="basic" 
                icon={<DataObject />}
                iconPosition="start"
                sx={{ textTransform: 'none', fontWeight: 500 }}
              />
              <Tab 
                label="Column Information" 
                value="columns" 
                icon={<TableChart />}
                iconPosition="start"
                sx={{ textTransform: 'none', fontWeight: 500 }}
              />
              <Tab 
                label="Lineage Information" 
                value="lineage" 
                icon={<AccountTree />}
                iconPosition="start"
                sx={{ textTransform: 'none', fontWeight: 500 }}
              />
              <Tab 
                label="Metadata" 
                value="metadata" 
                icon={<Info />}
                iconPosition="start"
                sx={{ textTransform: 'none', fontWeight: 500 }}
              />
              <Tab 
                label="Data Quality" 
                value="quality" 
                icon={<FilterList />}
                iconPosition="start"
                sx={{ textTransform: 'none', fontWeight: 500 }}
              />
              {selectedAssetDetails?.pipeline_metadata?.is_pipeline_asset && (
                <Tab 
                  label="Pipeline Summary" 
                  value="pipeline" 
                  icon={<AccountTree />}
                  iconPosition="start"
                  sx={{ textTransform: 'none', fontWeight: 500 }}
                />
              )}
            </Tabs>
                </Box>
          
          <CardContent sx={{ p: 4, minHeight: '350px', overflow: 'auto' }}>
            {}
            {activeDetailTab === 'basic' && (
              <Box sx={{ display: 'flex', flexDirection: 'column', gap: 3 }}>
                <Box>
                  <Typography variant="body2" color="text.secondary" sx={{ fontSize: 12, fontWeight: 500, mb: 0.5 }}>
                    Asset Name
                    </Typography>
                  <Typography variant="h5" sx={{ fontWeight: 600, color: '#333' }}>
                    {selectedAssetDetails.name}
                    </Typography>
                </Box>
                <Box>
                  <Typography variant="body2" color="text.secondary" sx={{ fontSize: 12, fontWeight: 500, mb: 0.5 }}>
                      Asset ID
                    </Typography>
                  <Typography variant="body2" sx={{ fontFamily: 'monospace', fontSize: 12, wordBreak: 'break-all', color: '#666' }}>
                    {selectedAssetDetails.id}
                    </Typography>
                </Box>
                <Box sx={{ display: 'flex', gap: 2, flexWrap: 'wrap' }}>
                  <Chip 
                    label={selectedAssetDetails.type} 
                    size="medium" 
                    variant="outlined"
                    sx={{ height: 32, fontSize: 13, borderColor: '#ccc', color: '#666', fontWeight: 500 }}
                  />
                  <Chip 
                    label={selectedAssetDetails.source_system || selectedAssetDetails.technical_metadata?.source_system || selectedAssetDetails.connector_id || 'Azure Blob Storage'} 
                    size="medium" 
                    variant="outlined"
                    sx={{ height: 32, fontSize: 13, borderColor: '#ccc', color: '#666', fontWeight: 500 }}
                  />
                </Box>
                <Box>
                  <Typography variant="body2" color="text.secondary" sx={{ fontSize: 12, fontWeight: 500, mb: 0.5 }}>
                    Catalog
                    </Typography>
                  <Typography variant="body2" sx={{ fontFamily: 'monospace', fontSize: 12, color: '#666' }}>
                    {selectedAssetDetails.catalog || 'N/A'}
                      </Typography>
                </Box>
              </Box>
            )}

            {}
            {activeDetailTab === 'columns' && (
              <Box>
                <Typography variant="h6" sx={{ fontWeight: 600, mb: 3, color: '#333' }}>
                  Column Information ({selectedAssetDetails.columns?.length || 0} columns)
                </Typography>
                {selectedAssetDetails.columns && selectedAssetDetails.columns.length > 0 ? (
                      <TableContainer component={Paper} variant="outlined">
                    <Table>
                          <TableHead>
                            <TableRow>
                          <TableCell sx={{ fontWeight: 600, backgroundColor: '#f5f5f5' }}>Name</TableCell>
                          <TableCell sx={{ fontWeight: 600, backgroundColor: '#f5f5f5' }}>Type</TableCell>
                          <TableCell sx={{ fontWeight: 600, backgroundColor: '#f5f5f5' }}>PII Status</TableCell>
                          <TableCell sx={{ fontWeight: 600, backgroundColor: '#f5f5f5' }}>Description</TableCell>
                          <TableCell sx={{ fontWeight: 600, backgroundColor: '#f5f5f5' }}>Constraints</TableCell>
                            </TableRow>
                          </TableHead>
                          <TableBody>
                        {selectedAssetDetails.columns.map((col, index) => {
                          
                          const isPII = detectPII(col.name, col.description);
                          return (
                              <TableRow key={index}>
                              <TableCell sx={{ fontFamily: 'monospace', fontWeight: 500 }}>
                                {col.name}
                              </TableCell>
                                <TableCell>
                                <Chip 
                                  label={col.type} 
                                  size="small" 
                                  variant="outlined"
                                  sx={{ borderColor: '#ddd', color: '#666' }}
                                />
                                </TableCell>
                              <TableCell>
                                <Box sx={{ display: 'flex', alignItems: 'center', gap: 1 }}>
                                  {(col.pii_detected !== undefined ? col.pii_detected : isPII) ? (
                                    <Chip 
                                      label="PII" 
                                      size="small" 
                                      color="error"
                                      sx={{ fontWeight: 600, cursor: 'pointer' }}
                                      onClick={async () => {
                                        try {
                                          const API_BASE_URL = import.meta.env.VITE_API_BASE_URL;
                                          const response = await fetch(
                                            `${API_BASE_URL}/api/assets/${selectedAssetDetails.id}/columns/${col.name}/pii`,
                                            {
                                              method: 'PUT',
                                              headers: { 'Content-Type': 'application/json' },
                                              body: JSON.stringify({
                                                pii_detected: false,
                                                pii_types: null
                                              })
                                            }
                                          );
                                          if (response.ok) {
                                            const updatedAsset = { ...selectedAssetDetails };
                                            updatedAsset.columns = updatedAsset.columns.map(c => 
                                              c.name === col.name 
                                                ? { ...c, pii_detected: false, pii_types: null }
                                                : c
                                            );
                                            setSelectedAssetDetails(updatedAsset);
                                          }
                                        } catch (err) {
                                          console.error('Failed to update PII status:', err);
                                        }
                                      }}
                                    />
                                  ) : (
                                    <Chip 
                                      label="Safe" 
                                      size="small" 
                                      color="success"
                                      variant="outlined"
                                      sx={{ fontWeight: 500, cursor: 'pointer' }}
                                      onClick={async () => {
                                        try {
                                          const API_BASE_URL = import.meta.env.VITE_API_BASE_URL;
                                          const response = await fetch(
                                            `${API_BASE_URL}/api/assets/${selectedAssetDetails.id}/columns/${col.name}/pii`,
                                            {
                                              method: 'PUT',
                                              headers: { 'Content-Type': 'application/json' },
                                              body: JSON.stringify({
                                                pii_detected: true,
                                                pii_types: col.pii_types || ['PII']
                                              })
                                            }
                                          );
                                          if (response.ok) {
                                            const updatedAsset = { ...selectedAssetDetails };
                                            updatedAsset.columns = updatedAsset.columns.map(c => 
                                              c.name === col.name 
                                                ? { ...c, pii_detected: true, pii_types: c.pii_types || ['PII'] }
                                                : c
                                            );
                                            setSelectedAssetDetails(updatedAsset);
                                          }
                                        } catch (err) {
                                          console.error('Failed to update PII status:', err);
                                        }
                                      }}
                                    />
                                  )}
                                </Box>
                              </TableCell>
                              <TableCell sx={{ color: '#666' }}>
                                {col.description || '-'}
                              </TableCell>
                              <TableCell>
                                <Box sx={{ display: 'flex', gap: 0.5, flexWrap: 'wrap' }}>
                                  {col.nullable === false && (
                                    <Chip 
                                      label="NOT NULL" 
                                      size="small" 
                                      variant="outlined"
                                      sx={{ fontSize: 9, height: 20, borderColor: '#ff9800', color: '#ff9800' }}
                                    />
                                  )}
                                  {col.unique && (
                                    <Chip 
                                      label="UNIQUE" 
                                      size="small" 
                                      variant="outlined"
                                      sx={{ fontSize: 9, height: 20, borderColor: '#2196f3', color: '#2196f3' }}
                                    />
                                  )}
                                  {col.primary_key && (
                                    <Chip 
                                      label="PK" 
                                      size="small" 
                                      color="primary"
                                      sx={{ fontSize: 9, height: 20 }}
                                    />
                                  )}
                                </Box>
                              </TableCell>
                              </TableRow>
                          );
                        })}
                          </TableBody>
                        </Table>
                      </TableContainer>
                ) : (
                  <Typography variant="body2" color="text.secondary">
                    No column information available
                  </Typography>
                )}
              </Box>
            )}

            {}
            {activeDetailTab === 'lineage' && (
              <Box>
                <Typography variant="h6" sx={{ fontWeight: 600, mb: 3, color: '#333' }}>
                  Lineage Information
                </Typography>
                <Grid container spacing={3}>
                  <Grid item xs={12} md={6}>
                    <Typography variant="subtitle1" sx={{ fontWeight: 600, mb: 2, color: '#333' }}>
                      Upstream Dependencies
                    </Typography>
                    {fullLineageData.rawData?.edges?.filter(e => e.target === selectedAssetDetails.id).length > 0 ? (
                      <Box sx={{ display: 'flex', flexDirection: 'column', gap: 1 }}>
                        {fullLineageData.rawData.edges
                          .filter(e => e.target === selectedAssetDetails.id)
                          .map((edge, index) => {
                            const sourceNode = fullLineageData.rawData.nodes.find(n => n.id === edge.source);
                            return (
                              <Box key={index} sx={{ display: 'flex', alignItems: 'center', gap: 1, p: 2, backgroundColor: '#f9f9f9', borderRadius: 1 }}>
                                <Typography variant="body2" sx={{ fontFamily: 'monospace', fontWeight: 500, flex: 1 }}>
                                  {sourceNode?.name || edge.source}
                                </Typography>
                                <Chip 
                                  label={`${edge.column_lineage?.length || 0} cols`}
                                  size="small"
                                  variant="outlined"
                                  sx={{ borderColor: '#ddd', color: '#666' }}
                                />
                              </Box>
                            );
                          })}
                      </Box>
                    ) : (
                      <Typography variant="body2" color="text.secondary" sx={{ fontStyle: 'italic' }}>
                        No upstream dependencies
                        </Typography>
                      )}
                    </Grid>
                  <Grid item xs={12} md={6}>
                    <Typography variant="subtitle1" sx={{ fontWeight: 600, mb: 2, color: '#333' }}>
                      Downstream Dependencies
                    </Typography>
                    {fullLineageData.rawData?.edges?.filter(e => e.source === selectedAssetDetails.id).length > 0 ? (
                      <Box sx={{ display: 'flex', flexDirection: 'column', gap: 1 }}>
                        {fullLineageData.rawData.edges
                          .filter(e => e.source === selectedAssetDetails.id)
                          .map((edge, index) => {
                            const targetNode = fullLineageData.rawData.nodes.find(n => n.id === edge.target);
                            return (
                              <Box key={index} sx={{ display: 'flex', alignItems: 'center', gap: 1, p: 2, backgroundColor: '#f9f9f9', borderRadius: 1 }}>
                                <Typography variant="body2" sx={{ fontFamily: 'monospace', fontWeight: 500, flex: 1 }}>
                                  {targetNode?.name || edge.target}
                                </Typography>
                                <Chip 
                                  label={`${edge.column_lineage?.length || 0} cols`}
                                  size="small"
                                  variant="outlined"
                                  sx={{ borderColor: '#ddd', color: '#666' }}
                                />
                              </Box>
                            );
                          })}
                      </Box>
                    ) : (
                      <Typography variant="body2" color="text.secondary" sx={{ fontStyle: 'italic' }}>
                        No downstream dependencies
                      </Typography>
                  )}
                </Grid>
                </Grid>
              </Box>
            )}

            {}
            {activeDetailTab === 'metadata' && (
              <Box>
                <Typography variant="h6" sx={{ fontWeight: 600, mb: 4, color: '#333' }}>
                  Metadata Information
                </Typography>
                
                {}
                <Grid container spacing={3} sx={{ mb: 4 }}>
                  <Grid item xs={12} md={6}>
                    <Box sx={{ p: 3, border: '1px solid #e0e0e0', borderRadius: 2, backgroundColor: '#fafafa' }}>
                      <Typography variant="subtitle1" sx={{ fontWeight: 600, mb: 2, color: '#333' }}>
                        Connection Details
                      </Typography>
                      <Box sx={{ display: 'flex', flexDirection: 'column', gap: 2 }}>
                        <Box>
                          <Typography variant="body2" color="text.secondary" sx={{ fontSize: 12, fontWeight: 500, mb: 0.5 }}>
                            Connector ID
                          </Typography>
                          <Typography variant="body2" sx={{ fontFamily: 'monospace', fontSize: 12, color: '#333', wordBreak: 'break-all' }}>
                            {selectedAssetDetails.connector_id || 'N/A'}
                          </Typography>
                        </Box>
                        <Box>
                          <Typography variant="body2" color="text.secondary" sx={{ fontSize: 12, fontWeight: 500, mb: 0.5 }}>
                            Source System
                          </Typography>
                          <Typography variant="body2" sx={{ fontSize: 12, color: '#333' }}>
                            {selectedAssetDetails.source_system || selectedAssetDetails.technical_metadata?.source_system || selectedAssetDetails.connector_id || 'Azure Blob Storage'}
                          </Typography>
                        </Box>
                        <Box>
                          <Typography variant="body2" color="text.secondary" sx={{ fontSize: 12, fontWeight: 500, mb: 0.5 }}>
                            Status
                          </Typography>
                          <Chip 
                            label="Active" 
                            size="small" 
                            variant="outlined"
                            sx={{ height: 24, fontSize: 11, borderColor: '#4caf50', color: '#4caf50' }}
                          />
                        </Box>
                      </Box>
                    </Box>
                  </Grid>
                  
                  <Grid item xs={12} md={6}>
                    <Box sx={{ p: 3, border: '1px solid #e0e0e0', borderRadius: 2, backgroundColor: '#fafafa' }}>
                      <Typography variant="subtitle1" sx={{ fontWeight: 600, mb: 2, color: '#333' }}>
                        Discovery Information
                      </Typography>
                      <Box sx={{ display: 'flex', flexDirection: 'column', gap: 2 }}>
                        <Box>
                          <Typography variant="body2" color="text.secondary" sx={{ fontSize: 12, fontWeight: 500, mb: 0.5 }}>
                            Last Discovered
                          </Typography>
                          <Typography variant="body2" sx={{ fontSize: 12, color: '#333' }}>
                            {new Date().toLocaleString()}
                          </Typography>
                        </Box>
                        <Box>
                          <Typography variant="body2" color="text.secondary" sx={{ fontSize: 12, fontWeight: 500, mb: 0.5 }}>
                            Last Modified
                          </Typography>
                          <Typography variant="body2" sx={{ fontSize: 12, color: '#333' }}>
                            {selectedAssetDetails.last_modified ? new Date(selectedAssetDetails.last_modified).toLocaleString() : 'Unknown'}
                          </Typography>
                        </Box>
                        <Box>
                          <Typography variant="body2" color="text.secondary" sx={{ fontSize: 12, fontWeight: 500, mb: 0.5 }}>
                            Owner
                          </Typography>
                          <Typography variant="body2" sx={{ fontSize: 12, color: '#333' }}>
                            {selectedAssetDetails.owner || 'Unassigned'}
                          </Typography>
                        </Box>
                      </Box>
                    </Box>
                  </Grid>
                </Grid>

                {}
                <Box sx={{ mb: 4 }}>
                  <Typography variant="subtitle1" sx={{ fontWeight: 600, mb: 3, color: '#333' }}>
                    Technical Details
                  </Typography>
                  <Grid container spacing={3}>
                    <Grid item xs={12} md={6}>
                      <Box sx={{ p: 2, border: '1px solid #e0e0e0', borderRadius: 1, textAlign: 'center' }}>
                        <Typography variant="body2" color="text.secondary" sx={{ fontSize: 11, fontWeight: 500, mb: 1 }}>
                          Schema
                        </Typography>
                        <Typography variant="body2" sx={{ fontFamily: 'monospace', fontSize: 12, color: '#333' }}>
                          {selectedAssetDetails.schema || selectedAssetDetails.catalog || 'N/A'}
                        </Typography>
                      </Box>
                    </Grid>
                    <Grid item xs={12} md={6}>
                      <Box sx={{ p: 2, border: '1px solid #e0e0e0', borderRadius: 1, textAlign: 'center' }}>
                        <Typography variant="body2" color="text.secondary" sx={{ fontSize: 11, fontWeight: 500, mb: 1 }}>
                          Database
                        </Typography>
                        <Typography variant="body2" sx={{ fontSize: 12, color: '#333' }}>
                          {selectedAssetDetails.database || selectedAssetDetails.source_system || selectedAssetDetails.technical_metadata?.source_system || 'N/A'}
                        </Typography>
                      </Box>
                    </Grid>
                    <Grid item xs={12} md={6}>
                      <Box sx={{ p: 2, border: '1px solid #e0e0e0', borderRadius: 1, textAlign: 'center' }}>
                        <Typography variant="body2" color="text.secondary" sx={{ fontSize: 11, fontWeight: 500, mb: 1 }}>
                          Row Count
                        </Typography>
                        <Typography variant="body2" sx={{ fontSize: 12, color: '#333' }}>
                          {selectedAssetDetails.row_count ? selectedAssetDetails.row_count.toLocaleString() : 'Unknown'}
                        </Typography>
                      </Box>
                    </Grid>
                    <Grid item xs={12} md={6}>
                      <Box sx={{ p: 2, border: '1px solid #e0e0e0', borderRadius: 1, textAlign: 'center' }}>
                        <Typography variant="body2" color="text.secondary" sx={{ fontSize: 11, fontWeight: 500, mb: 1 }}>
                          Size
                        </Typography>
                        <Typography variant="body2" sx={{ fontSize: 12, color: '#333' }}>
                          {selectedAssetDetails.size ? `${(selectedAssetDetails.size / 1024 / 1024).toFixed(2)} MB` : 'Unknown'}
                        </Typography>
                      </Box>
                    </Grid>
                  </Grid>
                </Box>

                {}
                <Box sx={{ mb: 4 }}>
                  <Typography variant="subtitle1" sx={{ fontWeight: 600, mb: 3, color: '#333' }}>
                    Data Governance
                  </Typography>
                  <Grid container spacing={3}>
                    <Grid item xs={12} md={4}>
                      <Box sx={{ p: 2, border: '1px solid #e0e0e0', borderRadius: 1, textAlign: 'center' }}>
                        <Typography variant="body2" color="text.secondary" sx={{ fontSize: 11, fontWeight: 500, mb: 1 }}>
                          Classification
                        </Typography>
                        <Chip 
                          label={selectedAssetDetails.business_metadata?.classification || selectedAssetDetails.classification || 'Unclassified'} 
                          size="small" 
                          variant="outlined"
                          sx={{ height: 24, fontSize: 11, borderColor: '#666', color: '#666' }}
                        />
                      </Box>
                    </Grid>
                    <Grid item xs={12} md={4}>
                      <Box sx={{ p: 2, border: '1px solid #e0e0e0', borderRadius: 1, textAlign: 'center' }}>
                        <Typography variant="body2" color="text.secondary" sx={{ fontSize: 11, fontWeight: 500, mb: 1 }}>
                          Retention Policy
                        </Typography>
                        <Typography variant="body2" sx={{ fontSize: 12, color: '#333' }}>
                          {selectedAssetDetails.business_metadata?.retention_policy || 
                           selectedAssetDetails.operational_metadata?.retention_policy || 
                           selectedAssetDetails.retention_policy || 
                           'No policy defined'}
                        </Typography>
                      </Box>
                    </Grid>
                    <Grid item xs={12} md={4}>
                      <Box sx={{ p: 2, border: '1px solid #e0e0e0', borderRadius: 1, textAlign: 'center' }}>
                        <Typography variant="body2" color="text.secondary" sx={{ fontSize: 11, fontWeight: 500, mb: 1 }}>
                          Data Quality
                        </Typography>
                        <Typography variant="body2" sx={{ fontSize: 12, color: '#333' }}>
                          {(() => {
                            // Calculate data quality from columns with descriptions
                            if (selectedAssetDetails.columns && selectedAssetDetails.columns.length > 0) {
                              const columnsWithDesc = selectedAssetDetails.columns.filter(
                                col => col.description && col.description !== '-' && col.description.trim() !== ''
                              ).length;
                              return `${Math.round((columnsWithDesc / selectedAssetDetails.columns.length) * 100)}% Complete`;
                            }
                            // Check if data quality score exists in operational metadata
                            if (selectedAssetDetails.operational_metadata?.data_quality_score !== undefined) {
                              return `${Math.round(selectedAssetDetails.operational_metadata.data_quality_score)}% Complete`;
                            }
                            // Check discovery data quality score
                            if (selectedAssetDetails.data_quality_score !== undefined) {
                              return `${Math.round(selectedAssetDetails.data_quality_score)}% Complete`;
                            }
                            return '0% Complete';
                          })()}
                        </Typography>
                      </Box>
                    </Grid>
                  </Grid>
                </Box>

                {}
                <Box>
                  <Typography variant="subtitle1" sx={{ fontWeight: 600, mb: 3, color: '#333' }}>
                    PII Analysis Summary
                  </Typography>
                  <Grid container spacing={3}>
                    <Grid item xs={12} md={4}>
                      <Box sx={{ textAlign: 'center', p: 3, border: '1px solid #e0e0e0', borderRadius: 2, backgroundColor: '#fafafa' }}>
                        <Typography variant="h4" sx={{ fontWeight: 600, color: '#333', mb: 1 }}>
                          {selectedAssetDetails.columns ? selectedAssetDetails.columns.filter(col => detectPII(col.name, col.description)).length : 0}
                        </Typography>
                        <Typography variant="body2" color="text.secondary" sx={{ fontSize: 12, fontWeight: 500 }}>
                          PII Columns
                        </Typography>
                      </Box>
                    </Grid>
                    <Grid item xs={12} md={4}>
                      <Box sx={{ textAlign: 'center', p: 3, border: '1px solid #e0e0e0', borderRadius: 2, backgroundColor: '#fafafa' }}>
                        <Typography variant="h4" sx={{ fontWeight: 600, color: '#333', mb: 1 }}>
                          {selectedAssetDetails.columns ? selectedAssetDetails.columns.length - selectedAssetDetails.columns.filter(col => detectPII(col.name, col.description)).length : 0}
                        </Typography>
                        <Typography variant="body2" color="text.secondary" sx={{ fontSize: 12, fontWeight: 500 }}>
                          Safe Columns
                        </Typography>
                      </Box>
                    </Grid>
                    <Grid item xs={12} md={4}>
                      <Box sx={{ textAlign: 'center', p: 3, border: '1px solid #e0e0e0', borderRadius: 2, backgroundColor: '#fafafa' }}>
                        <Typography variant="h4" sx={{ fontWeight: 600, color: '#333', mb: 1 }}>
                          {selectedAssetDetails.columns ? Math.round((selectedAssetDetails.columns.filter(col => detectPII(col.name, col.description)).length / selectedAssetDetails.columns.length) * 100) : 0}%
                        </Typography>
                        <Typography variant="body2" color="text.secondary" sx={{ fontSize: 12, fontWeight: 500 }}>
                          PII Percentage
                        </Typography>
                      </Box>
                    </Grid>
                  </Grid>
                </Box>
              </Box>
            )}

            {}
            {activeDetailTab === 'quality' && (
              <Box>
                <Typography variant="h6" sx={{ fontWeight: 600, mb: 3, color: '#333' }}>
                  Data Quality Metrics
                </Typography>
                <Grid container spacing={3}>
                  <Grid item xs={12} md={4}>
                    <Box sx={{ textAlign: 'center', p: 2, backgroundColor: '#f9f9f9', borderRadius: 1 }}>
                      <Typography variant="body2" color="text.secondary" sx={{ fontSize: 12, fontWeight: 500, mb: 1 }}>
                        Column Completeness
                      </Typography>
                      <Typography variant="h4" sx={{ fontWeight: 600, color: '#4caf50' }}>
                        {selectedAssetDetails.columns ? Math.round((selectedAssetDetails.columns.filter(col => col.description && col.description !== '-').length / selectedAssetDetails.columns.length) * 100) : 0}%
                      </Typography>
                    </Box>
                  </Grid>
                  <Grid item xs={12} md={4}>
                    <Box sx={{ textAlign: 'center', p: 2, backgroundColor: '#f9f9f9', borderRadius: 1 }}>
                      <Typography variant="body2" color="text.secondary" sx={{ fontSize: 12, fontWeight: 500, mb: 1 }}>
                        Lineage Coverage
                      </Typography>
                      <Typography variant="h4" sx={{ fontWeight: 600, color: '#1976d2' }}>
                        {fullLineageData.rawData?.edges?.filter(e => e.source === selectedAssetDetails.id || e.target === selectedAssetDetails.id).length || 0}
                      </Typography>
                      <Typography variant="body2" color="text.secondary" sx={{ fontSize: 11 }}>
                        connections
                      </Typography>
                    </Box>
                  </Grid>
                  <Grid item xs={12} md={4}>
                    <Box sx={{ textAlign: 'center', p: 2, backgroundColor: '#f9f9f9', borderRadius: 1 }}>
                      <Typography variant="body2" color="text.secondary" sx={{ fontSize: 12, fontWeight: 500, mb: 1 }}>
                        Data Types
                      </Typography>
                      <Typography variant="h4" sx={{ fontWeight: 600, color: '#ff9800' }}>
                        {selectedAssetDetails.columns ? [...new Set(selectedAssetDetails.columns.map(col => col.type))].length : 0}
                      </Typography>
                      <Typography variant="body2" color="text.secondary" sx={{ fontSize: 11 }}>
                        unique types
                      </Typography>
                    </Box>
                  </Grid>
                  <Grid item xs={12}>
                    <Box>
                      <Typography variant="body2" color="text.secondary" sx={{ fontSize: 12, fontWeight: 500, mb: 1 }}>
                        Data Types Used
                      </Typography>
                      <Box sx={{ display: 'flex', gap: 1, flexWrap: 'wrap' }}>
                        {selectedAssetDetails.columns ? 
                          [...new Set(selectedAssetDetails.columns.map(col => col.type))].map((type, index) => (
                            <Chip 
                              key={index}
                              label={type} 
                              size="small" 
                              variant="outlined"
                              sx={{ borderColor: '#ddd', color: '#666' }}
                            />
                          )) : null
                        }
                      </Box>
                    </Box>
                  </Grid>
                </Grid>
              </Box>
            )}

            {}
            {activeDetailTab === 'pipeline' && selectedAssetDetails?.pipeline_metadata?.is_pipeline_asset && (
              <Box>
                <Typography variant="h6" sx={{ fontWeight: 600, mb: 3, color: '#333' }}>
                  Pipeline Summary
                </Typography>
                
                {}
                <Box sx={{ mb: 4 }}>
                  <Card sx={{ bgcolor: '#f5f5f5', border: '2px solid #1976d2', borderRadius: 2 }}>
                    <CardContent>
                      <Grid container spacing={3}>
                        <Grid item xs={12} md={6}>
                          <Box>
                            <Typography variant="body2" color="text.secondary" sx={{ fontSize: 12, fontWeight: 500, mb: 1 }}>
                              Pipeline Name
                            </Typography>
                            <Typography variant="h6" sx={{ fontWeight: 600, color: '#1976d2' }}>
                              {selectedAssetDetails.pipeline_metadata.pipeline_name || 'ETL/ELT Pipeline'}
                            </Typography>
                          </Box>
                        </Grid>
                        <Grid item xs={12} md={6}>
                          <Box>
                            <Typography variant="body2" color="text.secondary" sx={{ fontSize: 12, fontWeight: 500, mb: 1 }}>
                              Pipeline Type
                            </Typography>
                            <Chip 
                              label={selectedAssetDetails.pipeline_metadata.pipeline_type || 'ETL'} 
                              color="primary" 
                              sx={{ fontWeight: 600, fontSize: 14 }}
                            />
                          </Box>
                        </Grid>
                        <Grid item xs={12} md={6}>
                          <Box>
                            <Typography variant="body2" color="text.secondary" sx={{ fontSize: 12, fontWeight: 500, mb: 1 }}>
                              Current Stage
                            </Typography>
                            <Chip 
                              label={selectedAssetDetails.pipeline_metadata.pipeline_stage?.toUpperCase() || 'UNKNOWN'} 
                              color="success" 
                              sx={{ fontWeight: 600, fontSize: 13 }}
                            />
                          </Box>
                        </Grid>
                        <Grid item xs={12} md={6}>
                          <Box>
                            <Typography variant="body2" color="text.secondary" sx={{ fontSize: 12, fontWeight: 500, mb: 1 }}>
                              Total Pipeline Stages
                            </Typography>
                            <Typography variant="h6" sx={{ fontWeight: 600 }}>
                              {selectedAssetDetails.pipeline_metadata.total_stages || 3}
                            </Typography>
                          </Box>
                        </Grid>
                        <Grid item xs={12}>
                          <Box>
                            <Typography variant="body2" color="text.secondary" sx={{ fontSize: 12, fontWeight: 500, mb: 1 }}>
                              Description
                            </Typography>
                            <Typography variant="body2" sx={{ color: '#666' }}>
                              {selectedAssetDetails.pipeline_metadata.pipeline_description || 'Part of ETL/ELT data pipeline'}
                            </Typography>
                          </Box>
                        </Grid>
                      </Grid>
                    </CardContent>
                  </Card>
                </Box>

                {}
                <Box sx={{ mb: 4 }}>
                  <Typography variant="subtitle1" sx={{ fontWeight: 600, mb: 3, color: '#333' }}>
                    Pipeline Flow
                  </Typography>
                  <Box sx={{ display: 'flex', alignItems: 'center', justifyContent: 'center', gap: 2, flexWrap: 'wrap', mb: 3 }}>
                    {}
                    <Box sx={{ 
                      p: 2, 
                      borderRadius: 2, 
                      border: selectedAssetDetails.pipeline_metadata.pipeline_stage === 'extract' ? '3px solid #1976d2' : '1px solid #ddd',
                      bgcolor: selectedAssetDetails.pipeline_metadata.pipeline_stage === 'extract' ? '#e3f2fd' : '#f9f9f9',
                      minWidth: 150,
                      textAlign: 'center'
                    }}>
                      <Typography variant="h6" sx={{ fontWeight: 600, mb: 1, color: selectedAssetDetails.pipeline_metadata.pipeline_stage === 'extract' ? '#1976d2' : '#666' }}>
                        1. EXTRACT
                      </Typography>
                      <Typography variant="caption" color="text.secondary">
                        Source → Staging
                      </Typography>
                      {selectedAssetDetails.pipeline_metadata.pipeline_stage === 'extract' && (
                        <Chip label="Current" size="small" color="primary" sx={{ mt: 1 }} />
                      )}
                    </Box>
                    
                    <ArrowForward sx={{ color: '#999', fontSize: 32 }} />
                    
                    {}
                    <Box sx={{ 
                      p: 2, 
                      borderRadius: 2, 
                      border: selectedAssetDetails.pipeline_metadata.pipeline_stage === 'transform' ? '3px solid #1976d2' : '1px solid #ddd',
                      bgcolor: selectedAssetDetails.pipeline_metadata.pipeline_stage === 'transform' ? '#e3f2fd' : '#f9f9f9',
                      minWidth: 150,
                      textAlign: 'center'
                    }}>
                      <Typography variant="h6" sx={{ fontWeight: 600, mb: 1, color: selectedAssetDetails.pipeline_metadata.pipeline_stage === 'transform' ? '#1976d2' : '#666' }}>
                        2. TRANSFORM
                      </Typography>
                      <Typography variant="caption" color="text.secondary">
                        Staging → Warehouse
                      </Typography>
                      {selectedAssetDetails.pipeline_metadata.pipeline_stage === 'transform' && (
                        <Chip label="Current" size="small" color="primary" sx={{ mt: 1 }} />
                      )}
                    </Box>
                    
                    <ArrowForward sx={{ color: '#999', fontSize: 32 }} />
                    
                    {}
                    <Box sx={{ 
                      p: 2, 
                      borderRadius: 2, 
                      border: selectedAssetDetails.pipeline_metadata.pipeline_stage === 'load' ? '3px solid #1976d2' : '1px solid #ddd',
                      bgcolor: selectedAssetDetails.pipeline_metadata.pipeline_stage === 'load' ? '#e3f2fd' : '#f9f9f9',
                      minWidth: 150,
                      textAlign: 'center'
                    }}>
                      <Typography variant="h6" sx={{ fontWeight: 600, mb: 1, color: selectedAssetDetails.pipeline_metadata.pipeline_stage === 'load' ? '#1976d2' : '#666' }}>
                        3. LOAD
                      </Typography>
                      <Typography variant="caption" color="text.secondary">
                        Warehouse → Analytics
                      </Typography>
                      {selectedAssetDetails.pipeline_metadata.pipeline_stage === 'load' && (
                        <Chip label="Current" size="small" color="primary" sx={{ mt: 1 }} />
                      )}
                    </Box>
                  </Box>
                </Box>

                {}
                {selectedAssetDetails.pipeline_metadata.upstream_assets && selectedAssetDetails.pipeline_metadata.upstream_assets.length > 0 && (
                  <Box sx={{ mb: 4 }}>
                    <Typography variant="subtitle1" sx={{ fontWeight: 600, mb: 2, color: '#333' }}>
                      Upstream Assets in Pipeline
                    </Typography>
                    <Grid container spacing={2}>
                      {selectedAssetDetails.pipeline_metadata.upstream_assets.map((upstreamAsset, idx) => (
                        <Grid item xs={12} md={6} key={idx}>
                          <Card variant="outlined" sx={{ p: 2, cursor: 'pointer', '&:hover': { boxShadow: 2 } }}>
                            <Box sx={{ display: 'flex', justifyContent: 'space-between', alignItems: 'start' }}>
                              <Box>
                                <Typography variant="body1" sx={{ fontWeight: 600, mb: 0.5 }}>
                                  {upstreamAsset.name}
                                </Typography>
                                <Typography variant="caption" color="text.secondary" sx={{ display: 'block', mb: 1 }}>
                                  {upstreamAsset.id}
                                </Typography>
                                <Box sx={{ display: 'flex', gap: 1, flexWrap: 'wrap' }}>
                                  <Chip label={upstreamAsset.stage} size="small" color="info" variant="outlined" />
                                  <Chip label={upstreamAsset.type} size="small" variant="outlined" />
                                </Box>
                              </Box>
                            </Box>
                          </Card>
                        </Grid>
                      ))}
                    </Grid>
                  </Box>
                )}

                {}
                {selectedAssetDetails.pipeline_metadata.downstream_assets && selectedAssetDetails.pipeline_metadata.downstream_assets.length > 0 && (
                  <Box sx={{ mb: 4 }}>
                    <Typography variant="subtitle1" sx={{ fontWeight: 600, mb: 2, color: '#333' }}>
                      Downstream Assets in Pipeline
                    </Typography>
                    <Grid container spacing={2}>
                      {selectedAssetDetails.pipeline_metadata.downstream_assets.map((downstreamAsset, idx) => (
                        <Grid item xs={12} md={6} key={idx}>
                          <Card variant="outlined" sx={{ p: 2, cursor: 'pointer', '&:hover': { boxShadow: 2 } }}>
                            <Box sx={{ display: 'flex', justifyContent: 'space-between', alignItems: 'start' }}>
                              <Box>
                                <Typography variant="body1" sx={{ fontWeight: 600, mb: 0.5 }}>
                                  {downstreamAsset.name}
                                </Typography>
                                <Typography variant="caption" color="text.secondary" sx={{ display: 'block', mb: 1 }}>
                                  {downstreamAsset.id}
                                </Typography>
                                <Box sx={{ display: 'flex', gap: 1, flexWrap: 'wrap' }}>
                                  <Chip label={downstreamAsset.stage} size="small" color="success" variant="outlined" />
                                  <Chip label={downstreamAsset.type} size="small" variant="outlined" />
                                </Box>
                              </Box>
                            </Box>
                          </Card>
                        </Grid>
                      ))}
                    </Grid>
                  </Box>
                )}

                {}
                <Box>
                  <Typography variant="subtitle1" sx={{ fontWeight: 600, mb: 3, color: '#333' }}>
                    Pipeline Statistics
                  </Typography>
                  <Grid container spacing={3}>
                    <Grid item xs={12} md={4}>
                      <Box sx={{ p: 2, border: '1px solid #e0e0e0', borderRadius: 1, textAlign: 'center' }}>
                        <Typography variant="body2" color="text.secondary" sx={{ fontSize: 12, fontWeight: 500, mb: 1 }}>
                          Total Pipeline Assets
                        </Typography>
                        <Typography variant="h4" sx={{ fontWeight: 600, color: '#1976d2' }}>
                          {(selectedAssetDetails.pipeline_metadata.upstream_assets?.length || 0) + 
                           (selectedAssetDetails.pipeline_metadata.downstream_assets?.length || 0) + 1}
                        </Typography>
                      </Box>
                    </Grid>
                    <Grid item xs={12} md={4}>
                      <Box sx={{ p: 2, border: '1px solid #e0e0e0', borderRadius: 1, textAlign: 'center' }}>
                        <Typography variant="body2" color="text.secondary" sx={{ fontSize: 12, fontWeight: 500, mb: 1 }}>
                          Upstream Connections
                        </Typography>
                        <Typography variant="h4" sx={{ fontWeight: 600, color: '#00D4AA' }}>
                          {selectedAssetDetails.pipeline_metadata.upstream_assets?.length || 0}
                        </Typography>
                      </Box>
                    </Grid>
                    <Grid item xs={12} md={4}>
                      <Box sx={{ p: 2, border: '1px solid #e0e0e0', borderRadius: 1, textAlign: 'center' }}>
                        <Typography variant="body2" color="text.secondary" sx={{ fontSize: 12, fontWeight: 500, mb: 1 }}>
                          Downstream Connections
                        </Typography>
                        <Typography variant="h4" sx={{ fontWeight: 600, color: '#f57c00' }}>
                          {selectedAssetDetails.pipeline_metadata.downstream_assets?.length || 0}
                        </Typography>
                      </Box>
                    </Grid>
                  </Grid>
                </Box>
              </Box>
            )}
          </CardContent>
        </Card>
      )}

      {}
      <Dialog
        open={detailsDialogOpen}
        onClose={handleCloseDialog}
        maxWidth="md"
        fullWidth
      >
        {selectedNode && (
          <>
            <DialogTitle>
              <Box sx={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center' }}>
                <Box>
                  <Typography variant="h5" sx={{ fontWeight: 600 }}>
                    {selectedNode.name}
                  </Typography>
                  <Box sx={{ display: 'flex', gap: 1, mt: 1 }}>
                    <Chip 
                      label={selectedNode.isContainer ? 'Container' : selectedNode.isFolder ? 'Folder' : (selectedNode.type || 'Asset')} 
                      size="small" 
                      color={selectedNode.isContainer ? 'secondary' : selectedNode.isFolder ? 'info' : 'primary'} 
                    />
                    <Chip 
                      label={selectedNode.isContainer || selectedNode.isFolder 
                        ? 'Azure Blob Storage' 
                        : (selectedNode.source_system || selectedNode.connector_id || 'Azure Blob Storage')} 
                      size="small" 
                      variant="outlined"
                      sx={{ 
                        borderColor: '#999',
                        color: '#555',
                        backgroundColor: '#f5f5f5',
                        minWidth: '60px'
                      }}
                    />
                  </Box>
                </Box>
                <IconButton onClick={handleCloseDialog}>
                  <Close />
                </IconButton>
              </Box>
            </DialogTitle>
            <DialogContent dividers>
              {selectedNode.error ? (
                <Alert severity="error">{selectedNode.error}</Alert>
              ) : selectedNode.isContainer ? (
                <Grid container spacing={3}>
                  <Grid item xs={12}>
                    <Typography variant="subtitle2" color="text.secondary">
                      Container Name
                    </Typography>
                    <Typography variant="body1" sx={{ fontFamily: 'monospace', fontWeight: 600, fontSize: '1.1rem' }}>
                      {selectedNode.name}
                    </Typography>
                  </Grid>
                  <Grid item xs={12}>
                    <Typography variant="subtitle2" color="text.secondary">
                      Type
                    </Typography>
                    <Typography variant="body1">
                      Azure Blob Storage Container
                    </Typography>
                  </Grid>
                  <Grid item xs={12}>
                    <Typography variant="subtitle2" color="text.secondary">
                      Description
                    </Typography>
                    <Typography variant="body1">
                      {selectedNode.description || 'Storage container in Azure Blob Storage'}
                    </Typography>
                  </Grid>
                </Grid>
              ) : selectedNode.isFolder ? (
                <Grid container spacing={3}>
                  <Grid item xs={12}>
                    <Typography variant="subtitle2" color="text.secondary">
                      Folder Name
                    </Typography>
                    <Typography variant="body1" sx={{ fontFamily: 'monospace', fontWeight: 600, fontSize: '1.1rem' }}>
                      {selectedNode.name}
                    </Typography>
                  </Grid>
                  <Grid item xs={12}>
                    <Typography variant="subtitle2" color="text.secondary">
                      Full Path
                    </Typography>
                    <Typography variant="body2" sx={{ fontFamily: 'monospace', wordBreak: 'break-all' }}>
                      {selectedNode.full_path || selectedNode.name}
                    </Typography>
                  </Grid>
                  <Grid item xs={12}>
                    <Typography variant="subtitle2" color="text.secondary">
                      Type
                    </Typography>
                    <Typography variant="body1">
                      Folder/Directory
                    </Typography>
                  </Grid>
                  <Grid item xs={12}>
                    <Typography variant="subtitle2" color="text.secondary">
                      Description
                    </Typography>
                    <Typography variant="body1">
                      {selectedNode.description || 'Folder in the storage hierarchy'}
                    </Typography>
                  </Grid>
                </Grid>
              ) : (
                <Grid container spacing={3}>
                  <Grid item xs={12}>
                    <Typography variant="subtitle2" color="text.secondary">
                      Asset ID
                    </Typography>
                    <Typography variant="body2" sx={{ wordBreak: 'break-all', fontFamily: 'monospace' }}>
                      {selectedNode.id}
                    </Typography>
                  </Grid>
                  <Grid item xs={6}>
                    <Typography variant="subtitle2" color="text.secondary">
                      Type
                    </Typography>
                    <Typography variant="body2">
                      {selectedNode.type}
                    </Typography>
                  </Grid>
                  <Grid item xs={6}>
                    <Typography variant="subtitle2" color="text.secondary">
                      Source System
                    </Typography>
                    <Typography variant="body2">
                      {selectedNode.source_system || selectedNode.connector_id || 'Azure Blob Storage'}
                    </Typography>
                  </Grid>
                  <Grid item xs={12}>
                    <Typography variant="subtitle2" color="text.secondary">
                      Description
                    </Typography>
                    <Typography variant="body1">
                      {selectedNode.description || 'No description available'}
                    </Typography>
                  </Grid>
                  {selectedNode.columns && selectedNode.columns.length > 0 && (
                    <Grid item xs={12}>
                      <Box sx={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center', mb: 2 }}>
                        <Typography variant="subtitle2" color="text.secondary">
                          Columns ({selectedNode.columns.length})
                        </Typography>
                        {selectedNode.columns.length > columnsPerPage && (
                          <Box sx={{ display: 'flex', alignItems: 'center', gap: 1 }}>
                            <Button
                              size="small"
                              disabled={columnPage === 0}
                              onClick={() => setColumnPage(columnPage - 1)}
                            >
                              Previous
                            </Button>
                            <Typography variant="caption" color="text.secondary">
                              Page {columnPage + 1} of {Math.ceil(selectedNode.columns.length / columnsPerPage)}
                            </Typography>
                            <Button
                              size="small"
                              disabled={(columnPage + 1) * columnsPerPage >= selectedNode.columns.length}
                              onClick={() => setColumnPage(columnPage + 1)}
                            >
                              Next
                            </Button>
                          </Box>
                        )}
                      </Box>
                      <TableContainer component={Paper} variant="outlined">
                        <Table size="small">
                          <TableHead>
                            <TableRow>
                              <TableCell sx={{ fontWeight: 600, backgroundColor: '#f5f5f5' }}>Name</TableCell>
                              <TableCell sx={{ fontWeight: 600, backgroundColor: '#f5f5f5' }}>Type</TableCell>
                              <TableCell sx={{ fontWeight: 600, backgroundColor: '#f5f5f5' }}>PII</TableCell>
                              <TableCell sx={{ fontWeight: 600, backgroundColor: '#f5f5f5' }}>Description</TableCell>
                            </TableRow>
                          </TableHead>
                          <TableBody>
                            {selectedNode.columns
                              .slice(columnPage * columnsPerPage, (columnPage + 1) * columnsPerPage)
                              .map((col, index) => {
                              const isPII = detectPII(col.name, col.description);
                              return (
                              <TableRow key={index}>
                                  <TableCell sx={{ fontFamily: 'monospace', fontWeight: 500 }}>
                                    {col.name}
                                  </TableCell>
                                <TableCell>
                                    <Chip 
                                      label={col.type} 
                                      size="small" 
                                      variant="outlined"
                                      sx={{ borderColor: '#ddd', color: '#666' }}
                                    />
                                </TableCell>
                                  <TableCell>
                                    {(col.pii_detected !== undefined ? col.pii_detected : isPII) ? (
                                      <Chip 
                                        label="PII" 
                                        size="small" 
                                        color="error"
                                        sx={{ fontWeight: 600 }}
                                      />
                                    ) : (
                                      <Chip 
                                        label="Safe" 
                                        size="small" 
                                        color="success"
                                        variant="outlined"
                                        sx={{ fontWeight: 500 }}
                                      />
                                    )}
                                  </TableCell>
                                  <TableCell sx={{ color: '#666' }}>
                                    {col.description || '-'}
                                  </TableCell>
                              </TableRow>
                              );
                            })}
                          </TableBody>
                        </Table>
                      </TableContainer>
                      {selectedNode.columns.length > columnsPerPage && (
                        <Typography variant="caption" color="text.secondary" sx={{ mt: 1, display: 'block' }}>
                          Showing {columnPage * columnsPerPage + 1}-{Math.min((columnPage + 1) * columnsPerPage, selectedNode.columns.length)} of {selectedNode.columns.length} columns
                        </Typography>
                      )}
                    </Grid>
                  )}
                </Grid>
              )}
            </DialogContent>
            <DialogActions>
              <Button onClick={handleCloseDialog}>Close</Button>
              {!selectedNode.isContainer && !selectedNode.isFolder && !selectedNode.error && (
                <Button 
                  variant="contained" 
                  onClick={() => {
                    setSelectedAssetDetails(selectedNode);
                    setActiveDetailTab('basic');
                    handleCloseDialog();
                  }}
                >
                  View Full Details
                </Button>
              )}
            </DialogActions>
          </>
        )}
      </Dialog>

      {}
      <Dialog
        open={edgeDetailsOpen}
        onClose={handleCloseEdgeDialog}
        maxWidth="md"
        fullWidth
      >
        {selectedEdge && (
          <>
            <DialogTitle>
              <Box sx={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center' }}>
                <Box>
                  <Typography variant="h5" sx={{ fontWeight: 600 }}>
                    Column-Level Lineage
                  </Typography>
                  <Typography variant="body2" color="text.secondary" sx={{ mt: 0.5 }}>
                    {selectedEdge.column_lineage.length} column relationships
                  </Typography>
                </Box>
                <IconButton onClick={handleCloseEdgeDialog}>
                  <Close />
                </IconButton>
              </Box>
            </DialogTitle>
            <DialogContent dividers>
              {selectedEdge.column_lineage && selectedEdge.column_lineage.length > 0 ? (
                <TableContainer component={Paper} variant="outlined">
                  <Table>
                    <TableHead>
                      <TableRow>
                        <TableCell>Source Column</TableCell>
                        <TableCell>→</TableCell>
                        <TableCell>Target Column</TableCell>
                        <TableCell>Relationship</TableCell>
                        <TableCell>Transformation</TableCell>
                      </TableRow>
                    </TableHead>
                    <TableBody>
                      {selectedEdge.column_lineage.map((colRel, index) => (
                        <TableRow key={index}>
                          <TableCell>
                            <Box>
                              <Typography variant="body2" sx={{ fontWeight: 600 }}>
                                {colRel.source_column}
                              </Typography>
                              <Typography variant="caption" color="text.secondary">
                                {colRel.source_table ? (colRel.source_table.includes('.') ? colRel.source_table.split('.').pop() : colRel.source_table) : '-'}
                              </Typography>
                            </Box>
                          </TableCell>
                          <TableCell>
                            <Box sx={{ display: 'flex', justifyContent: 'center' }}>
                              →
                            </Box>
                          </TableCell>
                          <TableCell>
                            <Box>
                              <Typography variant="body2" sx={{ fontWeight: 600 }}>
                                {colRel.target_column}
                              </Typography>
                              <Typography variant="caption" color="text.secondary">
                                {colRel.target_table ? (colRel.target_table.includes('.') ? colRel.target_table.split('.').pop() : colRel.target_table) : '-'}
                              </Typography>
                            </Box>
                          </TableCell>
                          <TableCell>
                            <Chip 
                              label={(colRel.relationship_type || colRel.relationship || 'direct_mapping').replace(/_/g, ' ')} 
                              size="small" 
                              color={
                                (colRel.relationship_type || colRel.relationship || '').includes('direct') ? 'success' : 
                                (colRel.relationship_type || colRel.relationship || '').includes('transform') ? 'warning' : 'info'
                              }
                            />
                          </TableCell>
                          <TableCell>
                            {colRel.transformation_type || colRel.transformation ? (
                              <Box>
                                <Chip 
                                  label={colRel.transformation_type || colRel.transformation || 'pass_through'} 
                                  size="small" 
                                  color="primary"
                                  variant="outlined"
                                  sx={{ mb: 0.5 }}
                                />
                                {colRel.transformation_expression && (
                                  <Typography variant="caption" sx={{ display: 'block', color: '#666', fontFamily: 'monospace' }}>
                                    {colRel.transformation_expression}
                                  </Typography>
                                )}
                              </Box>
                            ) : (
                              <Typography variant="caption" color="text.secondary">-</Typography>
                            )}
                          </TableCell>
                        </TableRow>
                      ))}
                    </TableBody>
                  </Table>
                </TableContainer>
              ) : (
                <Alert severity="info">
                  No column-level lineage information available for this relationship.
                </Alert>
              )}
            </DialogContent>
            <DialogActions>
              <Button onClick={handleCloseEdgeDialog}>Close</Button>
            </DialogActions>
          </>
        )}
      </Dialog>

      {}
      {}
      <Dialog
        open={showImpactAnalysis}
        onClose={() => setShowImpactAnalysis(false)}
        maxWidth="md"
        fullWidth
      >
        <DialogTitle>
          <Box sx={{ display: 'flex', alignItems: 'center', gap: 1 }}>
            <Warning color="warning" />
            <Typography variant="h6" component="span">
              Impact Analysis
            </Typography>
          </Box>
        </DialogTitle>
        <DialogContent>
          {impactAnalysis ? (
            <Box>
              <Alert severity="warning" sx={{ mb: 3 }}>
                <Typography variant="body2" sx={{ fontWeight: 600, mb: 1 }}>
                  Changing this asset will impact {impactAnalysis.impact_summary.total_impacted_assets} downstream asset(s)
                </Typography>
                <Typography variant="caption">
                  Direct impact: {impactAnalysis.impact_summary.direct_impact} • 
                  Total dependencies: {impactAnalysis.impact_summary.total_dependencies}
                </Typography>
              </Alert>

              {impactAnalysis.impacted_assets.length > 0 && (
                <Box sx={{ mb: 3 }}>
                  <Typography variant="subtitle1" sx={{ fontWeight: 600, mb: 2 }}>
                    Impacted Assets ({impactAnalysis.impacted_assets.length})
                  </Typography>
                  <TableContainer component={Paper} variant="outlined">
                    <Table size="small">
                      <TableHead>
                        <TableRow>
                          <TableCell>Asset Name</TableCell>
                          <TableCell>Type</TableCell>
                          <TableCell>Catalog</TableCell>
                        </TableRow>
                      </TableHead>
                      <TableBody>
                        {impactAnalysis.impacted_assets.map((asset) => (
                          <TableRow key={asset.id}>
                            <TableCell>{asset.name}</TableCell>
                            <TableCell>
                              <Chip label={asset.type} size="small" variant="outlined" />
                            </TableCell>
                            <TableCell>{asset.catalog}</TableCell>
                          </TableRow>
                        ))}
                      </TableBody>
                    </Table>
                  </TableContainer>
                </Box>
              )}

              {impactAnalysis.dependencies.length > 0 && (
                <Box>
                  <Typography variant="subtitle1" sx={{ fontWeight: 600, mb: 2 }}>
                    Dependencies ({impactAnalysis.dependencies.length})
                  </Typography>
                  <TableContainer component={Paper} variant="outlined">
                    <Table size="small">
                      <TableHead>
                        <TableRow>
                          <TableCell>Asset Name</TableCell>
                          <TableCell>Type</TableCell>
                          <TableCell>Relationship</TableCell>
                        </TableRow>
                      </TableHead>
                      <TableBody>
                        {impactAnalysis.dependencies.map((dep) => (
                          <TableRow key={dep.id || dep.name}>
                            <TableCell>{dep.name || 'Azure Blob Storage'}</TableCell>
                            <TableCell>
                              <Chip label={dep.type || 'Azure Blob Storage'} size="small" variant="outlined" />
                            </TableCell>
                            <TableCell>
                              <Chip 
                                label={dep.relationship?.type || 'related'} 
                                size="small" 
                                color="info"
                              />
                            </TableCell>
                          </TableRow>
                        ))}
                      </TableBody>
                    </Table>
                  </TableContainer>
                </Box>
              )}

              {impactAnalysis.impact_paths.length > 0 && (
                <Box sx={{ mt: 3 }}>
                  <Typography variant="subtitle1" sx={{ fontWeight: 600, mb: 2 }}>
                    Impact Paths
                  </Typography>
                  {impactAnalysis.impact_paths.slice(0, 10).map((path, idx) => (
                    <Box key={idx} sx={{ mb: 1, p: 1, bgcolor: '#f5f5f5', borderRadius: 1 }}>
                      <Typography variant="caption" sx={{ fontFamily: 'monospace' }}>
                        {path.path.join(' → ')}
                      </Typography>
                    </Box>
                  ))}
                </Box>
              )}
            </Box>
          ) : (
            <Alert severity="info">
              No impact analysis available. Select an asset first.
            </Alert>
          )}
        </DialogContent>
        <DialogActions>
          <Button onClick={() => setShowImpactAnalysis(false)}>Close</Button>
        </DialogActions>
      </Dialog>

      {}
      <ManualLineageDialog
        open={manualLineageOpen}
        onClose={() => setManualLineageOpen(false)}
        assets={assets}
        onSuccess={() => {
          // Refresh both lineage + assets (so dropdown includes new discoveries)
          fetchLineage();
          fetchAssets();
          // If an asset is selected, reload its lineage to show the new manual relationship
          if (selectedAssetForLineage) {
            handleAssetSelection(selectedAssetForLineage);
          }
        }}
      />
    </Box>
  );
};

export default DataLineagePage;

