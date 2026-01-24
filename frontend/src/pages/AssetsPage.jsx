import React, { useState, useEffect } from 'react';
import { useNavigate } from 'react-router-dom';
import {
  Box,
  Typography,
  Card,
  CardContent,
  Grid,
  Chip,
  Button,
  Table,
  TableBody,
  TableCell,
  TableContainer,
  TableHead,
  TableRow,
  Paper,
  TextField,
  InputAdornment,
  FormControl,
  InputLabel,
  Select,
  MenuItem,
  Dialog,
  DialogTitle,
  DialogContent,
  DialogActions,
  Tabs,
  Tab,
  Divider,
  Alert,
  CircularProgress,
  LinearProgress,
  Pagination,
  Stack,
  RadioGroup,
  FormControlLabel,
  Radio,
  Checkbox,
  FormGroup,
  FormLabel,
  Tooltip,
  IconButton,
  Menu,
  ListItemText,
  Switch,
} from '@mui/material';
import {
  Search,
  Refresh,
  DataObject,
  FilterList,
  Visibility,
  Warning,
  CheckCircle,
  Close,
  ThumbUp,
  ThumbDown,
  Publish,
  Add,
  Edit,
  Save,
  FolderOpen,
  ArrowForward,
  Settings,
  ArrowDropDown,
  FileDownload,
  CloudUpload,
} from '@mui/icons-material';

const AssetsPage = () => {
  const navigate = useNavigate();
  const [assets, setAssets] = useState([]);
  const [loading, setLoading] = useState(false);
  const [publishing, setPublishing] = useState(false);
  const [searchTerm, setSearchTerm] = useState('');
  const [typeFilter, setTypeFilter] = useState([]);
  const [catalogFilter, setCatalogFilter] = useState([]);
  const [approvalStatusFilter, setApprovalStatusFilter] = useState([]);
  const [applicationNameFilter, setApplicationNameFilter] = useState([]);
  
  // Menu anchors for multi-select filters
  const [typeMenuAnchor, setTypeMenuAnchor] = useState(null);
  const [catalogMenuAnchor, setCatalogMenuAnchor] = useState(null);
  const [statusMenuAnchor, setStatusMenuAnchor] = useState(null);
  const [applicationMenuAnchor, setApplicationMenuAnchor] = useState(null);
  const [selectedAsset, setSelectedAsset] = useState(null);
  const [detailsDialogOpen, setDetailsDialogOpen] = useState(false);
  const [activeTab, setActiveTab] = useState(0);
  const [classification, setClassification] = useState('internal');
  const [sensitivityLevel, setSensitivityLevel] = useState('medium');
  const [department, setDepartment] = useState('Data Engineering');
  const [originalClassification, setOriginalClassification] = useState('internal');
  const [originalSensitivityLevel, setOriginalSensitivityLevel] = useState('medium');
  const [originalDepartment, setOriginalDepartment] = useState('Data Engineering');
  const [savingMetadata, setSavingMetadata] = useState(false);
  
  // Metadata visibility settings
  const [metadataSettingsOpen, setMetadataSettingsOpen] = useState(false);
  const [metadataSettingsTab, setMetadataSettingsTab] = useState(0);
  const [metadataDataSourceType, setMetadataDataSourceType] = useState('azure'); // 'azure' or 'oracle'
  
  // Export menu state
  const [exportAnchorEl, setExportAnchorEl] = useState(null);
  const [selectedColumns, setSelectedColumns] = useState([]);
  const [showColumnCheckboxes, setShowColumnCheckboxes] = useState(false);
  
  // Custom columns state
  const [addColumnDialogOpen, setAddColumnDialogOpen] = useState(false);
  const [newColumnLabel, setNewColumnLabel] = useState('');
  const [customColumns, setCustomColumns] = useState({}); // { columnId: { label: string, values: { columnName: value } } }
  const [editingCustomValue, setEditingCustomValue] = useState(null); // { columnId, columnName }
  const [customValueInput, setCustomValueInput] = useState('');

  // Hidden duplicates review
  const [hiddenDuplicatesOpen, setHiddenDuplicatesOpen] = useState(false);
  const [hiddenDuplicatesLoading, setHiddenDuplicatesLoading] = useState(false);
  const [hiddenDuplicates, setHiddenDuplicates] = useState([]);
  const [hiddenDuplicatesPage, setHiddenDuplicatesPage] = useState(1);
  const [hiddenDuplicatesPerPage, setHiddenDuplicatesPerPage] = useState(50);
  const [hiddenDuplicatesTotal, setHiddenDuplicatesTotal] = useState(0);
  const [hiddenDuplicatesTotalPages, setHiddenDuplicatesTotalPages] = useState(0);
  const [removeDuplicatesMenuAnchor, setRemoveDuplicatesMenuAnchor] = useState(null);
  
  // Deduplication job status
  const [deduplicationJobId, setDeduplicationJobId] = useState(null);
  const [deduplicationStatus, setDeduplicationStatus] = useState(null);
  const [deduplicationProgressOpen, setDeduplicationProgressOpen] = useState(false);
  
  // Starburst ingestion dialog state
  const [starburstDialogOpen, setStarburstDialogOpen] = useState(false);
  const [starburstAsset, setStarburstAsset] = useState(null);
  const [starburstHost, setStarburstHost] = useState('');
  const [starburstPort, setStarburstPort] = useState('443');
  const [starburstUser, setStarburstUser] = useState('');
  const [starburstPassword, setStarburstPassword] = useState('');
  const [starburstHttpScheme, setStarburstHttpScheme] = useState('https');
  const [starburstCatalog, setStarburstCatalog] = useState('');
  const [starburstSchema, setStarburstSchema] = useState('');
  const [starburstTableName, setStarburstTableName] = useState('');
  const [starburstViewName, setStarburstViewName] = useState('');
  const [starburstViewSql, setStarburstViewSql] = useState('');
  const [starburstLoading, setStarburstLoading] = useState(false);
  const [starburstError, setStarburstError] = useState('');
  const [starburstSuccess, setStarburstSuccess] = useState('');
  
  // Default metadata field visibility (all visible by default)
  // Only includes fields that are actually displayed in the UI and controlled by visibility settings
  const defaultMetadataVisibility = {
    technical: {
      // Azure Blob Storage fields (all controlled by visibility)
      'Asset ID': true,
      'Last Modified': true,
      'Creation Time': true,
      'Type': true,
      'Size': true,
      'Format': true,
      'Access Tier': true,
      'ETAG': true,
      'Content Type': true,
      'Location': true,
      'Application Name': true,
      'File Extension': true,
      'Number of Rows': true,
      // Oracle DB fields (only those controlled by visibility)
      'Database Type': true,
      'Schema': true,
      'Object Type': true,
      'Table Name': true,
      'View Name': true,
    },
    operational: {
      // Azure Blob Storage fields
      'Status': true,
      'Owner': true,
      'Last Modified': true,
      'Last Accessed': true,
      'Access Count': true,
      'Data Source Type': true,
      'Connector ID': true,
      'Catalog': true,
      'Discovered At': true,
      'Discovery ID': true,
      'Application Name': true,
      // Oracle DB fields
      'Object Status': true,
      'Schema Owner': true,
      'Last Analyzed': true,
      'Last Refresh Date': true,
    },
    business: {
      'Description': true,
      'Business Owner': true,
      'Department': true,
      'Classification': true,
      'Sensitivity Level': true,
      'Tags': true,
    },
  };
  
  // Define which fields belong to which data source type
  const technicalFieldsBySource = {
    azure: ['Asset ID', 'Last Modified', 'Creation Time', 'Type', 'Size', 'Format', 'Access Tier', 'ETAG', 'Content Type', 'Location', 'Application Name', 'File Extension', 'Number of Rows'],
    oracle: ['Asset ID', 'Database Type', 'Schema', 'Object Type', 'Table Name', 'View Name'],
  };
  
  const operationalFieldsBySource = {
    azure: ['Status', 'Owner', 'Last Modified', 'Last Accessed', 'Access Count', 'Data Source Type', 'Connector ID', 'Catalog', 'Discovered At', 'Discovery ID', 'Application Name'],
    oracle: ['Object Status', 'Schema Owner', 'Last Analyzed', 'Last Refresh Date', 'Connector ID', 'Discovered At', 'Discovery ID', 'Application Name'],
  };
  
  // Load metadata visibility preferences from localStorage
  // Merge with defaults to ensure new fields are included
  const loadMetadataVisibility = () => {
    try {
      const saved = localStorage.getItem('metadataVisibility');
      if (saved) {
        const savedVisibility = JSON.parse(saved);
        // Merge saved preferences with defaults to add any new fields
        return {
          technical: { ...defaultMetadataVisibility.technical, ...savedVisibility.technical },
          operational: { ...defaultMetadataVisibility.operational, ...savedVisibility.operational },
          business: { ...defaultMetadataVisibility.business, ...savedVisibility.business },
        };
      }
    } catch (e) {
      console.error('Error loading metadata visibility:', e);
    }
    return defaultMetadataVisibility;
  };
  
  const [metadataVisibility, setMetadataVisibility] = useState(loadMetadataVisibility);
  
  // Ensure preferences match defaults - add missing fields and remove invalid ones
  useEffect(() => {
    const current = metadataVisibility;
    let needsUpdate = false;
    const updated = {
      technical: {},
      operational: {},
      business: {}
    };
    
    // Technical: Only keep fields that are in defaults
    Object.keys(defaultMetadataVisibility.technical).forEach(field => {
      updated.technical[field] = current.technical?.[field] ?? defaultMetadataVisibility.technical[field];
      if (current.technical?.[field] !== updated.technical[field]) {
        needsUpdate = true;
      }
    });
    
    // Operational: Only keep fields that are in defaults
    Object.keys(defaultMetadataVisibility.operational).forEach(field => {
      updated.operational[field] = current.operational?.[field] ?? defaultMetadataVisibility.operational[field];
      if (current.operational?.[field] !== updated.operational[field]) {
        needsUpdate = true;
      }
    });
    
    // Business: Only keep fields that are in defaults (remove Container, Application Name, etc.)
    Object.keys(defaultMetadataVisibility.business).forEach(field => {
      updated.business[field] = current.business?.[field] ?? defaultMetadataVisibility.business[field];
      if (current.business?.[field] !== updated.business[field]) {
        needsUpdate = true;
      }
    });
    
    // Check if any fields were removed (exist in current but not in defaults)
    if (current.technical) {
      Object.keys(current.technical).forEach(field => {
        if (!(field in defaultMetadataVisibility.technical)) {
          needsUpdate = true;
        }
      });
    }
    if (current.operational) {
      Object.keys(current.operational).forEach(field => {
        if (!(field in defaultMetadataVisibility.operational)) {
          needsUpdate = true;
        }
      });
    }
    if (current.business) {
      Object.keys(current.business).forEach(field => {
        if (!(field in defaultMetadataVisibility.business)) {
          needsUpdate = true;
        }
      });
    }
    
    if (needsUpdate) {
      setMetadataVisibility(updated);
      saveMetadataVisibility(updated);
    }
  }, []); // Only run once on mount
  
  // Save metadata visibility preferences to localStorage
  const saveMetadataVisibility = (visibility) => {
    try {
      localStorage.setItem('metadataVisibility', JSON.stringify(visibility));
      setMetadataVisibility(visibility);
    } catch (e) {
      console.error('Error saving metadata visibility:', e);
    }
  };
  
  // Reset to default visibility
  const resetMetadataVisibility = () => {
    saveMetadataVisibility(defaultMetadataVisibility);
  };
  
  // Available departments
  const DEPARTMENTS = [
    'Data Engineering',
    'Data Science',
    'Business Intelligence',
    'IT Operations',
    'Security & Compliance',
    'Finance',
    'Risk Management',
    'Customer Analytics',
    'Product Development',
    'Marketing',
    'Sales',
    'Human Resources',
    'Legal',
    'Operations',
    'Other'
  ];
  
  
  const [rejectDialogOpen, setRejectDialogOpen] = useState(false);
  const [rejectReason, setRejectReason] = useState('');
  const [selectedRejectReason, setSelectedRejectReason] = useState('');
  const [customRejectReason, setCustomRejectReason] = useState('');
  const [assetToReject, setAssetToReject] = useState(null);
  
  // Column editing state
  const [editingColumn, setEditingColumn] = useState(null);
  const [columnEditData, setColumnEditData] = useState({});
  const [savingColumn, setSavingColumn] = useState(false);
  
  // Description state
  const [description, setDescription] = useState('');
  const [originalDescription, setOriginalDescription] = useState('');
  const [defaultDescription, setDefaultDescription] = useState('');
  
  // Governance rejection reasons
  const GOVERNANCE_REJECTION_REASONS = [
    { code: '001', reason: 'Data Quality Issues - Incomplete or inaccurate data', tag: 'Data Quality' },
    { code: '002', reason: 'Data Privacy Violation - Contains sensitive PII without proper controls', tag: 'Privacy Violation' },
    { code: '003', reason: 'Compliance Risk - Does not meet regulatory requirements', tag: 'Compliance Risk' },
    { code: '004', reason: 'Data Classification Mismatch - Incorrect sensitivity level assigned', tag: 'Classification Mismatch' },
    { code: '005', reason: 'Archive / Duplicate - Redundant or archived data', tag: 'Archive/Duplicate' },
    { code: '006', reason: 'Data Lineage Issues - Missing or incorrect lineage information', tag: 'Lineage Issues' },
    { code: '007', reason: 'Metadata Incomplete - Missing required metadata fields', tag: 'Incomplete Metadata' },
    { code: '008', reason: 'Data Retention Policy Violation - Exceeds retention period', tag: 'Retention Violation' },
    { code: '009', reason: 'Access Control Issues - Improper access permissions', tag: 'Access Control' },
    { code: '010', reason: 'Data Source Not Authorized - Source not approved for use', tag: 'Unauthorized Source' },
    { code: '011', reason: 'Others', tag: 'Rejected' }
  ];
  
  // Get short tag name for rejection reason
  const getRejectionTag = (reasonCode, customReason = '') => {
    if (reasonCode === '011') {
      // For "Others", use first 2 words of custom reason or "Rejected"
      if (customReason.trim()) {
        const words = customReason.trim().split(/\s+/).slice(0, 2);
        return words.join(' ');
      }
      return 'Rejected';
    }
    const selected = GOVERNANCE_REJECTION_REASONS.find(r => r.code === reasonCode);
    return selected ? selected.tag : 'Rejected';
  };
  
  
  const [discoveryDetailsOpen, setDiscoveryDetailsOpen] = useState(false);
  const [discoveryDetails, setDiscoveryDetails] = useState(null);
  
  const [currentPage, setCurrentPage] = useState(0);
  const [pageSize, setPageSize] = useState(50);
  const [totalAssets, setTotalAssets] = useState(0);
  const [totalPages, setTotalPages] = useState(0);
  const [allAssets, setAllAssets] = useState([]);
  
  // PII Dialog state
  const [piiDialogOpen, setPiiDialogOpen] = useState(false);
  const [selectedColumnForPii, setSelectedColumnForPii] = useState(null);
  const [piiDialogIsPii, setPiiDialogIsPii] = useState(false);
  
  const [piiDialogTypes, setPiiDialogTypes] = useState([]);
  const [savingPii, setSavingPii] = useState(false);
  const [customPiiType, setCustomPiiType] = useState('');
  // Masking logic state - track masking logic for each column
  const [columnMaskingLogic, setColumnMaskingLogic] = useState({});
  // Track original PII status to detect changes from Non-PII to PII
  const [originalPiiStatus, setOriginalPiiStatus] = useState({});
  // Track which columns have unsaved masking logic changes
  const [unsavedMaskingChanges, setUnsavedMaskingChanges] = useState({});
  const [savingMaskingLogic, setSavingMaskingLogic] = useState({}); 


  // Initialize masking logic from column data when asset is selected
  // Use asset-scoped keys to prevent masking logic from being shared across assets
  useEffect(() => {
    if (selectedAsset?.columns) {
      const initialMaskingLogic = {};
      const assetId = selectedAsset.id;
      selectedAsset.columns.forEach(col => {
        if (col.pii_detected) {
          const key = `${assetId}_${col.name}`;
          initialMaskingLogic[key] = {
            analytical: col.masking_logic_analytical || '',
            operational: col.masking_logic_operational || ''
          };
        }
      });
      setColumnMaskingLogic(prev => {
        // Merge with existing to preserve unsaved changes for other assets
        return { ...prev, ...initialMaskingLogic };
      });
      
      // Load custom columns from asset
      if (selectedAsset.custom_columns) {
        setCustomColumns(selectedAsset.custom_columns);
      } else {
        setCustomColumns({});
      }
    }
  }, [selectedAsset]);


  // Reset to page 0 when filters change
  useEffect(() => {
    if (currentPage !== 0) {
      setCurrentPage(0);
    }
  }, [searchTerm, typeFilter, catalogFilter, approvalStatusFilter, applicationNameFilter]);

  // Fetch assets when page or filters change
  useEffect(() => {
    fetchAssets();
  }, [currentPage, pageSize, searchTerm, typeFilter, catalogFilter, approvalStatusFilter, applicationNameFilter]);

  const fetchAssets = async (pageOverride = null, returnFiltered = false) => {
    setLoading(true);
    try {
      const API_BASE_URL = import.meta.env.VITE_API_BASE_URL;
      
      const page = pageOverride !== null ? pageOverride : currentPage;
      const pageParam = page + 1; // Backend uses 1-based pagination
      
      // Build query params with filters
      const params = new URLSearchParams({
        page: pageParam.toString(),
        per_page: pageSize.toString()
      });
      
      // Add filters to query params
      if (searchTerm) {
        params.append('search', searchTerm);
      }
      typeFilter.forEach(t => params.append('type', t));
      catalogFilter.forEach(c => params.append('catalog', c));
      approvalStatusFilter.forEach(s => params.append('approval_status', s));
      applicationNameFilter.forEach(a => params.append('application_name', a));
      
      const url = `${API_BASE_URL}/api/assets?${params.toString()}`;
      
      const response = await fetch(url);
      if (response.ok) {
        const data = await response.json();
        
        // Handle both old format (array) and new format (paginated object)
        let assetsList = [];
        let total = 0;
        let totalPagesCount = 0;
        
        if (Array.isArray(data)) {
          // Old format - backward compatibility
          assetsList = data;
          total = data.length;
          totalPagesCount = Math.ceil(data.length / pageSize);
        } else if (data.assets && data.pagination) {
          // New paginated format (with backend filtering)
          assetsList = data.assets;
          total = data.pagination.total;
          totalPagesCount = data.pagination.total_pages;
        } else {
          assetsList = [];
          total = 0;
          totalPagesCount = 0;
        }
        
        setAssets(assetsList);
        setAllAssets(assetsList);
        setTotalAssets(total);
        setTotalPages(totalPagesCount);
        
        // Return filtered count if requested (for checking empty pages)
        if (returnFiltered) {
          return assetsList.length;
        }
      } else {
        setAssets([]);
        setAllAssets([]);
        setTotalAssets(0);
        setTotalPages(0);
        if (returnFiltered) {
          return 0;
        }
      }
    } catch (error) {
      console.error('Error fetching assets:', error);
      setAssets([]);
      setAllAssets([]);
      setTotalAssets(0);
      setTotalPages(0);
      if (returnFiltered) {
        return 0;
      }
    } finally {
      setLoading(false);
    }
  };

  
  const uniqueTypes = allAssets ? [...new Set(allAssets.map(asset => asset.type))] : [];
  const uniqueCatalogs = allAssets ? [...new Set(allAssets.map(asset => asset.catalog))] : [];
  const uniqueApplicationNames = allAssets ? [...new Set(allAssets.map(asset => 
    asset.application_name || asset.technical_metadata?.application_name || asset.business_metadata?.application_name
  ).filter(Boolean))] : [];

  const getDataSource = (connectorId) => {
    if (!connectorId) return 'Unknown';
    
    
    if (connectorId.startsWith('azure_blob_')) {
      return 'Azure Blob Storage';
    }
    
    
    if (connectorId.startsWith('azure_')) {
      return 'Azure Storage';
    }
    
    
    if (connectorId.startsWith('oracle_db_')) {
      return 'Oracle DB';
    }
    
    
    return connectorId;
  };

  const getDataSourceColor = (connectorId) => {
    if (!connectorId) return 'default';
    
    if (connectorId.startsWith('azure_blob_') || connectorId.startsWith('azure_')) {
      return 'primary';
    }
    
    if (connectorId.startsWith('oracle_db_')) {
      return 'error';
    }
    
    return 'default';
  };

  const handleApproveAsset = async (assetId) => {
    
    const asset = allAssets.find(a => a.id === assetId);
    // Store original state for rollback
    const originalAsset = asset ? { ...asset } : null;
    
    if (asset) {
      const updatedAsset = {
        ...asset,
        operational_metadata: {
          ...(asset.operational_metadata || {}),
          approval_status: 'approved',
          approved_at: new Date().toISOString()
        }
      };
      setAllAssets(prev => prev.map(a => a.id === assetId ? updatedAsset : a));
      setAssets(prev => prev.map(a => a.id === assetId ? updatedAsset : a));
      
      if (import.meta.env.DEV) {
        console.log('Optimistic update - Asset approved:', assetId, updatedAsset.operational_metadata);
      }
    }
    
    try {
      const API_BASE_URL = import.meta.env.VITE_API_BASE_URL;
      const response = await fetch(`${API_BASE_URL}/api/assets/${assetId}/approve`, {
        method: 'POST',
        headers: {
          'Content-Type': 'application/json',
        },
      });
      
      if (response.ok) {
        const result = await response.json();
        
        if (import.meta.env.DEV) {
          console.log('Server response - Asset approved:', assetId, result);
        }
        
        // Refetch assets from server to ensure consistency
        // This ensures the current page still has items after approval
        // If the approved asset is filtered out, we'll get the correct page data
        const filteredCount = await fetchAssets(null, true);
        
        // If current page is empty after filtering and we're not on page 0, go to previous page
        if (filteredCount === 0 && currentPage > 0) {
          const newPage = Math.max(0, currentPage - 1);
          setCurrentPage(newPage);
          await fetchAssets(newPage);
        }
      } else {
        // Rollback optimistic update on error
        if (originalAsset) {
          setAllAssets(prev => prev.map(a => a.id === assetId ? originalAsset : a));
          setAssets(prev => prev.map(a => a.id === assetId ? originalAsset : a));
        }
        
        await fetchAssets();
        const errorData = await response.json();
        throw new Error(errorData.error || 'Failed to approve asset');
      }
    } catch (error) {
      // Rollback optimistic update on error
      if (originalAsset) {
        setAllAssets(prev => prev.map(a => a.id === assetId ? originalAsset : a));
        setAssets(prev => prev.map(a => a.id === assetId ? originalAsset : a));
      }
      
      await fetchAssets();
      if (import.meta.env.DEV) {
        console.error('Error approving asset:', error);
      }
      alert(`Failed to approve asset: ${error.message}`);
    }
  };

  const handleOpenStarburstDialog = (asset) => {
    if (!asset) return;
    setStarburstAsset(asset);
    setStarburstCatalog(asset.catalog || '');
    setStarburstSchema('');
    setStarburstTableName(asset.name || '');
    setStarburstViewName(`${asset.name || 'masked_view'}_masked`);
    setStarburstHost('');
    setStarburstPort('443');
    setStarburstUser('');
    setStarburstPassword('');
    setStarburstHttpScheme('https');
    setStarburstViewSql('');
    setStarburstError('');
    setStarburstSuccess('');
    setStarburstDialogOpen(true);
  };

  const handleCloseStarburstDialog = () => {
    if (starburstLoading) return;
    setStarburstDialogOpen(false);
  };

  const handleStarburstPreview = async () => {
    if (!starburstAsset) return;
    setStarburstLoading(true);
    setStarburstError('');
    setStarburstSuccess('');
    try {
      const API_BASE_URL = import.meta.env.VITE_API_BASE_URL;
      const response = await fetch(`${API_BASE_URL}/api/assets/${starburstAsset.id}/starburst/ingest`, {
        method: 'POST',
        headers: {
          'Content-Type': 'application/json',
        },
        body: JSON.stringify({
          preview_only: true,
          connection: {
            host: starburstHost,
            port: starburstPort ? Number(starburstPort) : 443,
            user: starburstUser,
            password: starburstPassword,
            http_scheme: starburstHttpScheme,
          },
          catalog: starburstCatalog,
          schema: starburstSchema,
          table_name: starburstTableName,
          view_name: starburstViewName,
        }),
      });

      const data = await response.json().catch(() => ({}));
      if (!response.ok || !data.success) {
        throw new Error(data.error || 'Failed to generate Starburst view SQL');
      }

      setStarburstViewSql(data.view_sql || '');
      setStarburstSuccess('Generated Starburst masking view SQL. Review below before ingesting.');
    } catch (error) {
      console.error('Error generating Starburst view SQL:', error);
      setStarburstError(error?.message || 'Failed to generate Starburst view SQL');
    } finally {
      setStarburstLoading(false);
    }
  };

  const handleStarburstIngest = async () => {
    if (!starburstAsset) return;
    setStarburstLoading(true);
    setStarburstError('');
    setStarburstSuccess('');
    try {
      const API_BASE_URL = import.meta.env.VITE_API_BASE_URL;
      const response = await fetch(`${API_BASE_URL}/api/assets/${starburstAsset.id}/starburst/ingest`, {
        method: 'POST',
        headers: {
          'Content-Type': 'application/json',
        },
        body: JSON.stringify({
          preview_only: false,
          connection: {
            host: starburstHost,
            port: starburstPort ? Number(starburstPort) : 443,
            user: starburstUser,
            password: starburstPassword,
            http_scheme: starburstHttpScheme,
          },
          catalog: starburstCatalog,
          schema: starburstSchema,
          table_name: starburstTableName,
          view_name: starburstViewName,
        }),
      });

      const data = await response.json().catch(() => ({}));
      if (!response.ok || !data.success) {
        throw new Error(data.error || 'Failed to ingest view into Starburst');
      }

      setStarburstViewSql(data.view_sql || '');
      setStarburstSuccess('Successfully created masked view in Starburst Enterprise.');
    } catch (error) {
      console.error('Error ingesting view into Starburst:', error);
      setStarburstError(error?.message || 'Failed to ingest view into Starburst');
    } finally {
      setStarburstLoading(false);
    }
  };

  const handleRejectClick = (assetId) => {
    setAssetToReject(assetId);
    setRejectReason('');
    setSelectedRejectReason('');
    setCustomRejectReason('');
    setRejectDialogOpen(true);
  };

  const handleRejectConfirm = async () => {
    if (!assetToReject) return;
    
    // Determine the final rejection reason
    let finalReason = '';
    if (selectedRejectReason === '011') {
      // "Others" selected - use custom reason
      if (!customRejectReason.trim()) {
        alert('Please provide a reason for rejection');
        return;
      }
      finalReason = customRejectReason.trim();
    } else {
      // Predefined reason selected
      if (!selectedRejectReason) {
        alert('Please select a reason for rejection');
        return;
      }
      const selected = GOVERNANCE_REJECTION_REASONS.find(r => r.code === selectedRejectReason);
      finalReason = selected ? `${selected.code} - ${selected.reason}` : selectedRejectReason;
    }
    
    setRejectDialogOpen(false);
    
    
    const asset = allAssets.find(a => a.id === assetToReject);
    
    // Validate asset exists before proceeding
    if (!asset) {
      alert('Asset not found. Please refresh the page and try again.');
      return;
    }
    
    // Add rejection reason as a short table tag (1-2 words)
    const shortTag = getRejectionTag(selectedRejectReason, customRejectReason);
    const rejectionTag = `REJECTED: ${shortTag}`;
    const existingTags = asset.business_metadata?.tags || [];
    
    // Remove any existing REJECTED tag, filter out "torrocon", and add the new one
    const filteredTags = existingTags
      .filter(tag => !tag.startsWith('REJECTED:') && tag.toLowerCase() !== 'torrocon')
      .filter(tag => tag.trim() !== ''); // Also filter out empty tags
    filteredTags.push(rejectionTag);
    
    // Store original state for rollback
    const originalAsset = { ...asset };
    
    if (asset) {
      const updatedAsset = {
        ...asset,
        operational_metadata: {
          ...asset.operational_metadata,
          approval_status: 'rejected',
          rejected_at: new Date().toISOString(),
          rejection_reason: finalReason
        },
        business_metadata: {
          ...asset.business_metadata,
          tags: filteredTags
        }
      };
      setAllAssets(prev => prev.map(a => a.id === assetToReject ? updatedAsset : a));
      setAssets(prev => prev.map(a => a.id === assetToReject ? updatedAsset : a));
    }
    
    try {
      const API_BASE_URL = import.meta.env.VITE_API_BASE_URL;
      
      // First, reject the asset
      const rejectResponse = await fetch(`${API_BASE_URL}/api/assets/${assetToReject}/reject`, {
        method: 'POST',
        headers: {
          'Content-Type': 'application/json',
        },
        body: JSON.stringify({ reason: finalReason }),
      });
      
      if (!rejectResponse.ok) {
        // Rollback optimistic update on error
        setAllAssets(prev => prev.map(a => a.id === assetToReject ? originalAsset : a));
        setAssets(prev => prev.map(a => a.id === assetToReject ? originalAsset : a));
        
        const errorData = await rejectResponse.json();
        throw new Error(errorData.error || 'Failed to reject asset');
      }
      
      // Then, update the business metadata with the tag
      const updateResponse = await fetch(`${API_BASE_URL}/api/assets/${assetToReject}`, {
        method: 'PUT',
        headers: {
          'Content-Type': 'application/json',
        },
        body: JSON.stringify({
          business_metadata: {
            ...asset?.business_metadata,
            tags: filteredTags
          }
        }),
      });
      
      if (updateResponse.ok) {
        const filteredCount = await fetchAssets(null, true);
        
        // If current page is empty after filtering and we're not on page 0, go to previous page
        if (filteredCount === 0 && currentPage > 0) {
          const newPage = Math.max(0, currentPage - 1);
          setCurrentPage(newPage);
          await fetchAssets(newPage);
        }
      } else {
        // Rollback optimistic update on error
        setAllAssets(prev => prev.map(a => a.id === assetToReject ? originalAsset : a));
        setAssets(prev => prev.map(a => a.id === assetToReject ? originalAsset : a));
        
        await fetchAssets();
        const errorData = await updateResponse.json();
        throw new Error(errorData.error || 'Failed to update tags');
      }
    } catch (error) {
      // Rollback optimistic update on error
      setAllAssets(prev => prev.map(a => a.id === assetToReject ? originalAsset : a));
      setAssets(prev => prev.map(a => a.id === assetToReject ? originalAsset : a));
      
      const filteredCount = await fetchAssets(null, true);
      
      // If current page is empty after filtering and we're not on page 0, go to previous page
      if (filteredCount === 0 && currentPage > 0) {
        const newPage = Math.max(0, currentPage - 1);
        setCurrentPage(newPage);
        await fetchAssets(newPage);
      }
      if (import.meta.env.DEV) {
        console.error('Error rejecting asset:', error);
      }
      alert(`Failed to reject asset: ${error.message}`);
    } finally {
      setAssetToReject(null);
      setRejectReason('');
      setSelectedRejectReason('');
      setCustomRejectReason('');
    }
  };

  const handlePublishAsset = async (assetId) => {
    const asset = allAssets.find(a => a.id === assetId);
    if (!asset) {
      alert('Asset not found');
      return;
    }

    // Show loader
    setPublishing(true);

    // Store original state for rollback
    const originalAsset = { ...asset };

    const updatedAsset = {
      ...asset,
      operational_metadata: {
        ...asset.operational_metadata,
        publish_status: 'published',
        published_at: new Date().toISOString()
      }
    };
    setAllAssets(prev => prev.map(a => a.id === assetId ? updatedAsset : a));
    setAssets(prev => prev.map(a => a.id === assetId ? updatedAsset : a));
    
    try {
      const API_BASE_URL = import.meta.env.VITE_API_BASE_URL;
      
      const response = await fetch(`${API_BASE_URL}/api/assets/${assetId}/publish`, {
        method: 'POST',
        headers: {
          'Content-Type': 'application/json',
        },
        body: JSON.stringify({ published_to: 'catalog' }),
      });
      
      if (response.ok) {
        const result = await response.json();
        const discoveryId = result.discovery_id;
        
        if (!discoveryId) {
          // Rollback optimistic update on error
          setAllAssets(prev => prev.map(a => a.id === assetId ? originalAsset : a));
          setAssets(prev => prev.map(a => a.id === assetId ? originalAsset : a));
          setPublishing(false);
          throw new Error('Discovery ID not returned from publish endpoint');
        }
        
        const discoveryResponse = await fetch(`${API_BASE_URL}/api/discovery/${discoveryId}`, {
          method: 'GET',
          headers: {
            'Content-Type': 'application/json',
          },
        });
        
        if (!discoveryResponse.ok) {
          // Rollback optimistic update on error
          setAllAssets(prev => prev.map(a => a.id === assetId ? originalAsset : a));
          setAssets(prev => prev.map(a => a.id === assetId ? originalAsset : a));
          setPublishing(false);
          throw new Error('Failed to fetch discovery details');
        }
        
        const discoveryData = await discoveryResponse.json();
        
        if (import.meta.env.DEV) {
          console.log('Full discovery data fetched:', discoveryData);
          console.log('Discovery data keys:', Object.keys(discoveryData));
        }
        
        const filteredCount = await fetchAssets(null, true);
        
        // Check if page is empty before navigation (in case navigation fails)
        if (filteredCount === 0 && currentPage > 0) {
          const newPage = Math.max(0, currentPage - 1);
          setCurrentPage(newPage);
          await fetchAssets(newPage);
        }
        
        // Navigate directly to /app/dataOnboarding without /airflow-fe prefix
        // Keep loader visible until navigation completes
        window.location.href = `/app/dataOnboarding?id=${discoveryId}`;
      } else {
        // Rollback optimistic update on error
        setAllAssets(prev => prev.map(a => a.id === assetId ? originalAsset : a));
        setAssets(prev => prev.map(a => a.id === assetId ? originalAsset : a));
        
        setPublishing(false);
        const filteredCount = await fetchAssets(null, true);
        
        // If current page is empty after filtering and we're not on page 0, go to previous page
        if (filteredCount === 0 && currentPage > 0) {
          const newPage = Math.max(0, currentPage - 1);
          setCurrentPage(newPage);
          await fetchAssets(newPage);
        }
        const errorData = await response.json();
        throw new Error(errorData.error || 'Failed to publish asset');
      }
    } catch (error) {
      // Rollback optimistic update on error
      setAllAssets(prev => prev.map(a => a.id === assetId ? originalAsset : a));
      setAssets(prev => prev.map(a => a.id === assetId ? originalAsset : a));
      
      setPublishing(false);
      const filteredCount = await fetchAssets(null, true);
      
      // If current page is empty after filtering and we're not on page 0, go to previous page
      if (filteredCount === 0 && currentPage > 0) {
        const newPage = Math.max(0, currentPage - 1);
        setCurrentPage(newPage);
        await fetchAssets(newPage);
      }
      if (import.meta.env.DEV) {
        console.error('Error publishing asset:', error);
      }
      alert(`Failed to publish asset: ${error.message}`);
    }
  };

  // Export handlers - Using batch publishing template format
  const handleExportTemplate = () => {
    // Template format: discovery_id,column_name,data_owner,column_tag_identifier,column_tag_value,policy_name,column_description,table_tag_identifier,table_tag_value,published_to
    const headers = ['discovery_id', 'column_name', 'data_owner', 'column_tag_identifier', 'column_tag_value', 'policy_name', 'column_description', 'table_tag_identifier', 'table_tag_value', 'published_to'];
    const csvContent = headers.join(',') + '\n';
    const blob = new Blob([csvContent], { type: 'text/csv' });
    const url = window.URL.createObjectURL(blob);
    const a = document.createElement('a');
    a.href = url;
    a.download = `Data-Batch-Publishing-Template.csv`;
    a.click();
    window.URL.revokeObjectURL(url);
  };
  
  const handleExportPII = () => {
    if (!selectedAsset?.columns) return;
    const piiColumns = selectedAsset.columns.filter(col => col.pii_detected);
    exportColumnsToCSV(piiColumns, 'pii_only');
  };
  
  const handleExportAll = () => {
    if (!selectedAsset?.columns) return;
    exportColumnsToCSV(selectedAsset.columns, 'all');
  };
  
  const handleExportSelected = () => {
    if (!selectedAsset?.columns || selectedColumns.length === 0) {
      alert('Please select columns to export');
      return;
    }
    const columnsToExport = selectedAsset.columns.filter(col => selectedColumns.includes(col.name));
    exportColumnsToCSV(columnsToExport, 'selected');
    setShowColumnCheckboxes(false); // Hide checkboxes after export
    setSelectedColumns([]); // Reset selection
  };
  
  const exportColumnsToCSV = (columns, type) => {
    if (!selectedAsset) return;
    
    // Get discovery_id from asset
    const discoveryId = selectedAsset.discovery_id || selectedAsset.operational_metadata?.discovery_id || '';
    
    // Get table tags from business metadata
    // Filter out "torrocon", container names (catalog), and empty tags
    const businessMetadata = selectedAsset.business_metadata || {};
    const catalog = selectedAsset.catalog || '';
    const container = businessMetadata.container || selectedAsset.technical_metadata?.container || '';
    const allTags = businessMetadata.tags || [];
    
    // Filter out torrocon, container names (catalog), and empty tags
    const filteredTableTags = allTags
      .filter(tag => {
        if (!tag || typeof tag !== 'string') return false;
        const tagTrimmed = tag.trim();
        const tagLower = tagTrimmed.toLowerCase();
        
        // Exclude empty tags
        if (tagTrimmed === '') return false;
        
        // Exclude torrocon
        if (tagLower === 'torrocon') return false;
        
        // Exclude if tag matches catalog (container name)
        if (catalog && tagLower === catalog.toLowerCase().trim()) return false;
        
        // Exclude if tag matches container field
        if (container && tagLower === container.toLowerCase().trim()) return false;
        
        return true;
      });
    
    const tableTagIdentifier = filteredTableTags.length > 0 ? filteredTableTags[0] : '';
    const tableTagValue = filteredTableTags.length > 1 ? filteredTableTags.slice(1).join('; ') : '';
    
    // Template format: discovery_id,column_name,data_owner,column_tag_identifier,column_tag_value,policy_name,column_description,table_tag_identifier,table_tag_value,published_to
    const headers = ['discovery_id', 'column_name', 'data_owner', 'column_tag_identifier', 'column_tag_value', 'policy_name', 'column_description', 'table_tag_identifier', 'table_tag_value', 'published_to'];
    
    const rows = columns.map(col => [
      discoveryId, // discovery_id
      col.name || '', // column_name
      '', // data_owner (empty for now)
      '', // column_tag_identifier (empty for now)
      '', // column_tag_value (empty for now)
      '', // policy_name (empty for now)
      col.description || '', // column_description
      tableTagIdentifier, // table_tag_identifier
      tableTagValue, // table_tag_value
      '' // published_to (empty for now)
    ]);
    
    const csvContent = [
      headers.join(','),
      ...rows.map(row => row.map(cell => {
        const cellValue = String(cell || '').replace(/"/g, '""');
        return `"${cellValue}"`;
      }).join(','))
    ].join('\n');
    
    const blob = new Blob([csvContent], { type: 'text/csv' });
    const url = window.URL.createObjectURL(blob);
    const a = document.createElement('a');
    a.href = url;
    const suffix = type === 'pii_only' ? '_pii_only' : type === 'selected' ? '_selected' : '_all';
    const assetName = selectedAsset.name ? selectedAsset.name.replace(/\.[^/.]+$/, '') : 'asset';
    a.download = `${assetName}${suffix}.csv`;
    a.click();
    window.URL.revokeObjectURL(url);
  };

  const handleViewAsset = async (assetId) => {
    // OPTIMIZED: First check if asset is already in state (instant - no API call)
    const cachedAsset = allAssets.find(a => a.id === assetId);
    
    if (cachedAsset) {
      // Asset already loaded - use it directly (instant!)
      setSelectedAsset(cachedAsset);
      setDetailsDialogOpen(true);
      setSelectedColumns([]); // Reset selected columns when opening new asset
      setShowColumnCheckboxes(false); // Hide checkboxes when opening new asset
        setOriginalClassification(cachedAsset.business_metadata?.classification || 'internal');
        setOriginalSensitivityLevel(cachedAsset.business_metadata?.sensitivity_level || 'medium');
        setOriginalDepartment(cachedAsset.business_metadata?.department || 'Data Engineering');
        setClassification(cachedAsset.business_metadata?.classification || 'internal');
        setSensitivityLevel(cachedAsset.business_metadata?.sensitivity_level || 'medium');
        setDepartment(cachedAsset.business_metadata?.department || 'Data Engineering');
        
        // Set default description
        const defaultDesc = `Azure Blob Storage file: ${cachedAsset.name}`;
        setDefaultDescription(defaultDesc);
        
        const desc = cachedAsset.business_metadata?.description || '';
        setDescription(desc || defaultDesc);
        setOriginalDescription(desc);
      return; // Exit early - no API call needed!
    }
    
    // Only fetch if asset not in state (rare case - asset not loaded yet)
    try {
      const API_BASE_URL = import.meta.env.VITE_API_BASE_URL;
      // OPTIMIZED: Fetch only the specific asset, not all assets
      const response = await fetch(`${API_BASE_URL}/api/assets/${assetId}`);
      if (response.ok) {
        const asset = await response.json();
        setSelectedAsset(asset);
        setDetailsDialogOpen(true);
        setSelectedColumns([]); // Reset selected columns when opening new asset
        setShowColumnCheckboxes(false); // Hide checkboxes when opening new asset
        setOriginalClassification(asset.business_metadata?.classification || 'internal');
        setOriginalSensitivityLevel(asset.business_metadata?.sensitivity_level || 'medium');
        setOriginalDepartment(asset.business_metadata?.department || 'Data Engineering');
        setClassification(asset.business_metadata?.classification || 'internal');
        setSensitivityLevel(asset.business_metadata?.sensitivity_level || 'medium');
        setDepartment(asset.business_metadata?.department || 'Data Engineering');
        
        // Set default description
        const defaultDesc = `Azure Blob Storage file: ${asset.name}`;
        setDefaultDescription(defaultDesc);
        
        const desc = asset.business_metadata?.description || '';
        setDescription(desc || defaultDesc);
        setOriginalDescription(desc);
      } else {
        const errorData = await response.json();
        alert(`Failed to load asset: ${errorData.error || 'Asset not found'}`);
      }
    } catch (error) {
      if (import.meta.env.DEV) {
        console.error('Error fetching asset:', error);
      }
      alert('Failed to load asset details. Please try again.');
    }
  };

  const handleCloseDialog = () => {
    setDetailsDialogOpen(false);
    setSelectedAsset(null);
    setActiveTab(0);
    setClassification('internal');
    setSensitivityLevel('medium');
    setDepartment('Data Engineering');
    setOriginalClassification('internal');
    setOriginalSensitivityLevel('medium');
    setOriginalDepartment('Data Engineering');
    setDescription('');
    setOriginalDescription('');
    setDefaultDescription('');
    setSelectedColumns([]); // Reset selected columns when dialog closes
    setShowColumnCheckboxes(false); // Hide checkboxes when dialog closes
  };

  // PII Dialog handlers
  const handleOpenPiiDialog = (column) => {
    if (!selectedAsset) return;
    setSelectedColumnForPii(column);
    setPiiDialogIsPii(column.pii_detected || false);
    setPiiDialogTypes(column.pii_types || []);
    setCustomPiiType('');
    // Track original PII status to detect changes (asset-scoped)
    const key = `${selectedAsset.id}_${column.name}`;
    setOriginalPiiStatus(prev => ({
      ...prev,
      [key]: column.pii_detected || false
    }));
    // Initialize masking logic from column data if available (asset-scoped)
    if (!columnMaskingLogic[key]) {
      setColumnMaskingLogic(prev => ({
        ...prev,
        [key]: {
          analytical: column.masking_logic_analytical || '',
          operational: column.masking_logic_operational || ''
        }
      }));
    }
    setPiiDialogOpen(true);
  };

  const handleClosePiiDialog = () => {
    setPiiDialogOpen(false);
    setSelectedColumnForPii(null);
    setPiiDialogIsPii(false);
    setPiiDialogTypes([]);
    setCustomPiiType('');
  };

  const handleAddCustomPiiType = () => {
    const trimmed = customPiiType.trim();
    if (trimmed && !piiDialogTypes.includes(trimmed) && !PII_TYPES.includes(trimmed)) {
      setPiiDialogTypes([...piiDialogTypes, trimmed]);
      setCustomPiiType('');
    }
  };

  const handleSavePii = async () => {
    if (!selectedAsset || !selectedColumnForPii) return;
    
    setSavingPii(true);
    try {
      const API_BASE_URL = import.meta.env.VITE_API_BASE_URL;
      const key = `${selectedAsset.id}_${selectedColumnForPii.name}`;
      const maskingLogic = columnMaskingLogic[key] || { analytical: '', operational: '' };
      const response = await fetch(
        `${API_BASE_URL}/api/assets/${selectedAsset.id}/columns/${selectedColumnForPii.name}/pii`,
        {
          method: 'PUT',
          headers: { 'Content-Type': 'application/json' },
          body: JSON.stringify({
            pii_detected: piiDialogIsPii,
            pii_types: piiDialogIsPii ? piiDialogTypes : null,
            masking_logic_analytical: piiDialogIsPii ? maskingLogic.analytical : null,
            masking_logic_operational: piiDialogIsPii ? maskingLogic.operational : null
          })
        }
      );
      
      if (response.ok) {
        const updatedAsset = { ...selectedAsset };
        updatedAsset.columns = updatedAsset.columns.map(c => 
          c.name === selectedColumnForPii.name 
            ? { 
                ...c, 
                pii_detected: piiDialogIsPii, 
                pii_types: piiDialogIsPii ? piiDialogTypes : null,
                masking_logic_analytical: piiDialogIsPii ? maskingLogic.analytical : null,
                masking_logic_operational: piiDialogIsPii ? maskingLogic.operational : null
              }
            : c
        );
        setSelectedAsset(updatedAsset);
        // Also update in assets list
        setAssets(prev => prev.map(a => 
          a.id === selectedAsset.id ? updatedAsset : a
        ));
        setAllAssets(prev => prev.map(a => 
          a.id === selectedAsset.id ? updatedAsset : a
        ));
        // Clear unsaved masking changes for this column (asset-scoped)
        setUnsavedMaskingChanges(prev => {
          const newState = { ...prev };
          delete newState[key];
          return newState;
        });
        handleClosePiiDialog();
      } else {
        throw new Error('Failed to update PII status');
      }
    } catch (err) {
      console.error('Failed to update PII status:', err);
      alert('Failed to update PII status: ' + err.message);
    } finally {
      setSavingPii(false);
    }
  };

  // Handler to add custom column
  const handleAddCustomColumn = async () => {
    if (!selectedAsset || !newColumnLabel.trim()) return;
    
    const columnId = `custom_${Date.now()}_${Math.random().toString(36).substr(2, 9)}`;
    const updatedCustomColumns = {
      ...customColumns,
      [columnId]: {
        label: newColumnLabel.trim(),
        values: {}
      }
    };
    
    setCustomColumns(updatedCustomColumns);
    
    try {
      const API_BASE_URL = import.meta.env.VITE_API_BASE_URL;
      const response = await fetch(`${API_BASE_URL}/api/assets/${selectedAsset.id}`, {
        method: 'PUT',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ custom_columns: updatedCustomColumns })
      });
      
      if (response.ok) {
        // Refresh asset
        const assetResponse = await fetch(`${API_BASE_URL}/api/assets/${selectedAsset.id}`);
        if (assetResponse.ok) {
          const updatedAsset = await assetResponse.json();
          setSelectedAsset(updatedAsset);
        }
        setAddColumnDialogOpen(false);
        setNewColumnLabel('');
      } else {
        throw new Error('Failed to add custom column');
      }
    } catch (err) {
      console.error('Failed to add custom column:', err);
      alert('Failed to add custom column: ' + err.message);
      // Revert on error
      setCustomColumns(customColumns);
    }
  };

  // Handler to save masking logic changes directly from table
  const handleSaveMaskingLogic = async (columnName) => {
    if (!selectedAsset) return;
    
    const column = selectedAsset.columns.find(c => c.name === columnName);
    if (!column || !column.pii_detected) return;
    
    // Use asset-scoped key
    const key = `${selectedAsset.id}_${columnName}`;
    const maskingLogic = columnMaskingLogic[key];
    if (!maskingLogic) return;
    
    setSavingMaskingLogic(prev => ({ ...prev, [key]: true }));
    try {
      const API_BASE_URL = import.meta.env.VITE_API_BASE_URL;
      const response = await fetch(
        `${API_BASE_URL}/api/assets/${selectedAsset.id}/columns/${columnName}/pii`,
        {
          method: 'PUT',
          headers: { 'Content-Type': 'application/json' },
          body: JSON.stringify({
            pii_detected: column.pii_detected,
            pii_types: column.pii_types || [],
            masking_logic_analytical: maskingLogic.analytical || null,
            masking_logic_operational: maskingLogic.operational || null
          })
        }
      );
      
      if (response.ok) {
        const updatedAsset = { ...selectedAsset };
        updatedAsset.columns = updatedAsset.columns.map(c => 
          c.name === columnName 
            ? { 
                ...c, 
                masking_logic_analytical: maskingLogic.analytical || null,
                masking_logic_operational: maskingLogic.operational || null
              }
            : c
        );
        setSelectedAsset(updatedAsset);
        // Also update in assets list
        setAssets(prev => prev.map(a => 
          a.id === selectedAsset.id ? updatedAsset : a
        ));
        setAllAssets(prev => prev.map(a => 
          a.id === selectedAsset.id ? updatedAsset : a
        ));
        // Clear unsaved changes flag (asset-scoped)
        setUnsavedMaskingChanges(prev => {
          const newState = { ...prev };
          delete newState[key];
          return newState;
        });
      } else {
        throw new Error('Failed to save masking logic');
      }
    } catch (err) {
      console.error('Failed to save masking logic:', err);
      alert('Failed to save masking logic: ' + err.message);
    } finally {
      setSavingMaskingLogic(prev => {
        const newState = { ...prev };
        delete newState[key];
        return newState;
      });
    }
  };

  // Available PII types
  const PII_TYPES = [
    'Email',
    'PhoneNumber',
    'SSN',
    'CreditCard',
    'PersonName',
    'Address',
    'DateOfBirth',
    'IPAddress',
    'AccountNumber',
    'CustomerID',
    'TransactionID',
    'UserID',
    'ID',
    'PassportNumber',
    'DriverLicense',
    'BankAccount',
    'MedicalRecord',
    'LicensePlate',
    'Password',
    'Gender',
    'Race',
    'Religion',
  ];

  // Masking logic options based on PII type
  const MASKING_OPTIONS = {
    'Email': {
      analytical: [
        { value: 'mask_domain', label: 'Mask domain (e.g., john.***@***.com)' },
        { value: 'mask_all', label: 'Mask all (***@***.com)' },
        { value: 'show_first_letter', label: 'Show first letter (j***@***.com)' },
        { value: 'hash', label: 'Hash entire email' },
        { value: 'redact', label: 'Redact completely' }
      ],
      operational: [
        { value: 'show_full', label: 'Show full email' },
        { value: 'mask_domain', label: 'Mask domain (e.g., john.***@***.com)' },
        { value: 'show_first_letter', label: 'Show first letter (j***@***.com)' },
        { value: 'partial_mask', label: 'Partial mask (j***@ex***.com)' }
      ]
    },
    'PhoneNumber': {
      analytical: [
        { value: 'mask_all', label: 'Mask all (***-***-****)' },
        { value: 'show_last_4', label: 'Show last 4 digits (***-***-1234)' },
        { value: 'hash', label: 'Hash entire number' },
        { value: 'redact', label: 'Redact completely' }
      ],
      operational: [
        { value: 'show_full', label: 'Show full number' },
        { value: 'show_last_4', label: 'Show last 4 digits (***-***-1234)' },
        { value: 'partial_mask', label: 'Partial mask (***-***-1234)' }
      ]
    },
    'SSN': {
      analytical: [
        { value: 'mask_all', label: 'Mask all (***-**-****)' },
        { value: 'show_last_4', label: 'Show last 4 digits (***-**-1234)' },
        { value: 'hash', label: 'Hash entire SSN' },
        { value: 'redact', label: 'Redact completely' }
      ],
      operational: [
        { value: 'show_full', label: 'Show full SSN' },
        { value: 'show_last_4', label: 'Show last 4 digits (***-**-1234)' },
        { value: 'mask_all', label: 'Mask all (***-**-****)' }
      ]
    },
    'CreditCard': {
      analytical: [
        { value: 'mask_all', label: 'Mask all (****-****-****-****)' },
        { value: 'show_last_4', label: 'Show last 4 digits (****-****-****-1234)' },
        { value: 'hash', label: 'Hash entire card number' },
        { value: 'redact', label: 'Redact completely' }
      ],
      operational: [
        { value: 'show_full', label: 'Show full card number' },
        { value: 'show_last_4', label: 'Show last 4 digits (****-****-****-1234)' },
        { value: 'mask_all', label: 'Mask all (****-****-****-****)' }
      ]
    },
    'PersonName': {
      analytical: [
        { value: 'mask_all', label: 'Mask all (***)' },
        { value: 'show_first_letter', label: 'Show first letter (J***)' },
        { value: 'show_initials', label: 'Show initials (J.D.)' },
        { value: 'hash', label: 'Hash entire name' },
        { value: 'redact', label: 'Redact completely' }
      ],
      operational: [
        { value: 'show_full', label: 'Show full name' },
        { value: 'show_first_letter', label: 'Show first letter (J***)' },
        { value: 'show_initials', label: 'Show initials (J.D.)' },
        { value: 'partial_mask', label: 'Partial mask (J*** D***)' }
      ]
    },
    'Address': {
      analytical: [
        { value: 'mask_all', label: 'Mask all (***)' },
        { value: 'show_city_only', label: 'Show city only' },
        { value: 'show_state_only', label: 'Show state only' },
        { value: 'hash', label: 'Hash entire address' },
        { value: 'redact', label: 'Redact completely' }
      ],
      operational: [
        { value: 'show_full', label: 'Show full address' },
        { value: 'show_city_only', label: 'Show city only' },
        { value: 'show_state_only', label: 'Show state only' },
        { value: 'partial_mask', label: 'Partial mask (*** Main St, City, ST)' }
      ]
    },
    'DateOfBirth': {
      analytical: [
        { value: 'mask_all', label: 'Mask all (***)' },
        { value: 'show_year_only', label: 'Show year only (****)' },
        { value: 'show_age_range', label: 'Show age range (20-30)' },
        { value: 'hash', label: 'Hash entire date' },
        { value: 'redact', label: 'Redact completely' }
      ],
      operational: [
        { value: 'show_full', label: 'Show full date' },
        { value: 'show_year_only', label: 'Show year only (****)' },
        { value: 'show_age_range', label: 'Show age range (20-30)' }
      ]
    },
    'IPAddress': {
      analytical: [
        { value: 'mask_all', label: 'Mask all (***.***.***.***)' },
        { value: 'show_first_octet', label: 'Show first octet (192.***.***.***)' },
        { value: 'hash', label: 'Hash entire IP' },
        { value: 'redact', label: 'Redact completely' }
      ],
      operational: [
        { value: 'show_full', label: 'Show full IP' },
        { value: 'show_first_octet', label: 'Show first octet (192.***.***.***)' },
        { value: 'partial_mask', label: 'Partial mask (192.168.***.***)' }
      ]
    },
    'default': {
      analytical: [
        { value: 'mask_all', label: 'Mask all' },
        { value: 'hash', label: 'Hash value' },
        { value: 'redact', label: 'Redact completely' }
      ],
      operational: [
        { value: 'show_full', label: 'Show full value' },
        { value: 'partial_mask', label: 'Partial mask' },
        { value: 'mask_all', label: 'Mask all' }
      ]
    }
  };

  // Helper function to get masking options for a column based on its PII types
  const getMaskingOptions = (column, userType) => {
    const piiTypes = column.pii_types || [];
    if (piiTypes.length === 0) {
      return MASKING_OPTIONS.default[userType] || [];
    }
    // Use the first PII type to determine masking options
    const firstPiiType = piiTypes[0];
    const options = MASKING_OPTIONS[firstPiiType]?.[userType] || MASKING_OPTIONS.default[userType] || [];
    return options;
  };

  const handleSaveMetadata = async () => {
    if (!selectedAsset) return;
    setSavingMetadata(true);
    try {
      const API_BASE_URL = import.meta.env.VITE_API_BASE_URL;
      
      const descToSave = description || defaultDescription;
      
      const response = await fetch(`${API_BASE_URL}/api/assets/${selectedAsset.id}`, {
        method: 'PUT',
        headers: {
          'Content-Type': 'application/json',
        },
        body: JSON.stringify({
          business_metadata: {
            ...selectedAsset.business_metadata,
            description: descToSave,
            classification: classification,
            sensitivity_level: sensitivityLevel,
            department: department,
          }
        }),
      });

      if (response.ok) {
        const updatedAsset = await response.json();
        
        setAllAssets(prev => prev.map(a => a.id === updatedAsset.id ? updatedAsset : a));
        setSelectedAsset(updatedAsset);
        setOriginalDescription(descToSave);
        setOriginalClassification(classification);
        setOriginalSensitivityLevel(sensitivityLevel);
        setOriginalDepartment(department);
      alert('Metadata saved successfully!');
      } else {
        throw new Error('Failed to save metadata');
      }
    } catch (error) {
      
      if (import.meta.env.DEV) {
      console.error('Error saving metadata:', error);
      }
      alert('Failed to save metadata. Please try again.');
    } finally {
      setSavingMetadata(false);
    }
  };

  const handleTabChange = (event, newValue) => {
    setActiveTab(newValue);
  };

  
  const handlePageChange = (event, page) => {
    setCurrentPage(page - 1); 
  };

  const handlePageSizeChange = (event) => {
    const newSize = parseInt(event.target.value, 10);
    setPageSize(newSize);
    setCurrentPage(0); 
  };

  
  const handleSearchChange = (event) => {
    setSearchTerm(event.target.value);
    setCurrentPage(0); 
  };

  const handleTypeFilterChange = (type) => {
    setTypeFilter(prev => {
      const newFilter = prev.includes(type) 
        ? prev.filter(t => t !== type)
        : [...prev, type];
      setCurrentPage(0);
      return newFilter;
    });
  };

  const handleCatalogFilterChange = (catalog) => {
    setCatalogFilter(prev => {
      const newFilter = prev.includes(catalog)
        ? prev.filter(c => c !== catalog)
        : [...prev, catalog];
      setCurrentPage(0);
      return newFilter;
    });
  };

  const handleApprovalStatusFilterChange = (status) => {
    setApprovalStatusFilter(prev => {
      const newFilter = prev.includes(status)
        ? prev.filter(s => s !== status)
        : [...prev, status];
      setCurrentPage(0);
      return newFilter;
    });
  };

  const handleApplicationNameFilterChange = (appName) => {
    setApplicationNameFilter(prev => {
      const newFilter = prev.includes(appName)
        ? prev.filter(a => a !== appName)
        : [...prev, appName];
      setCurrentPage(0);
      return newFilter;
    });
  };

  const handleViewDiscoveryDetails = async (discoveryId) => {
    try {
      const API_BASE_URL = import.meta.env.VITE_API_BASE_URL;
      const response = await fetch(`${API_BASE_URL}/api/discovery/${discoveryId}`);
      if (response.ok) {
        const discoveryData = await response.json();
        setDiscoveryDetails(discoveryData);
        setDiscoveryDetailsOpen(true);
      } else {
        const errorData = await response.json();
        alert(`Error: ${errorData.error || 'Failed to fetch discovery details'}`);
      }
    } catch (error) {
      console.error('Error fetching discovery details:', error);
      alert('Failed to fetch discovery details');
    }
  };

  const formatBytes = (bytes) => {
    if (!bytes) return '0 Bytes';
    const k = 1024;
    const sizes = ['Bytes', 'KB', 'MB', 'GB', 'TB'];
    const i = Math.floor(Math.log(bytes) / Math.log(k));
    return Math.round((bytes / Math.pow(k, i)) * 100) / 100 + ' ' + sizes[i];
  };

  const formatNumber = (num) => {
    if (!num) return '0';
    return num.toLocaleString();
  };

  const fetchHiddenDuplicates = async (page = hiddenDuplicatesPage) => {
    setHiddenDuplicatesLoading(true);
    try {
      const API_BASE_URL = import.meta.env.VITE_API_BASE_URL;
      const resp = await fetch(`${API_BASE_URL}/api/discovery/duplicates/hidden?page=${page}&per_page=${hiddenDuplicatesPerPage}`);
      const data = await resp.json().catch(() => ({}));
      if (!resp.ok) {
        throw new Error(data?.error || 'Failed to load hidden duplicates');
      }
      setHiddenDuplicates(Array.isArray(data?.hidden_duplicates) ? data.hidden_duplicates : []);
      setHiddenDuplicatesTotal(data?.total || 0);
      setHiddenDuplicatesTotalPages(data?.total_pages || 0);
      setHiddenDuplicatesPage(page);
    } catch (e) {
      console.error('Error loading hidden duplicates:', e);
      alert(`Error: ${e?.message || 'Failed to load hidden duplicates'}`);
      setHiddenDuplicates([]);
    } finally {
      setHiddenDuplicatesLoading(false);
    }
  };

  const pollDeduplicationStatus = async (jobId) => {
    const API_BASE_URL = import.meta.env.VITE_API_BASE_URL;
    const maxAttempts = 600; // 10 minutes max (1s intervals)
    let attempts = 0;
    
    const poll = async () => {
      try {
        const resp = await fetch(`${API_BASE_URL}/api/discovery/deduplicate/status/${jobId}`);
        const data = await resp.json().catch(() => ({}));
        
        if (!resp.ok) {
          throw new Error(data?.error || 'Failed to fetch job status');
        }
        
        setDeduplicationStatus(data);
        
        if (data.status === 'completed') {
          setDeduplicationProgressOpen(false);
          alert(`Deduplication complete. Hidden ${data.hidden_count || 0} duplicate asset(s).`);
          setCurrentPage(0);
          await fetchAssets(0);
          setDeduplicationJobId(null);
        } else if (data.status === 'failed') {
          setDeduplicationProgressOpen(false);
          alert(`Deduplication failed: ${data.error_message || 'Unknown error'}`);
          setDeduplicationJobId(null);
        } else if (data.status === 'running' || data.status === 'queued') {
          attempts++;
          if (attempts < maxAttempts) {
            setTimeout(poll, 1000); // Poll every 1 second
          } else {
            setDeduplicationProgressOpen(false);
            alert('Deduplication is taking longer than expected. Please check status manually.');
            setDeduplicationJobId(null);
          }
        }
      } catch (error) {
        console.error('Error polling deduplication status:', error);
        setDeduplicationProgressOpen(false);
        alert(`Error: ${error?.message || 'Failed to check deduplication status'}`);
        setDeduplicationJobId(null);
      }
    };
    
    poll();
  };

  const restoreHiddenDuplicate = async (discoveryId) => {
    try {
      setLoading(true);
      const API_BASE_URL = import.meta.env.VITE_API_BASE_URL;
      const resp = await fetch(`${API_BASE_URL}/api/discovery/${discoveryId}/restore`, {
        method: 'PUT',
        headers: { 'Content-Type': 'application/json' },
      });
      const data = await resp.json().catch(() => ({}));
      if (!resp.ok) {
        throw new Error(data?.error || 'Failed to restore');
      }
      await fetchHiddenDuplicates();
      setCurrentPage(0);
      await fetchAssets(0);
    } catch (e) {
      console.error('Error restoring hidden duplicate:', e);
      alert(`Error: ${e?.message || 'Failed to restore'}`);
    } finally {
      setLoading(false);
    }
  };

  return (
    <Box sx={{ p: 3 }}>
      <Box sx={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center', mb: 3 }}>
        <Typography variant="h4" component="h1" sx={{ fontWeight: 600, fontFamily: 'Comfortaa' }}>
          Discovered Assets
        </Typography>
        <Box sx={{ display: 'flex', gap: 2, alignItems: 'center' }}>
          <Tooltip title="Refresh the assets list to get the latest data from the database">
            <span>
              <Button
                variant="outlined"
                startIcon={<Refresh />}
                onClick={async () => {
                  try {
                    setLoading(true);
                const API_BASE_URL = import.meta.env.VITE_API_BASE_URL;
                
                
                const connectionsResponse = await fetch(`${API_BASE_URL}/api/connections`);
                if (!connectionsResponse.ok) {
                  throw new Error('Failed to fetch connections');
                }
                
                const connections = await connectionsResponse.json();
                const azureConnections = connections.filter(conn => conn.connector_type === 'azure_blob');
                
                if (azureConnections.length === 0) {
                  
                  await fetchAssets();
                  return;
                }
                
                
                const discoveryPromises = azureConnections.map(async (connection) => {
                  try {
                    const discoverResponse = await fetch(`${API_BASE_URL}/api/connections/${connection.id}/discover`, {
                      method: 'POST',
                      headers: {
                        'Content-Type': 'application/json',
                      },
                      body: JSON.stringify({
                        // IMPORTANT: do NOT send empty containers/folder_path; it overrides connection config.
                        skip_deduplication: false, // IMPORTANT: Set to false to enable refresh logic (check existing assets)
                      }),
                    });
                    
                    if (discoverResponse.ok) {
                      const result = await discoverResponse.json();
                      console.log(`Discovery complete for ${connection.name}:`, result);
                      return result;
                    } else {
                      console.warn(`Discovery failed for ${connection.name}`);
                      return null;
                    }
                  } catch (error) {
                    console.error(`Error discovering ${connection.name}:`, error);
                    return null;
                  }
                });
                
                
                await Promise.all(discoveryPromises);
                
                // Poll discovery progress for each connection to ensure discovery completes
                const pollDiscoveryProgress = async (connectionId, maxAttempts = 30) => {
                  for (let i = 0; i < maxAttempts; i++) {
                    try {
                      const progressResponse = await fetch(`${API_BASE_URL}/api/connections/${connectionId}/discover-progress`);
                      if (progressResponse.ok) {
                        const progress = await progressResponse.json();
                        // Only stop when the run is actually finished.
                        if (progress.status === 'done' || progress.status === 'error') {
                          return progress;
                        }
                      }
                    } catch (error) {
                      console.warn(`Error polling progress for connection ${connectionId}:`, error);
                    }
                    // Wait 2 seconds before next poll
                    await new Promise(resolve => setTimeout(resolve, 2000));
                  }
                  return { status: 'timeout' };
                };
                
                // Wait for all discoveries to complete
                const progressPromises = azureConnections.map(conn => pollDiscoveryProgress(conn.id));
                await Promise.all(progressPromises);
                
                try {
                  await fetch(`${API_BASE_URL}/api/discovery/trigger`, {
                    method: 'POST',
                    headers: {
                      'Content-Type': 'application/json',
                    },
                    body: JSON.stringify({})
                  });
                } catch (error) {
                  console.warn('Airflow DAG trigger failed (non-critical):', error);
                }
                
                // Additional wait to ensure database commits are complete
                await new Promise(resolve => setTimeout(resolve, 1000));
                
                // Force refresh - fetch fresh data with backend filtering
                // Reset to first page for refresh
                setCurrentPage(0);
                
                // Use fetchAssets which now handles backend filtering
                await fetchAssets(0);
              } catch (error) {
                console.error('Error refreshing:', error);
                
                await fetchAssets();
              } finally {
                setLoading(false);
              }
            }}
            disabled={loading}
          >
            Refresh
          </Button>
            </span>
          </Tooltip>
          <Tooltip title="Remove duplicate assets with identical column schemas. Only the latest modified asset is kept visible.">
            <span>
              <Button
                variant="outlined"
                startIcon={<DataObject />}
                endIcon={<ArrowDropDown />}
                onClick={(e) => setRemoveDuplicatesMenuAnchor(e.currentTarget)}
                disabled={loading}
              >
                Remove Duplicates
              </Button>
            </span>
          </Tooltip>
          <Menu
            anchorEl={removeDuplicatesMenuAnchor}
            open={Boolean(removeDuplicatesMenuAnchor)}
            onClose={() => setRemoveDuplicatesMenuAnchor(null)}
            anchorOrigin={{ vertical: 'bottom', horizontal: 'right' }}
            transformOrigin={{ vertical: 'top', horizontal: 'right' }}
          >
            <MenuItem
              onClick={async () => {
                setRemoveDuplicatesMenuAnchor(null);
                try {
                  setLoading(true);
                  const API_BASE_URL = import.meta.env.VITE_API_BASE_URL;
                  const resp = await fetch(`${API_BASE_URL}/api/discovery/deduplicate`, {
                    method: 'POST',
                    headers: { 'Content-Type': 'application/json' },
                  });
                  const payload = await resp.json().catch(() => ({}));
                  if (!resp.ok) {
                    throw new Error(payload?.error || 'Failed to deduplicate discoveries');
                  }
                  
                  // Check if async (job_id present) or sync (immediate result)
                  if (payload.job_id) {
                    // Async mode: start polling
                    setDeduplicationJobId(payload.job_id);
                    setDeduplicationStatus({
                      status: payload.status || 'queued',
                      total_discoveries: payload.total_discoveries || 0,
                      progress_percent: 0
                    });
                    setDeduplicationProgressOpen(true);
                    pollDeduplicationStatus(payload.job_id);
                  } else {
                    // Sync mode: immediate result
                    const hidden = payload?.hidden ?? 0;
                    alert(`Deduplication complete. Hidden ${hidden} duplicate asset(s).`);
                    setCurrentPage(0);
                    await fetchAssets(0);
                  }
                } catch (error) {
                  console.error('Error deduplicating discoveries:', error);
                  alert(`Error: ${error?.message || 'Failed to deduplicate discoveries'}`);
                } finally {
                  setLoading(false);
                }
              }}
            >
              <Tooltip title="Hide duplicate assets with identical column schemas. Only the latest modified asset will remain visible." placement="right">
                <span>Hide duplicates</span>
              </Tooltip>
            </MenuItem>
            <MenuItem
              onClick={async () => {
                setRemoveDuplicatesMenuAnchor(null);
                setHiddenDuplicatesOpen(true);
                setHiddenDuplicatesPage(1);
                await fetchHiddenDuplicates(1);
              }}
            >
              <Tooltip title="View and restore previously hidden duplicate assets" placement="right">
                <span>View hidden duplicates</span>
              </Tooltip>
            </MenuItem>
          </Menu>
        </Box>
      </Box>

      <Dialog
        open={deduplicationProgressOpen}
        onClose={() => {}}
        disableEscapeKeyDown
        fullWidth
        maxWidth="sm"
      >
        <DialogTitle>Deduplicating Assets</DialogTitle>
        <DialogContent>
          <Box sx={{ py: 2 }}>
            {deduplicationStatus && (
              <>
                <Typography variant="body1" gutterBottom>
                  Status: <strong>{deduplicationStatus.status === 'queued' ? 'Queued' : deduplicationStatus.status === 'running' ? 'Processing...' : deduplicationStatus.status}</strong>
                </Typography>
                {deduplicationStatus.total_discoveries > 0 && (
                  <Typography variant="body2" color="text.secondary" gutterBottom>
                    Processing {deduplicationStatus.total_discoveries.toLocaleString()} assets...
                  </Typography>
                )}
                {deduplicationStatus.progress_percent > 0 && (
                  <>
                    <Box sx={{ mt: 2, mb: 1 }}>
                      <LinearProgress variant="determinate" value={deduplicationStatus.progress_percent} sx={{ mb: 1 }} />
                      <Typography variant="body2" color="text.secondary">
                        {deduplicationStatus.progress_percent.toFixed(1)}% complete
                      </Typography>
                    </Box>
                    {deduplicationStatus.hidden_count > 0 && (
                      <Typography variant="body2" color="text.secondary">
                        Hidden so far: {deduplicationStatus.hidden_count.toLocaleString()}
                      </Typography>
                    )}
                  </>
                )}
                {deduplicationStatus.status === 'queued' && (
                  <Box sx={{ mt: 2 }}>
                    <CircularProgress />
                  </Box>
                )}
                {deduplicationStatus.status === 'running' && deduplicationStatus.progress_percent === 0 && (
                  <Box sx={{ mt: 2 }}>
                    <CircularProgress />
                  </Box>
                )}
              </>
            )}
          </Box>
        </DialogContent>
        <DialogActions>
          <Button onClick={() => {
            setDeduplicationProgressOpen(false);
            setDeduplicationJobId(null);
            setDeduplicationStatus(null);
          }} disabled={deduplicationStatus?.status === 'running'}>
            {deduplicationStatus?.status === 'running' ? 'Processing...' : 'Close'}
          </Button>
        </DialogActions>
      </Dialog>

      <Dialog
        open={starburstDialogOpen}
        onClose={handleCloseStarburstDialog}
        fullWidth
        maxWidth="md"
      >
        <DialogTitle>Ingest Masked View to Starburst Enterprise</DialogTitle>
        <DialogContent dividers>
          <Box sx={{ display: 'flex', flexDirection: 'column', gap: 2, mt: 1 }}>
            <Typography variant="body2" color="text.secondary">
              Configure your Starburst Enterprise connection and target view location. The generated view will apply masking based on this asset&apos;s PII configuration.
            </Typography>

            <Typography variant="subtitle2" sx={{ mt: 1 }}>Connection details</Typography>
            <Grid container spacing={2}>
              <Grid item xs={12} md={6}>
                <TextField
                  label="Host"
                  fullWidth
                  size="small"
                  value={starburstHost}
                  onChange={(e) => setStarburstHost(e.target.value)}
                  placeholder="starburst.mycompany.com"
                />
              </Grid>
              <Grid item xs={6} md={3}>
                <TextField
                  label="Port"
                  fullWidth
                  size="small"
                  value={starburstPort}
                  onChange={(e) => setStarburstPort(e.target.value)}
                  placeholder="443"
                />
              </Grid>
              <Grid item xs={6} md={3}>
                <TextField
                  label="HTTP Scheme"
                  fullWidth
                  size="small"
                  value={starburstHttpScheme}
                  onChange={(e) => setStarburstHttpScheme(e.target.value)}
                  placeholder="https"
                  helperText="e.g. https or http"
                />
              </Grid>
              <Grid item xs={12} md={6}>
                <TextField
                  label="User"
                  fullWidth
                  size="small"
                  value={starburstUser}
                  onChange={(e) => setStarburstUser(e.target.value)}
                />
              </Grid>
              <Grid item xs={12} md={6}>
                <TextField
                  label="Password"
                  type="password"
                  fullWidth
                  size="small"
                  value={starburstPassword}
                  onChange={(e) => setStarburstPassword(e.target.value)}
                />
              </Grid>
            </Grid>

            <Typography variant="subtitle2" sx={{ mt: 2 }}>Target view location</Typography>
            <Grid container spacing={2}>
              <Grid item xs={12} md={4}>
                <TextField
                  label="Catalog"
                  fullWidth
                  size="small"
                  value={starburstCatalog}
                  onChange={(e) => setStarburstCatalog(e.target.value)}
                  placeholder="lz_lakehouse"
                />
              </Grid>
              <Grid item xs={12} md={4}>
                <TextField
                  label="Schema"
                  fullWidth
                  size="small"
                  value={starburstSchema}
                  onChange={(e) => setStarburstSchema(e.target.value)}
                  placeholder="en_visionplus"
                />
              </Grid>
              <Grid item xs={12} md={4}>
                <TextField
                  label="Base Table Name"
                  fullWidth
                  size="small"
                  value={starburstTableName}
                  onChange={(e) => setStarburstTableName(e.target.value)}
                  placeholder={starburstAsset?.name || ''}
                  helperText="Starburst table to mask (FROM clause)"
                />
              </Grid>
              <Grid item xs={12} md={6}>
                <TextField
                  label="View Name"
                  fullWidth
                  size="small"
                  value={starburstViewName}
                  onChange={(e) => setStarburstViewName(e.target.value)}
                  placeholder={`${starburstAsset?.name || 'table'}_masked`}
                  helperText="Masked view name to create"
                />
              </Grid>
            </Grid>

            {starburstError && (
              <Alert severity="error" sx={{ mt: 2 }}>
                {starburstError}
              </Alert>
            )}
            {starburstSuccess && (
              <Alert severity="success" sx={{ mt: 2 }}>
                {starburstSuccess}
              </Alert>
            )}

            <Box sx={{ mt: 2 }}>
              <Typography variant="subtitle2" sx={{ mb: 1 }}>
                Generated masking view SQL
              </Typography>
              <TextField
                value={starburstViewSql}
                onChange={() => {}}
                fullWidth
                multiline
                minRows={6}
                maxRows={18}
                size="small"
                placeholder="Click Generate SQL to see the Starburst view definition"
                InputProps={{
                  readOnly: true,
                  sx: {
                    fontFamily: 'monospace',
                    fontSize: 12,
                  },
                }}
              />
            </Box>
          </Box>
        </DialogContent>
        <DialogActions>
          <Button onClick={handleCloseStarburstDialog} disabled={starburstLoading}>
            Close
          </Button>
          <Button onClick={handleStarburstPreview} disabled={starburstLoading}>
            {starburstLoading ? 'Generating...' : 'Generate SQL'}
          </Button>
          <Button
            variant="contained"
            color="primary"
            startIcon={<CloudUpload />}
            onClick={handleStarburstIngest}
            disabled={starburstLoading || !starburstViewSql}
          >
            {starburstLoading ? 'Ingesting...' : 'Ingest to Starburst'}
          </Button>
        </DialogActions>
      </Dialog>
      <Dialog
        open={hiddenDuplicatesOpen}
        onClose={() => setHiddenDuplicatesOpen(false)}
        fullWidth
        maxWidth="lg"
      >
        <DialogTitle>Hidden Duplicates</DialogTitle>
        <DialogContent>
          {hiddenDuplicatesLoading ? (
            <Box sx={{ display: 'flex', justifyContent: 'center', py: 4 }}>
              <CircularProgress />
            </Box>
          ) : hiddenDuplicates.length === 0 ? (
            <Alert severity="info">No hidden duplicates.</Alert>
          ) : (
            <TableContainer component={Paper} variant="outlined">
              <Table size="small">
                <TableHead>
                  <TableRow>
                    <TableCell>Name</TableCell>
                    <TableCell>Type</TableCell>
                    <TableCell>File</TableCell>
                    <TableCell>Last Modified</TableCell>
                    <TableCell>Storage Path</TableCell>
                    <TableCell align="right">Action</TableCell>
                  </TableRow>
                </TableHead>
                <TableBody>
                  {hiddenDuplicates.map((row) => (
                    <TableRow key={row.discovery_id}>
                      <TableCell>{row.asset_name || '—'}</TableCell>
                      <TableCell>{row.asset_type || '—'}</TableCell>
                      <TableCell>{row.file_name || '—'}</TableCell>
                      <TableCell>
                        {row.file_last_modified ? new Date(row.file_last_modified).toLocaleString() : '—'}
                      </TableCell>
                      <TableCell sx={{ maxWidth: 420, whiteSpace: 'nowrap', overflow: 'hidden', textOverflow: 'ellipsis' }}>
                        {row.storage_path || '—'}
                      </TableCell>
                      <TableCell align="right">
                        <Tooltip title="Restore this hidden duplicate asset to make it visible again">
                          <span>
                            <Button
                              variant="contained"
                              size="small"
                              onClick={() => restoreHiddenDuplicate(row.discovery_id)}
                              disabled={loading}
                            >
                              Restore
                            </Button>
                          </span>
                        </Tooltip>
                      </TableCell>
                    </TableRow>
                  ))}
                </TableBody>
              </Table>
            </TableContainer>
          )}
        </DialogContent>
        <DialogActions sx={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center', px: 2, py: 1 }}>
          <Box>
            <Typography variant="body2" color="text.secondary">
              Showing {hiddenDuplicates.length > 0 ? ((hiddenDuplicatesPage - 1) * hiddenDuplicatesPerPage + 1) : 0} - {Math.min(hiddenDuplicatesPage * hiddenDuplicatesPerPage, hiddenDuplicatesTotal)} of {hiddenDuplicatesTotal}
            </Typography>
          </Box>
          <Box sx={{ display: 'flex', gap: 1, alignItems: 'center' }}>
            <Pagination
              count={hiddenDuplicatesTotalPages}
              page={hiddenDuplicatesPage}
              onChange={(e, newPage) => fetchHiddenDuplicates(newPage)}
              color="primary"
              size="small"
              showFirstButton
              showLastButton
            />
            <Button onClick={() => setHiddenDuplicatesOpen(false)}>Close</Button>
          </Box>
        </DialogActions>
      </Dialog>

      <Card sx={{ mb: 3 }}>
        <CardContent>
          <Grid container spacing={2} alignItems="center">
            <Grid item xs={12} md={2}>
                <TextField
                  fullWidth
                  placeholder="Search assets..."
                  value={searchTerm}
                  onChange={handleSearchChange}
                  InputProps={{
                    startAdornment: (
                      <InputAdornment position="start">
                        <Search />
                      </InputAdornment>
                    ),
                  }}
                />
            </Grid>
            <Grid item xs={12} md={2}>
              <Tooltip title="Filter assets by data type (e.g., Table, View, File)">
                <span>
                  <Button
                    fullWidth
                    variant="outlined"
                    onClick={(e) => setTypeMenuAnchor(e.currentTarget)}
                    sx={{ justifyContent: 'space-between', textTransform: 'none' }}
                  >
                    {typeFilter.length === 0 ? 'All Types' : `${typeFilter.length} Selected`}
                    <FilterList fontSize="small" />
                  </Button>
                </span>
              </Tooltip>
              <Menu
                anchorEl={typeMenuAnchor}
                open={Boolean(typeMenuAnchor)}
                onClose={() => setTypeMenuAnchor(null)}
                PaperProps={{
                  style: {
                    maxHeight: 300,
                    width: 200,
                  },
                }}
              >
                {uniqueTypes.map(type => (
                  <MenuItem key={type} onClick={() => handleTypeFilterChange(type)}>
                    <Checkbox checked={typeFilter.includes(type)} />
                    <ListItemText primary={type} />
                  </MenuItem>
                ))}
              </Menu>
            </Grid>
            <Grid item xs={12} md={2.5}>
              <Tooltip title="Filter assets by catalog/database name">
                <span>
                  <Button
                    fullWidth
                    variant="outlined"
                    onClick={(e) => setCatalogMenuAnchor(e.currentTarget)}
                    sx={{ justifyContent: 'space-between', textTransform: 'none' }}
                  >
                    {catalogFilter.length === 0 ? 'All Catalogs' : `${catalogFilter.length} Selected`}
                    <FilterList fontSize="small" />
                  </Button>
                </span>
              </Tooltip>
              <Menu
                anchorEl={catalogMenuAnchor}
                open={Boolean(catalogMenuAnchor)}
                onClose={() => setCatalogMenuAnchor(null)}
                PaperProps={{
                  style: {
                    maxHeight: 300,
                    width: 200,
                  },
                }}
              >
                {uniqueCatalogs.map(catalog => (
                  <MenuItem key={catalog} onClick={() => handleCatalogFilterChange(catalog)}>
                    <Checkbox checked={catalogFilter.includes(catalog)} />
                    <ListItemText primary={catalog} />
                  </MenuItem>
                ))}
              </Menu>
            </Grid>
            <Grid item xs={12} md={2}>
              <Tooltip title="Filter assets by approval status (Pending Review, Approved, Rejected)">
                <span>
                  <Button
                    fullWidth
                    variant="outlined"
                    onClick={(e) => setStatusMenuAnchor(e.currentTarget)}
                    sx={{ justifyContent: 'space-between', textTransform: 'none' }}
                  >
                    {approvalStatusFilter.length === 0 ? 'All Statuses' : `${approvalStatusFilter.length} Selected`}
                    <FilterList fontSize="small" />
                  </Button>
                </span>
              </Tooltip>
              <Menu
                anchorEl={statusMenuAnchor}
                open={Boolean(statusMenuAnchor)}
                onClose={() => setStatusMenuAnchor(null)}
                PaperProps={{
                  style: {
                    maxHeight: 300,
                    width: 350,
                  },
                }}
              >
                <MenuItem onClick={() => handleApprovalStatusFilterChange('pending_review')}>
                  <Checkbox checked={approvalStatusFilter.includes('pending_review')} />
                  <ListItemText primary="Pending Review" />
                </MenuItem>
                <MenuItem onClick={() => handleApprovalStatusFilterChange('approved')}>
                  <Checkbox checked={approvalStatusFilter.includes('approved')} />
                  <ListItemText primary="Approved" />
                </MenuItem>
                <MenuItem onClick={() => handleApprovalStatusFilterChange('rejected')}>
                  <Checkbox checked={approvalStatusFilter.includes('rejected')} />
                  <ListItemText primary="Rejected" />
                </MenuItem>
              </Menu>
            </Grid>
            <Grid item xs={12} md={2}>
              <Tooltip title="Filter assets by application name">
                <span>
                  <Button
                    fullWidth
                    variant="outlined"
                    onClick={(e) => setApplicationMenuAnchor(e.currentTarget)}
                    sx={{ justifyContent: 'space-between', textTransform: 'none' }}
                  >
                    {applicationNameFilter.length === 0 ? 'All Applications' : `${applicationNameFilter.length} Selected`}
                    <FilterList fontSize="small" />
                  </Button>
                </span>
              </Tooltip>
              <Menu
                anchorEl={applicationMenuAnchor}
                open={Boolean(applicationMenuAnchor)}
                onClose={() => setApplicationMenuAnchor(null)}
                PaperProps={{
                  style: {
                    maxHeight: 300,
                    width: 400,
                  },
                }}
              >
                {uniqueApplicationNames.map(appName => (
                  <MenuItem key={appName} onClick={() => handleApplicationNameFilterChange(appName)}>
                    <Checkbox checked={applicationNameFilter.includes(appName)} />
                    <ListItemText primary={appName} />
                  </MenuItem>
                ))}
              </Menu>
            </Grid>
            <Grid item xs={12} md={1.5}>
              <Tooltip title="Clear all filters and search terms">
                <span>
                  <Button
                    fullWidth
                    variant="outlined"
                    startIcon={<FilterList />}
                    onClick={() => {
                      setSearchTerm('');
                      setTypeFilter([]);
                      setCatalogFilter([]);
                      setApprovalStatusFilter([]);
                      setApplicationNameFilter([]);
                      setCurrentPage(0);
                    }}
                  >
                    Clear
                  </Button>
                </span>
              </Tooltip>
            </Grid>
            <Grid item xs={12} md={1}>
              <Tooltip title="Configure which metadata fields are visible in the asset details">
                <span>
                  <Button
                    fullWidth
                    variant="outlined"
                    startIcon={<Settings />}
                    onClick={() => setMetadataSettingsOpen(true)}
                    sx={{ textTransform: 'none' }}
                  >
                    Settings
                  </Button>
                </span>
              </Tooltip>
            </Grid>
          </Grid>
        </CardContent>
      </Card>

      <Card>
        <CardContent>
          <TableContainer sx={{ maxHeight: 'none' }}>
            <Table>
              <TableHead>
                <TableRow>
                  <TableCell sx={{ width: '60px', fontWeight: 600 }}>#</TableCell>
                  <TableCell>Name</TableCell>
                  <TableCell>Type</TableCell>
                  <TableCell>File Size</TableCell>
                  <TableCell>Catalog</TableCell>
                  <TableCell>Application Name</TableCell>
                  <TableCell>Data Source</TableCell>
                  <TableCell>Discovered</TableCell>
                  <TableCell>Actions</TableCell>
                </TableRow>
              </TableHead>
              <TableBody>
                {assets.length === 0 && !loading ? (
                  <TableRow>
                    <TableCell colSpan={9} sx={{ textAlign: 'center', py: 8 }}>
                      <Box sx={{ display: 'flex', flexDirection: 'column', alignItems: 'center', gap: 2 }}>
                        <FolderOpen sx={{ fontSize: 64, color: 'text.secondary', opacity: 0.5 }} />
                        <Typography variant="h6" color="text.secondary" sx={{ fontWeight: 500 }}>
                          No Assets Discovered
                        </Typography>
                        <Typography variant="body2" color="text.secondary" sx={{ maxWidth: 500, textAlign: 'center' }}>
                          Start discovering assets by creating a connection and running discovery. Go to Connectors to set up your data sources.
                        </Typography>
                        <Button
                          variant="contained"
                          color="primary"
                          startIcon={<DataObject />}
                          endIcon={<ArrowForward />}
                          onClick={() => navigate('/connectors')}
                          sx={{ mt: 1 }}
                        >
                          Go to Connectors
                        </Button>
                      </Box>
                    </TableCell>
                  </TableRow>
                ) : (
                  assets.map((asset, index) => (
                    <TableRow key={asset.id}>
                      <TableCell sx={{ fontWeight: 500, color: 'text.secondary' }}>
                        {currentPage * pageSize + index + 1}
                      </TableCell>
                      <TableCell>
                        <Box sx={{ display: 'flex', alignItems: 'center' }}>
                          <DataObject sx={{ mr: 1, color: 'text.secondary' }} />
                          <Typography variant="body2" sx={{ fontWeight: 500, fontFamily: 'Roboto' }}>
                            {asset.name}
                          </Typography>
                        </Box>
                      </TableCell>
                      <TableCell>
                        <Chip 
                          label={asset.type} 
                          size="small" 
                          variant="outlined"
                        />
                      </TableCell>
                      <TableCell>
                        <Typography variant="body2" sx={{ fontFamily: 'Roboto', fontSize: '0.875rem' }}>
                          {(() => {
                            const sizeBytes = asset.technical_metadata?.size_bytes || asset.technical_metadata?.size || 0;
                            return formatBytes(sizeBytes);
                          })()}
                        </Typography>
                      </TableCell>
                      <TableCell>
                        <Typography variant="body2" sx={{ fontFamily: 'Roboto' }}>
                          {asset.catalog}
                        </Typography>
                      </TableCell>
                      <TableCell>
                        <Typography variant="body2" sx={{ fontFamily: 'Roboto' }}>
                          {asset.application_name || 'N/A'}
                        </Typography>
                      </TableCell>
                      <TableCell>
                        <Chip 
                          label={getDataSource(asset.connector_id)} 
                          size="small" 
                          color={getDataSourceColor(asset.connector_id)}
                        />
                      </TableCell>
                      <TableCell>
                        <Box>
                          <Typography variant="body2" color="text.secondary" sx={{ fontFamily: 'Roboto' }}>
                            {new Date(asset.discovered_at).toLocaleDateString()}
                          </Typography>
                          {asset.discovery_id && (
                            <Typography variant="caption" color="text.secondary" sx={{ fontFamily: 'Roboto', fontSize: '0.75rem', opacity: 0.7 }}>
                              ID: {asset.discovery_id}
                            </Typography>
                          )}
                        </Box>
                      </TableCell>
                      <TableCell>
                        <Box sx={{ display: 'flex', gap: 1, alignItems: 'center' }}>
                          <Tooltip title="View detailed information about this asset">
                            <span>
                              <Button
                                size="small"
                                startIcon={<Visibility />}
                                variant="outlined"
                                onClick={() => handleViewAsset(asset.id)}
                              >
                                View
                              </Button>
                            </span>
                          </Tooltip>
                          <Tooltip title="Ingest a masked analytical view for this asset into Starburst Enterprise">
                            <span>
                              <Button
                                size="small"
                                startIcon={<CloudUpload />}
                                variant="outlined"
                                onClick={() => handleOpenStarburstDialog(asset)}
                              >
                                Ingest
                              </Button>
                            </span>
                          </Tooltip>
                          {(asset.operational_metadata?.approval_status === 'pending_review' || 
                            !asset.operational_metadata?.approval_status ||
                            asset.operational_metadata?.approval_status === 'pending') && (
                            <>
                              <Tooltip title="Approve this asset for use">
                                <span>
                                  <Button
                                    size="small"
                                    startIcon={<ThumbUp />}
                                    variant="contained"
                                    color="success"
                                    onClick={() => handleApproveAsset(asset.id)}
                                  >
                                    Approve
                                  </Button>
                                </span>
                              </Tooltip>
                              <Tooltip title="Reject this asset and provide a reason">
                                <span>
                                  <Button
                                    size="small"
                                    startIcon={<ThumbDown />}
                                    variant="contained"
                                    color="error"
                                    onClick={() => handleRejectClick(asset.id)}
                                  >
                                    Reject
                                  </Button>
                                </span>
                              </Tooltip>
                            </>
                          )}
                          {asset.operational_metadata?.approval_status === 'approved' && (
                            <>
                            <Tooltip title="This asset has been approved for use">
                              <Chip
                                icon={<CheckCircle />}
                                label="Approved"
                                color="success"
                                size="small"
                              />
                            </Tooltip>
                              <Tooltip title={asset.operational_metadata?.publish_status === 'published' ? 'Republish this asset to update its published version' : 'Publish this approved asset'}>
                                <span>
                                  <Button
                                    size="small"
                                    startIcon={<Publish />}
                                    variant="contained"
                                    color="primary"
                                    onClick={() => handlePublishAsset(asset.id)}
                                    disabled={publishing}
                                  >
                                    {asset.operational_metadata?.publish_status === 'published' ? 'Republish' : 'Publish'}
                                  </Button>
                                </span>
                              </Tooltip>
                            </>
                          )}
                          {asset.operational_metadata?.approval_status === 'rejected' && (
                            <Box sx={{ display: 'flex', flexDirection: 'column', gap: 0.5 }}>
                              <Tooltip title="This asset has been rejected. See reason below.">
                                <Chip
                                  icon={<Close />}
                                  label="Rejected"
                                  color="error"
                                  size="small"
                                />
                              </Tooltip>
                              {asset.operational_metadata?.rejection_reason && (
                                <Typography variant="caption" color="error" sx={{ fontSize: '0.7rem', fontStyle: 'italic', maxWidth: '250px', wordWrap: 'break-word' }}>
                                  Reason: {asset.operational_metadata.rejection_reason}
                                </Typography>
                              )}
                            </Box>
                          )}
                        </Box>
                      </TableCell>
                    </TableRow>
                  ))
                )}
              </TableBody>
            </Table>
          </TableContainer>
          
          {}
          <Box sx={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center', mt: 3, px: 2 }}>
            <Box sx={{ display: 'flex', alignItems: 'center', gap: 2 }}>
              <Typography variant="body2" color="text.secondary">
                Showing {assets.length} of {totalAssets} assets
              </Typography>
              <FormControl size="small" sx={{ minWidth: 80 }}>
                <Select
                  value={pageSize}
                  onChange={handlePageSizeChange}
                  displayEmpty
                >
                  <MenuItem value={25}>25</MenuItem>
                  <MenuItem value={50}>50</MenuItem>
                  <MenuItem value={100}>100</MenuItem>
                </Select>
              </FormControl>
              <Typography variant="body2" color="text.secondary">
                per page
              </Typography>
            </Box>
            
            <Box sx={{ display: 'flex', alignItems: 'center', gap: 2 }}>
            <Pagination
              count={totalPages}
              page={currentPage + 1}
              onChange={handlePageChange}
              color="primary"
              showFirstButton
              showLastButton
              disabled={loading}
            />
              <Typography variant="body2" color="text.secondary">
                Total: {totalAssets} assets
              </Typography>
            </Box>
          </Box>
        </CardContent>
      </Card>

      {}
      <Dialog
        open={detailsDialogOpen}
        onClose={handleCloseDialog}
        maxWidth="xl"
        fullWidth
        sx={{
          '& .MuiDialog-paper': {
            width: '95%',
            maxWidth: '1400px',
          }
        }}
      >
        {!selectedAsset ? (
          <Box sx={{ p: 4, display: 'flex', flexDirection: 'column', justifyContent: 'center', alignItems: 'center', minHeight: 300, gap: 2 }}>
            <CircularProgress />
            <Typography>Loading asset details...</Typography>
          </Box>
        ) : (
          <>
            <DialogTitle>
              <Box sx={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center' }}>
                <Box>
                  <Typography variant="h5" sx={{ fontWeight: 600 }}>
                    {selectedAsset.name}
                  </Typography>
                  <Box sx={{ display: 'flex', gap: 1, mt: 1 }}>
                    <Chip label={selectedAsset.type} size="small" color="primary" />
                    <Chip label={selectedAsset.catalog} size="small" variant="outlined" />
                  </Box>
                </Box>
                <Button onClick={handleCloseDialog} startIcon={<Close />}>
                  Close
                </Button>
              </Box>
            </DialogTitle>
            <DialogContent dividers>
              <Tabs value={activeTab} onChange={handleTabChange} sx={{ mb: 3 }}>
                <Tab label="Technical Metadata" />
                <Tab label="Operational Metadata" />
                <Tab label="Business Metadata" />
                <Tab label="Columns & PII" />
              </Tabs>

              {}
              {activeTab === 0 && (
                <Box>
                  <Typography variant="h6" sx={{ mb: 2, fontWeight: 600 }}>
                    Technical Metadata
                  </Typography>
                  {(() => {
                    
                    const technicalMetadata = selectedAsset?.technical_metadata || {};
                    const safeAssetId = technicalMetadata.asset_id || selectedAsset?.id || 'N/A';
                    const connectorId = selectedAsset?.connector_id || '';
                    
                    // Check if this is an Oracle DB asset
                    const isOracleDB = technicalMetadata.database_type === 'oracle' || connectorId.startsWith('oracle_db');
                    
                    if (isOracleDB) {
                      // Oracle DB specific metadata
                      const schema = technicalMetadata.schema || selectedAsset?.catalog || 'N/A';
                      const tableName = technicalMetadata.table_name || 'N/A';
                      const viewName = technicalMetadata.view_name || 'N/A';
                      const procedureName = technicalMetadata.procedure_name || 'N/A';
                      const functionName = technicalMetadata.function_name || 'N/A';
                      const objectName = technicalMetadata.object_name || 'N/A';
                      const tablespace = technicalMetadata.tablespace || 'N/A';
                      const lastAnalyzed = technicalMetadata.last_analyzed || null;
                      const viewDefinition = technicalMetadata.view_definition || null;
                      const sourceCode = technicalMetadata.source_code || null;
                      const mviewName = technicalMetadata.mview_name || null;
                      const lastRefreshDate = technicalMetadata.last_refresh_date || null;
                      const assetType = selectedAsset?.type || 'N/A';
                      
                      return (
                        <Grid container spacing={2}>
                          {metadataVisibility.technical['Asset ID'] && (
                            <Grid item xs={6}>
                              <Card variant="outlined">
                                <CardContent>
                                  <Typography color="text.secondary" gutterBottom>
                                    Asset ID
                                  </Typography>
                                  <Typography variant="body1" sx={{ wordBreak: 'break-all', fontSize: '0.875rem' }}>
                                    {safeAssetId}
                                  </Typography>
                                </CardContent>
                              </Card>
                            </Grid>
                          )}
                          
                          {metadataVisibility.technical['Database Type'] && (
                            <Grid item xs={6}>
                              <Card variant="outlined">
                                <CardContent>
                                  <Typography color="text.secondary" gutterBottom>
                                    Database Type
                                  </Typography>
                                  <Typography variant="body1">
                                    Oracle Database
                                  </Typography>
                                </CardContent>
                              </Card>
                            </Grid>
                          )}
                          
                          {metadataVisibility.technical['Schema'] && (
                            <Grid item xs={6}>
                              <Card variant="outlined">
                                <CardContent>
                                  <Typography color="text.secondary" gutterBottom>
                                    Schema
                                  </Typography>
                                  <Typography variant="body1">
                                    {schema}
                                  </Typography>
                                </CardContent>
                              </Card>
                            </Grid>
                          )}
                          
                          {metadataVisibility.technical['Object Type'] && (
                            <Grid item xs={6}>
                              <Card variant="outlined">
                                <CardContent>
                                  <Typography color="text.secondary" gutterBottom>
                                    Object Type
                                  </Typography>
                                  <Typography variant="body1">
                                    {assetType.charAt(0).toUpperCase() + assetType.slice(1).replace('_', ' ')}
                                  </Typography>
                                </CardContent>
                              </Card>
                            </Grid>
                          )}
                          
                          {(tableName !== 'N/A' || viewName !== 'N/A' || procedureName !== 'N/A' || functionName !== 'N/A' || mviewName) && 
                           ((assetType === 'table' && metadataVisibility.technical['Table Name']) ||
                            (assetType === 'view' && metadataVisibility.technical['View Name']) ||
                            (assetType !== 'table' && assetType !== 'view')) && (
                            <Grid item xs={6}>
                              <Card variant="outlined">
                                <CardContent>
                                  <Typography color="text.secondary" gutterBottom>
                                    {assetType === 'table' ? 'Table Name' : 
                                     assetType === 'view' ? 'View Name' : 
                                     assetType === 'materialized_view' ? 'Materialized View Name' :
                                     assetType === 'procedure' ? 'Procedure Name' :
                                     assetType === 'function' ? 'Function Name' : 'Object Name'}
                                  </Typography>
                                  <Typography variant="body1">
                                    {tableName !== 'N/A' ? tableName : 
                                     viewName !== 'N/A' ? viewName : 
                                     mviewName || procedureName !== 'N/A' ? procedureName : 
                                     functionName !== 'N/A' ? functionName : objectName}
                                  </Typography>
                                </CardContent>
                              </Card>
                            </Grid>
                          )}
                          
                          {tablespace !== 'N/A' && (
                            <Grid item xs={6}>
                              <Card variant="outlined">
                                <CardContent>
                                  <Typography color="text.secondary" gutterBottom>
                                    Tablespace
                                  </Typography>
                                  <Typography variant="body1">
                                    {tablespace}
                                  </Typography>
                                </CardContent>
                              </Card>
                            </Grid>
                          )}
                          
                          {technicalMetadata.blocks > 0 && (
                            <Grid item xs={6}>
                              <Card variant="outlined">
                                <CardContent>
                                  <Typography color="text.secondary" gutterBottom>
                                    Blocks
                                  </Typography>
                                  <Typography variant="body1">
                                    {formatNumber(technicalMetadata.blocks)}
                                  </Typography>
                                </CardContent>
                              </Card>
                            </Grid>
                          )}
                          
                          {technicalMetadata.avg_row_length > 0 && (
                            <Grid item xs={6}>
                              <Card variant="outlined">
                                <CardContent>
                                  <Typography color="text.secondary" gutterBottom>
                                    Avg Row Length
                                  </Typography>
                                  <Typography variant="body1">
                                    {formatNumber(technicalMetadata.avg_row_length)} bytes
                                  </Typography>
                                </CardContent>
                              </Card>
                            </Grid>
                          )}
                          
                          {lastAnalyzed && (
                            <Grid item xs={6}>
                              <Card variant="outlined">
                                <CardContent>
                                  <Typography color="text.secondary" gutterBottom>
                                    Last Analyzed
                                  </Typography>
                                  <Typography variant="body1">
                                    {new Date(lastAnalyzed).toLocaleString()}
                                  </Typography>
                                </CardContent>
                              </Card>
                            </Grid>
                          )}
                          
                          {lastRefreshDate && (
                            <Grid item xs={6}>
                              <Card variant="outlined">
                                <CardContent>
                                  <Typography color="text.secondary" gutterBottom>
                                    Last Refresh Date
                                  </Typography>
                                  <Typography variant="body1">
                                    {new Date(lastRefreshDate).toLocaleString()}
                                  </Typography>
                                </CardContent>
                              </Card>
                            </Grid>
                          )}
                          
                          {viewDefinition && (
                            <Grid item xs={12}>
                              <Card variant="outlined">
                                <CardContent>
                                  <Typography color="text.secondary" gutterBottom>
                                    View Definition
                                  </Typography>
                                  <Typography variant="body2" sx={{ 
                                    fontFamily: 'monospace', 
                                    fontSize: '0.75rem',
                                    whiteSpace: 'pre-wrap',
                                    wordBreak: 'break-all',
                                    maxHeight: '200px',
                                    overflow: 'auto',
                                    backgroundColor: '#f5f5f5',
                                    padding: 1,
                                    borderRadius: 1
                                  }}>
                                    {viewDefinition}
                                  </Typography>
                                </CardContent>
                              </Card>
                            </Grid>
                          )}
                          
                          {objectName !== 'N/A' && assetType !== 'table' && assetType !== 'view' && (
                            <Grid item xs={6}>
                              <Card variant="outlined">
                                <CardContent>
                                  <Typography color="text.secondary" gutterBottom>
                                    Object Name
                                  </Typography>
                                  <Typography variant="body1">
                                    {objectName}
                                  </Typography>
                                </CardContent>
                              </Card>
                            </Grid>
                          )}
                          
                          {technicalMetadata.procedure_name_in_package && (
                            <Grid item xs={6}>
                              <Card variant="outlined">
                                <CardContent>
                                  <Typography color="text.secondary" gutterBottom>
                                    Package Procedure
                                  </Typography>
                                  <Typography variant="body1">
                                    {technicalMetadata.procedure_name_in_package}
                                  </Typography>
                                </CardContent>
                              </Card>
                            </Grid>
                          )}
                          
                          {technicalMetadata.trigger_type && (
                            <Grid item xs={6}>
                              <Card variant="outlined">
                                <CardContent>
                                  <Typography color="text.secondary" gutterBottom>
                                    Trigger Type
                                  </Typography>
                                  <Typography variant="body1">
                                    {technicalMetadata.trigger_type}
                                  </Typography>
                                </CardContent>
                              </Card>
                            </Grid>
                          )}
                          
                          {technicalMetadata.triggering_event && (
                            <Grid item xs={6}>
                              <Card variant="outlined">
                                <CardContent>
                                  <Typography color="text.secondary" gutterBottom>
                                    Triggering Event
                                  </Typography>
                                  <Typography variant="body1">
                                    {technicalMetadata.triggering_event}
                                  </Typography>
                                </CardContent>
                              </Card>
                            </Grid>
                          )}
                          
                          {technicalMetadata.table_name && assetType === 'trigger' && (
                            <Grid item xs={6}>
                              <Card variant="outlined">
                                <CardContent>
                                  <Typography color="text.secondary" gutterBottom>
                                    Target Table
                                  </Typography>
                                  <Typography variant="body1">
                                    {technicalMetadata.table_name}
                                  </Typography>
                                </CardContent>
                              </Card>
                            </Grid>
                          )}
                          
                          {selectedAsset?.discovered_at && (
                            <Grid item xs={6}>
                              <Card variant="outlined">
                                <CardContent>
                                  <Typography color="text.secondary" gutterBottom>
                                    Discovered At
                                  </Typography>
                                  <Typography variant="body1">
                                    {new Date(selectedAsset.discovered_at).toLocaleString()}
                                  </Typography>
                                </CardContent>
                              </Card>
                            </Grid>
                          )}
                        </Grid>
                      );
                    }
                    
                    // Azure Blob Storage metadata (existing code)
                    const safeLocation = technicalMetadata.location || 'N/A';
                    // Application name removed from technical_metadata - now comes from connection config
                    const safeSizeBytes = technicalMetadata.size_bytes || technicalMetadata.size || 0;
                    
                    let safeFormat = technicalMetadata.format;
                    if (!safeFormat || safeFormat === 'unknown') {
                        const fileExt = technicalMetadata.file_extension;
                        if (fileExt && fileExt !== 'N/A' && fileExt !== '') {
                            safeFormat = fileExt.replace('.', '').toUpperCase();
                        } else {
                            const contentType = technicalMetadata.content_type || '';
                            if (contentType.includes('/')) {
                                safeFormat = contentType.split('/')[1].toUpperCase();
                            } else {
                                safeFormat = contentType || 'UNKNOWN';
                            }
                        }
                    }
                    safeFormat = safeFormat.toUpperCase();
                    
                    const safeNumRows = technicalMetadata.num_rows || 0;
                    const safeCreatedAt = technicalMetadata.created_at || selectedAsset?.discovered_at || new Date().toISOString();
                    const safeLastModified = technicalMetadata.last_modified || safeCreatedAt;
                    const safeFileExtension = technicalMetadata.file_extension || 'N/A';
                    
                    const blobType = technicalMetadata.blob_type || 'Block blob';
                    const accessTier = technicalMetadata.access_tier || 'N/A';
                    const etag = technicalMetadata.etag || 'N/A';
                    const contentType = technicalMetadata.content_type || 'N/A';
                    
                    let storageType = 'Data File'; 
                    
                    const storageLocation = selectedAsset?.storage_location || technicalMetadata.storage_location || {};
                    const storageLocationType = storageLocation.type;
                    
                    if (storageLocationType) {
                        const typeMap = {
                            'azure_blob': 'Azure Blob Storage',
                            'azure_file_share': 'Azure File Share',
                            'azure_queue': 'Azure Queue',
                            'azure_table': 'Azure Table',
                            'blob_container': 'Blob Container'
                        };
                        storageType = typeMap[storageLocationType] || storageLocationType.replace(/_/g, ' ').replace(/\b\w/g, l => l.toUpperCase());
                    } else if (connectorId) {
                        if (connectorId.startsWith('azure_blob')) {
                            storageType = 'Azure Blob Storage';
                        } else if (connectorId.startsWith('azure_file_share')) {
                            storageType = 'Azure File Share';
                        } else if (connectorId.startsWith('azure_queue')) {
                            storageType = 'Azure Queue';
                        } else if (connectorId.startsWith('azure_table')) {
                            storageType = 'Azure Table';
                        }
                    }
                    
                    return (
                      <Grid container spacing={2}>
                        {metadataVisibility.technical['Asset ID'] && (
                          <Grid item xs={6}>
                            <Card variant="outlined">
                              <CardContent>
                                <Typography color="text.secondary" gutterBottom>
                                  Asset ID
                                </Typography>
                                <Typography variant="body1" sx={{ wordBreak: 'break-all', fontSize: '0.875rem' }}>
                                  {safeAssetId}
                                </Typography>
                              </CardContent>
                            </Card>
                          </Grid>
                        )}
                        
                        {metadataVisibility.technical['Last Modified'] && (
                          <Grid item xs={6}>
                            <Card variant="outlined">
                              <CardContent>
                                <Typography color="text.secondary" gutterBottom>
                                  Last Modified
                                </Typography>
                                <Typography variant="body1">
                                  {new Date(safeLastModified).toLocaleString()}
                                </Typography>
                              </CardContent>
                            </Card>
                          </Grid>
                        )}
                        {metadataVisibility.technical['Creation Time'] && (
                          <Grid item xs={6}>
                            <Card variant="outlined">
                              <CardContent>
                                <Typography color="text.secondary" gutterBottom>
                                  Creation Time
                                </Typography>
                                <Typography variant="body1">
                                  {new Date(safeCreatedAt).toLocaleString()}
                                </Typography>
                              </CardContent>
                            </Card>
                          </Grid>
                        )}
                        {metadataVisibility.technical['Type'] && (
                          <Grid item xs={6}>
                            <Card variant="outlined">
                              <CardContent>
                                <Typography color="text.secondary" gutterBottom>
                                  Type
                                </Typography>
                                <Typography variant="body1">
                                  {storageType}
                                </Typography>
                              </CardContent>
                            </Card>
                          </Grid>
                        )}
                        {metadataVisibility.technical['Size'] && (
                          <Grid item xs={6}>
                            <Card variant="outlined">
                              <CardContent>
                                <Typography color="text.secondary" gutterBottom>
                                  Size
                                </Typography>
                                <Typography variant="body1">
                                  {formatBytes(safeSizeBytes)}
                                </Typography>
                              </CardContent>
                            </Card>
                          </Grid>
                        )}
                        {metadataVisibility.technical['Format'] && (
                          <Grid item xs={6}>
                            <Card variant="outlined">
                              <CardContent>
                                <Typography color="text.secondary" gutterBottom>
                                  Format
                                </Typography>
                                <Typography variant="body1">
                                  {safeFormat}
                                </Typography>
                              </CardContent>
                            </Card>
                          </Grid>
                        )}
                        {metadataVisibility.technical['Access Tier'] && (
                          <Grid item xs={6}>
                            <Card variant="outlined">
                              <CardContent>
                                <Typography color="text.secondary" gutterBottom>
                                  Access Tier
                                </Typography>
                                <Typography variant="body1">
                                  {accessTier}
                                </Typography>
                              </CardContent>
                            </Card>
                          </Grid>
                        )}
                        {metadataVisibility.technical['ETAG'] && (
                          <Grid item xs={6}>
                            <Card variant="outlined">
                              <CardContent>
                                <Typography color="text.secondary" gutterBottom>
                                  ETAG
                                </Typography>
                                <Typography variant="body1" sx={{ fontSize: '0.875rem', wordBreak: 'break-all' }}>
                                  {etag}
                                </Typography>
                              </CardContent>
                            </Card>
                          </Grid>
                        )}
                        {metadataVisibility.technical['Content Type'] && (
                          <Grid item xs={6}>
                            <Card variant="outlined">
                              <CardContent>
                                <Typography color="text.secondary" gutterBottom>
                                  Content Type
                                </Typography>
                                <Typography variant="body1" sx={{ fontSize: '0.875rem' }}>
                                  {contentType}
                                </Typography>
                              </CardContent>
                            </Card>
                          </Grid>
                        )}
                        
                        {metadataVisibility.technical['Location'] && (
                          <Grid item xs={6}>
                            <Card variant="outlined">
                              <CardContent>
                                <Typography color="text.secondary" gutterBottom>
                                  Location
                                </Typography>
                                <Typography variant="body1" sx={{ fontSize: '0.875rem' }}>
                                  {safeLocation}
                                </Typography>
                              </CardContent>
                            </Card>
                          </Grid>
                        )}
                        {/* Application Name removed from technical metadata - now shown in main table from connection config */}
                        {safeNumRows > 0 && metadataVisibility.technical['Number of Rows'] && (
                          <Grid item xs={6}>
                            <Card variant="outlined">
                              <CardContent>
                                <Typography color="text.secondary" gutterBottom>
                                  Number of Rows
                                </Typography>
                                <Typography variant="body1">
                                  {formatNumber(safeNumRows)}
                                </Typography>
                              </CardContent>
                            </Card>
                          </Grid>
                        )}
                        {safeFileExtension !== 'N/A' && metadataVisibility.technical['File Extension'] && (
                          <Grid item xs={6}>
                            <Card variant="outlined">
                              <CardContent>
                                <Typography color="text.secondary" gutterBottom>
                                  File Extension
                                </Typography>
                                <Typography variant="body1">
                                  {safeFileExtension}
                                </Typography>
                              </CardContent>
                            </Card>
                          </Grid>
                        )}
                        
                      </Grid>
                    );
                  })()}
                </Box>
              )}

              {}
              {activeTab === 1 && (
                <Box>
                  <Typography variant="h6" sx={{ mb: 2, fontWeight: 600 }}>
                    Operational Metadata
                  </Typography>
                  {(() => {
                    const technicalMetadata = selectedAsset?.technical_metadata || {};
                    const connectorId = selectedAsset?.connector_id || '';
                    const isOracleDB = technicalMetadata.database_type === 'oracle' || connectorId.startsWith('oracle_db');
                    
                    const operationalMetadata = selectedAsset?.operational_metadata || {};
                    
                    if (isOracleDB) {
                      // Oracle DB specific operational metadata
                      const safeSchemaOwner = operationalMetadata.schema_owner || selectedAsset?.catalog || 'N/A';
                      const safeObjectStatus = operationalMetadata.object_status || 'VALID';
                      const safeLastAnalyzed = operationalMetadata.last_analyzed || technicalMetadata.last_analyzed || null;
                      const safeLastRefresh = operationalMetadata.last_refresh_date || technicalMetadata.last_refresh_date || null;
                      const safeConnectorId = selectedAsset?.connector_id || 'N/A';
                      const safeDiscoveryId = selectedAsset?.discovery_id || operationalMetadata.discovery_id || 'N/A';
                      const safeApplicationName = selectedAsset?.application_name || selectedAsset?.business_metadata?.application_name || operationalMetadata.application_name || 'N/A';
                      
                    return (
                      <Grid container spacing={2}>
                        {metadataVisibility.operational['Object Status'] && (
                          <Grid item xs={6}>
                            <Card variant="outlined">
                              <CardContent>
                                <Typography color="text.secondary" gutterBottom>
                                  Object Status
                                </Typography>
                                <Chip 
                                  label={safeObjectStatus} 
                                  color={safeObjectStatus === 'VALID' ? 'success' : 'error'} 
                                  size="small"
                                />
                              </CardContent>
                            </Card>
                          </Grid>
                        )}
                        {metadataVisibility.operational['Schema Owner'] && (
                          <Grid item xs={6}>
                            <Card variant="outlined">
                              <CardContent>
                                <Typography color="text.secondary" gutterBottom>
                                  Schema Owner
                                </Typography>
                                <Typography variant="body1">
                                  {safeSchemaOwner}
                                </Typography>
                              </CardContent>
                            </Card>
                          </Grid>
                        )}
                        {safeLastAnalyzed && metadataVisibility.operational['Last Analyzed'] && (
                          <Grid item xs={6}>
                            <Card variant="outlined">
                              <CardContent>
                                <Typography color="text.secondary" gutterBottom>
                                  Last Analyzed
                                </Typography>
                                <Typography variant="body1">
                                  {new Date(safeLastAnalyzed).toLocaleString()}
                                </Typography>
                              </CardContent>
                            </Card>
                          </Grid>
                        )}
                        {safeLastRefresh && metadataVisibility.operational['Last Refresh Date'] && (
                          <Grid item xs={6}>
                            <Card variant="outlined">
                              <CardContent>
                                <Typography color="text.secondary" gutterBottom>
                                  Last Refresh Date
                                </Typography>
                                <Typography variant="body1">
                                  {new Date(safeLastRefresh).toLocaleString()}
                                </Typography>
                              </CardContent>
                            </Card>
                          </Grid>
                        )}
                        {metadataVisibility.operational['Connector ID'] && (
                          <Grid item xs={6}>
                            <Card variant="outlined">
                              <CardContent>
                                <Typography color="text.secondary" gutterBottom>
                                  Connector ID
                                </Typography>
                                <Typography variant="body1" sx={{ wordBreak: 'break-all', fontSize: '0.875rem' }}>
                                  {safeConnectorId}
                                </Typography>
                              </CardContent>
                            </Card>
                          </Grid>
                        )}
                        {selectedAsset?.discovered_at && metadataVisibility.operational['Discovered At'] && (
                          <Grid item xs={6}>
                            <Card variant="outlined">
                              <CardContent>
                                <Typography color="text.secondary" gutterBottom>
                                  Discovered At
                                </Typography>
                                <Typography variant="body1">
                                  {new Date(selectedAsset.discovered_at).toLocaleString()}
                                </Typography>
                              </CardContent>
                            </Card>
                          </Grid>
                        )}
                        {safeDiscoveryId !== 'N/A' && metadataVisibility.operational['Discovery ID'] && (
                          <Grid item xs={6}>
                            <Card variant="outlined">
                              <CardContent>
                                <Typography color="text.secondary" gutterBottom>
                                  Discovery ID
                                </Typography>
                                <Typography variant="body1" sx={{ wordBreak: 'break-all', fontSize: '0.875rem' }}>
                                  {safeDiscoveryId}
                                </Typography>
                              </CardContent>
                            </Card>
                          </Grid>
                        )}
                        {safeApplicationName !== 'N/A' && metadataVisibility.operational['Application Name'] && (
                          <Grid item xs={6}>
                            <Card variant="outlined">
                              <CardContent>
                                <Typography color="text.secondary" gutterBottom>
                                  Application Name
                                </Typography>
                                <Typography variant="body1">
                                  {safeApplicationName}
                                </Typography>
                              </CardContent>
                            </Card>
                          </Grid>
                        )}
                      </Grid>
                    );
                    }
                    
                    // Azure Blob Storage operational metadata (existing code)
                    const safeStatus = operationalMetadata.status || 'active';
                    const safeOwner = typeof operationalMetadata.owner === 'object' && operationalMetadata.owner?.roleName 
                      ? operationalMetadata.owner.roleName 
                      : operationalMetadata.owner || 'workspace_owner@hdfc.bank.in';
                    const safeLastModified = operationalMetadata.last_modified || operationalMetadata.last_updated_at || selectedAsset?.discovered_at || new Date().toISOString();
                    const safeLastAccessed = operationalMetadata.last_accessed || operationalMetadata.last_updated_at || new Date().toISOString();
                    const safeAccessCount = operationalMetadata.access_count || operationalMetadata.access_level || 'internal';
                    // Derive data source type from connector_id or use stored value
                    let safeDataSourceType = selectedAsset?.data_source_type || operationalMetadata.data_source_type;
                    if (!safeDataSourceType || safeDataSourceType === 'N/A') {
                      const connectorId = selectedAsset?.connector_id || operationalMetadata.connector_id || '';
                      if (connectorId.startsWith('azure_blob_')) {
                        safeDataSourceType = 'Azure Blob Storage';
                      } else if (connectorId.startsWith('adls_gen2_') || connectorId.includes('datalake')) {
                        safeDataSourceType = 'ADLS Gen2';
                      } else if (connectorId) {
                        // Extract connector type from connector_id format: "type_name"
                        const parts = connectorId.split('_');
                        if (parts.length > 0) {
                          const connectorType = parts[0];
                          safeDataSourceType = connectorType.charAt(0).toUpperCase() + connectorType.slice(1).replace('_', ' ');
                        } else {
                          safeDataSourceType = connectorId;
                        }
                      } else {
                        safeDataSourceType = 'N/A';
                      }
                    } else {
                      // Format the stored data_source_type for better display
                      if (safeDataSourceType.toLowerCase().includes('azure') && safeDataSourceType.toLowerCase().includes('blob')) {
                        safeDataSourceType = 'Azure Blob Storage';
                      } else if (safeDataSourceType.toLowerCase().includes('adls') || safeDataSourceType.toLowerCase().includes('datalake')) {
                        safeDataSourceType = 'ADLS Gen2';
                      }
                    }
                    const safeConnectorId = selectedAsset?.connector_id || operationalMetadata.connector_id || 'N/A';
                    const safeCatalog = selectedAsset?.catalog || operationalMetadata.catalog || 'N/A';
                    const safeDiscoveryId = selectedAsset?.discovery_id || operationalMetadata.discovery_id || 'N/A';
                    const safeApplicationName = selectedAsset?.business_metadata?.application_name || operationalMetadata.application_name || 'N/A';
                    
                    return (
                      <Grid container spacing={2}>
                        {metadataVisibility.operational['Status'] && (
                          <Grid item xs={6}>
                            <Card variant="outlined">
                              <CardContent>
                                <Typography color="text.secondary" gutterBottom>
                                  Status
                                </Typography>
                                <Chip 
                                  label={safeStatus} 
                                  color="success" 
                                  size="small"
                                />
                              </CardContent>
                            </Card>
                          </Grid>
                        )}
                        {metadataVisibility.operational['Owner'] && (
                          <Grid item xs={6}>
                            <Card variant="outlined">
                              <CardContent>
                                <Typography color="text.secondary" gutterBottom>
                                  Owner
                                </Typography>
                                <Typography variant="body1">
                                  {safeOwner}
                                </Typography>
                              </CardContent>
                            </Card>
                          </Grid>
                        )}
                        {metadataVisibility.operational['Last Modified'] && (
                          <Grid item xs={6}>
                            <Card variant="outlined">
                              <CardContent>
                                <Typography color="text.secondary" gutterBottom>
                                  Last Modified
                                </Typography>
                                <Typography variant="body1">
                                  {new Date(safeLastModified).toLocaleString()}
                                </Typography>
                              </CardContent>
                            </Card>
                          </Grid>
                        )}
                        {metadataVisibility.operational['Last Accessed'] && (
                          <Grid item xs={6}>
                            <Card variant="outlined">
                              <CardContent>
                                <Typography color="text.secondary" gutterBottom>
                                  Last Accessed
                                </Typography>
                                <Typography variant="body1">
                                  {new Date(safeLastAccessed).toLocaleString()}
                                </Typography>
                              </CardContent>
                            </Card>
                          </Grid>
                        )}
                        {metadataVisibility.operational['Access Count'] && (
                          <Grid item xs={6}>
                            <Card variant="outlined">
                              <CardContent>
                                <Typography color="text.secondary" gutterBottom>
                                  Access Count
                                </Typography>
                                <Typography variant="body1">
                                  {safeAccessCount}
                                </Typography>
                              </CardContent>
                            </Card>
                          </Grid>
                        )}
                        {metadataVisibility.operational['Data Source Type'] && (
                          <Grid item xs={6}>
                            <Card variant="outlined">
                              <CardContent>
                                <Typography color="text.secondary" gutterBottom>
                                  Data Source Type
                                </Typography>
                                <Typography variant="body1">
                                  {safeDataSourceType}
                                </Typography>
                              </CardContent>
                            </Card>
                          </Grid>
                        )}
                        {metadataVisibility.operational['Connector ID'] && (
                          <Grid item xs={6}>
                            <Card variant="outlined">
                              <CardContent>
                                <Typography color="text.secondary" gutterBottom>
                                  Connector ID
                                </Typography>
                                <Typography variant="body1">
                                  {safeConnectorId}
                                </Typography>
                              </CardContent>
                            </Card>
                          </Grid>
                        )}
                        {metadataVisibility.operational['Catalog'] && (
                          <Grid item xs={6}>
                            <Card variant="outlined">
                              <CardContent>
                                <Typography color="text.secondary" gutterBottom>
                                  Catalog
                                </Typography>
                                <Typography variant="body1">
                                  {safeCatalog}
                                </Typography>
                              </CardContent>
                            </Card>
                          </Grid>
                        )}
                        {selectedAsset?.discovered_at && metadataVisibility.operational['Discovered At'] && (
                          <Grid item xs={6}>
                            <Card variant="outlined">
                              <CardContent>
                                <Typography color="text.secondary" gutterBottom>
                                  Discovered At
                                </Typography>
                                <Typography variant="body1">
                                  {new Date(selectedAsset.discovered_at).toLocaleString()}
                                </Typography>
                              </CardContent>
                            </Card>
                          </Grid>
                        )}
                        {safeDiscoveryId !== 'N/A' && metadataVisibility.operational['Discovery ID'] && (
                          <Grid item xs={6}>
                            <Card variant="outlined">
                              <CardContent>
                                <Typography color="text.secondary" gutterBottom>
                                  Discovery ID
                                </Typography>
                                <Typography variant="body1" sx={{ wordBreak: 'break-all', fontSize: '0.875rem' }}>
                                  {safeDiscoveryId}
                                </Typography>
                              </CardContent>
                            </Card>
                          </Grid>
                        )}
                        {safeApplicationName !== 'N/A' && metadataVisibility.operational['Application Name'] && (
                          <Grid item xs={6}>
                            <Card variant="outlined">
                              <CardContent>
                                <Typography color="text.secondary" gutterBottom>
                                  Application Name
                                </Typography>
                                <Typography variant="body1">
                                  {safeApplicationName}
                                </Typography>
                              </CardContent>
                            </Card>
                          </Grid>
                        )}
                      </Grid>
                    );
                  })()}
                </Box>
              )}

              {}
              {activeTab === 2 && (
                <Box>
                  <Typography variant="h6" sx={{ mb: 2, fontWeight: 600 }}>
                    Business Metadata
                  </Typography>
                  {(() => {
                    
                    const businessMetadata = selectedAsset?.business_metadata || {};
                    const safeDescription = businessMetadata.description || selectedAsset?.description || 'No description available';
                    const safeBusinessOwner = businessMetadata.business_owner || 'workspace_owner@hdfc.bank.in';
                    const safeDepartment = businessMetadata.department || 'N/A';
                    const safeClassification = businessMetadata.classification || 'internal';
                    const safeSensitivityLevel = businessMetadata.sensitivity_level || 'medium';
                    // Filter tags: remove empty, whitespace-only, and "torrocon" tags
                    const allTags = businessMetadata.tags || [];
                    const filteredTags = allTags
                      .filter(tag => tag && typeof tag === 'string' && tag.trim() !== '' && tag.toLowerCase() !== 'torrocon')
                      .map(tag => {
                        // Shorten rejection tags if needed
                        if (tag.startsWith('REJECTED:')) {
                          const reasonPart = tag.substring('REJECTED:'.length).trim();
                          const shortReason = GOVERNANCE_REJECTION_REASONS.find(r => reasonPart.startsWith(`${r.code} -`))?.shortTag ||
                                             reasonPart.split(' ').slice(0, 2).join(' ') ||
                                             'Rejected';
                          return `REJECTED: ${shortReason}`;
                        }
                        return tag;
                      });
                    
                    return (
                      <Grid container spacing={2}>
                        {metadataVisibility.business['Description'] && (
                          <Grid item xs={12}>
                            <Card variant="outlined">
                              <CardContent>
                                <Typography color="text.secondary" gutterBottom sx={{ mb: 1 }}>
                                  Description
                                </Typography>
                                <TextField
                                  fullWidth
                                  multiline
                                  rows={3}
                                  value={description || defaultDescription}
                                  InputProps={{
                                    readOnly: true,
                                  }}
                                  placeholder="Azure Blob Storage file: schema_20_file_099.parquet"
                                  variant="outlined"
                                  size="small"
                                  sx={{
                                    '& .MuiInputBase-input': {
                                      cursor: 'default',
                                    }
                                  }}
                                />
                              </CardContent>
                            </Card>
                          </Grid>
                        )}
                        {metadataVisibility.business['Business Owner'] && (
                          <Grid item xs={6}>
                            <Card variant="outlined">
                              <CardContent>
                                <Typography color="text.secondary" gutterBottom>
                                  Business Owner
                                </Typography>
                                <Typography variant="body1">
                                  {safeBusinessOwner}
                                </Typography>
                              </CardContent>
                            </Card>
                          </Grid>
                        )}
                        {metadataVisibility.business['Department'] && (
                          <Grid item xs={6}>
                            <Card variant="outlined">
                              <CardContent>
                                <Typography color="text.secondary" gutterBottom>
                                  Department
                                </Typography>
                                <FormControl fullWidth size="small" sx={{ mt: 1 }}>
                                  <Select
                                    value={department}
                                    onChange={(e) => setDepartment(e.target.value)}
                                    displayEmpty
                                  >
                                    {DEPARTMENTS.map((dept) => (
                                      <MenuItem key={dept} value={dept}>
                                        {dept}
                                      </MenuItem>
                                    ))}
                                  </Select>
                                </FormControl>
                              </CardContent>
                            </Card>
                          </Grid>
                        )}
                        {metadataVisibility.business['Classification'] && (
                          <Grid item xs={6}>
                            <Card variant="outlined">
                              <CardContent>
                                <Typography color="text.secondary" gutterBottom>
                                  Classification
                                </Typography>
                                <FormControl fullWidth size="small" sx={{ mt: 1 }}>
                                  <Select
                                    value={classification}
                                    onChange={(e) => setClassification(e.target.value)}
                                    displayEmpty
                                  >
                                    <MenuItem value="public">Public</MenuItem>
                                    <MenuItem value="internal">Internal</MenuItem>
                                    <MenuItem value="confidential">Confidential</MenuItem>
                                    <MenuItem value="restricted">Restricted</MenuItem>
                                    <MenuItem value="top_secret">Top Secret</MenuItem>
                                  </Select>
                                </FormControl>
                              </CardContent>
                            </Card>
                          </Grid>
                        )}
                        {metadataVisibility.business['Sensitivity Level'] && (
                          <Grid item xs={6}>
                            <Card variant="outlined">
                              <CardContent>
                                <Typography color="text.secondary" gutterBottom>
                                  Sensitivity Level
                                </Typography>
                                <FormControl fullWidth size="small" sx={{ mt: 1 }}>
                                  <Select
                                    value={sensitivityLevel}
                                    onChange={(e) => setSensitivityLevel(e.target.value)}
                                    displayEmpty
                                  >
                                    <MenuItem value="low">Low</MenuItem>
                                    <MenuItem value="medium">Medium</MenuItem>
                                    <MenuItem value="high">High</MenuItem>
                                    <MenuItem value="critical">Critical</MenuItem>
                                  </Select>
                                </FormControl>
                              </CardContent>
                            </Card>
                          </Grid>
                        )}
                        {metadataVisibility.business['Tags'] && (
                          <Grid item xs={12}>
                            <Card variant="outlined">
                              <CardContent>
                                <Typography color="text.secondary" gutterBottom sx={{ fontWeight: 600 }}>
                                  Table Tags
                                </Typography>
                                <Box sx={{ display: 'flex', gap: 1, mt: 1, flexWrap: 'wrap' }}>
                                  {filteredTags.length > 0 ? (
                                    filteredTags.map((tag, index) => (
                                      <Chip
                                        key={index}
                                        label={tag}
                                        size="small"
                                        color={tag.startsWith('REJECTED:') ? 'error' : 'default'}
                                        variant={tag.startsWith('REJECTED:') ? 'filled' : 'outlined'}
                                      />
                                    ))
                                  ) : (
                                    <Typography variant="body2" color="text.secondary" sx={{ fontStyle: 'italic' }}>
                                      No table tags
                                    </Typography>
                                  )}
                                </Box>
                              </CardContent>
                            </Card>
                          </Grid>
                        )}
                        <Grid item xs={12}>
                          <Card variant="outlined">
                            <CardContent>
                              <Typography color="text.secondary" gutterBottom sx={{ fontWeight: 600 }}>
                                Column Tags
                              </Typography>
                              <Box sx={{ mt: 1 }}>
                                <Typography variant="body2" color="text.secondary" sx={{ fontStyle: 'italic' }}>
                                  No column tags
                                </Typography>
                              </Box>
                            </CardContent>
                          </Card>
                        </Grid>
                      </Grid>
                    );
                  })()}
                </Box>
              )}

              {}
              {activeTab === 3 && (
                <Box>
                  <Box display="flex" justifyContent="space-between" alignItems="center" sx={{ mb: 2 }}>
                    <Typography variant="h6" sx={{ fontWeight: 600 }}>
                      Columns & PII Detection
                    </Typography>
                    <Box display="flex" gap={1} alignItems="center">
                      {showColumnCheckboxes && (
                        <Tooltip title={`Export ${selectedColumns.length} selected column(s) to CSV`}>
                          <span>
                            <Button
                              variant="contained"
                              color="primary"
                              onClick={handleExportSelected}
                              startIcon={<FileDownload />}
                              disabled={selectedColumns.length === 0}
                            >
                              Export Selected ({selectedColumns.length})
                            </Button>
                          </span>
                        </Tooltip>
                      )}
                      <Tooltip title="Add a custom column to track additional information for each column">
                        <span>
                          <Button
                            variant="outlined"
                            color="primary"
                            startIcon={<Add />}
                            onClick={() => setAddColumnDialogOpen(true)}
                          >
                            Add Column
                          </Button>
                        </span>
                      </Tooltip>
                      <Tooltip title="Export column data in various formats">
                        <span>
                          <Button
                            variant="outlined"
                            startIcon={<FileDownload />}
                            endIcon={<ArrowDropDown />}
                            onClick={(e) => setExportAnchorEl(e.currentTarget)}
                          >
                            EXPORT
                          </Button>
                        </span>
                      </Tooltip>
                    </Box>
                    <Menu
                      anchorEl={exportAnchorEl}
                      open={Boolean(exportAnchorEl)}
                      onClose={() => setExportAnchorEl(null)}
                    >
                      <Tooltip title="Download an empty CSV template with column headers" placement="right">
                        <MenuItem onClick={() => {
                          handleExportTemplate();
                          setExportAnchorEl(null);
                        }}>
                          <ListItemText 
                            primary="Download Template"
                            secondary="Empty template with headers"
                          />
                        </MenuItem>
                      </Tooltip>
                      <Tooltip title="Export only columns marked as containing PII data" placement="right">
                        <MenuItem onClick={() => {
                          handleExportPII();
                          setExportAnchorEl(null);
                        }}>
                          <ListItemText primary="Export only PII" />
                        </MenuItem>
                      </Tooltip>
                      <Tooltip title="Export all columns for the selected asset" placement="right">
                        <MenuItem onClick={() => {
                          handleExportAll();
                          setExportAnchorEl(null);
                        }}>
                          <ListItemText primary="Export all columns" />
                        </MenuItem>
                      </Tooltip>
                      <Tooltip title="Select specific columns to export using checkboxes" placement="right">
                        <MenuItem onClick={() => {
                          setShowColumnCheckboxes(true);
                          setExportAnchorEl(null);
                        }}>
                          <ListItemText primary="Export only selected columns" />
                        </MenuItem>
                      </Tooltip>
                    </Menu>
                  </Box>
                  {(() => {
                    
                    const columns = selectedAsset?.columns || [];
                    const piiColumns = columns.filter(col => col.pii_detected);
                    // Check if any column has PII or is being changed from Non-PII to PII
                    const hasPiiOrChanging = columns.some(col => {
                      const isPii = col.pii_detected || false;
                      // Check if this column is being changed from Non-PII to PII in the dialog
                      const isChangingToPii = piiDialogOpen && 
                                             selectedColumnForPii?.name === col.name && 
                                             piiDialogIsPii && 
                                             (originalPiiStatus[col.name] === false || !col.pii_detected);
                      return isPii || isChangingToPii;
                    });
                    
                    if (columns.length > 0) {
                      return (
                        <>
                          {piiColumns.length > 0 && (
                            <Alert severity="warning" sx={{ mb: 2 }}>
                              {piiColumns.length} column(s) contain PII data
                            </Alert>
                          )}
                          <TableContainer component={Paper} variant="outlined">
                            <Table>
                              <TableHead>
                                <TableRow>
                                  {showColumnCheckboxes && (
                                    <TableCell padding="checkbox">
                                      <Checkbox
                                        indeterminate={selectedColumns.length > 0 && selectedColumns.length < columns.length}
                                        checked={columns.length > 0 && selectedColumns.length === columns.length}
                                        onChange={(e) => {
                                          if (e.target.checked) {
                                            setSelectedColumns(columns.map(col => col.name));
                                          } else {
                                            setSelectedColumns([]);
                                          }
                                        }}
                                      />
                                    </TableCell>
                                  )}
                                  <TableCell>Column Name</TableCell>
                                  <TableCell>Data Type</TableCell>
                                  <TableCell>Nullable</TableCell>
                                  <TableCell>Description</TableCell>
                                  <TableCell>PII Status</TableCell>
                                  {/* Custom columns headers */}
                                  {Object.entries(customColumns).map(([columnId, customCol]) => (
                                    <TableCell key={columnId}>
                                      <Box sx={{ display: 'flex', alignItems: 'center', gap: 1 }}>
                                        {customCol.label}
                                        <Tooltip title={`Delete custom column "${customCol.label}"`}>
                                          <IconButton
                                            size="small"
                                            onClick={async () => {
                                              if (confirm(`Delete custom column "${customCol.label}"?`)) {
                                              const updatedCustomColumns = { ...customColumns };
                                              delete updatedCustomColumns[columnId];
                                              setCustomColumns(updatedCustomColumns);
                                              
                                              // Save to backend
                                              if (selectedAsset) {
                                                try {
                                                  const API_BASE_URL = import.meta.env.VITE_API_BASE_URL;
                                                  await fetch(`${API_BASE_URL}/api/assets/${selectedAsset.id}`, {
                                                    method: 'PUT',
                                                    headers: { 'Content-Type': 'application/json' },
                                                    body: JSON.stringify({ custom_columns: updatedCustomColumns })
                                                  });
                                                  // Refresh asset
                                                  const assetResponse = await fetch(`${API_BASE_URL}/api/assets/${selectedAsset.id}`);
                                                  if (assetResponse.ok) {
                                                    const updatedAsset = await assetResponse.json();
                                                    setSelectedAsset(updatedAsset);
                                                  }
                                                } catch (err) {
                                                  console.error('Failed to delete custom column:', err);
                                                  alert('Failed to delete custom column');
                                                }
                                              }
                                            }
                                          }}
                                          sx={{ ml: 0.5 }}
                                        >
                                          <Close fontSize="small" />
                                        </IconButton>
                                        </Tooltip>
                                      </Box>
                                    </TableCell>
                                  ))}
                                  {hasPiiOrChanging && (
                                    <>
                                      <TableCell>Masking logic (Analytical User)</TableCell>
                                      <TableCell>Masking logic (Operational User)</TableCell>
                                    </>
                                  )}
                                </TableRow>
                              </TableHead>
                              <TableBody>
                                {columns.map((column, index) => {
                                  const isEditing = editingColumn === column.name;
                                  const editData = columnEditData[column.name] || {};
                                  
                                  const handleStartEdit = () => {
                                    setEditingColumn(column.name);
                                    setColumnEditData({
                                      ...columnEditData,
                                      [column.name]: {}
                                    });
                                  };
                                  
                                  const handleCancelEdit = () => {
                                    setEditingColumn(null);
                                    const newEditData = { ...columnEditData };
                                    delete newEditData[column.name];
                                    setColumnEditData(newEditData);
                                  };
                                  
                                  const handleSaveColumn = async () => {
                                    if (!selectedAsset) return;
                                    setSavingColumn(true);
                                    try {
                                      const API_BASE_URL = import.meta.env.VITE_API_BASE_URL;
                                      const updatedColumns = selectedAsset.columns.map(col => 
                                        col.name === column.name 
                                          ? {
                                              ...col
                                            }
                                          : col
                                      );
                                      
                                      const response = await fetch(`${API_BASE_URL}/api/assets/${selectedAsset.id}`, {
                                        method: 'PUT',
                                        headers: { 'Content-Type': 'application/json' },
                                        body: JSON.stringify({ columns: updatedColumns })
                                      });
                                      
                                      if (response.ok) {
                                        const updatedAsset = await response.json();
                                        setSelectedAsset(updatedAsset);
                                        setEditingColumn(null);
                                        const newEditData = { ...columnEditData };
                                        delete newEditData[column.name];
                                        setColumnEditData(newEditData);
                                        
                                        // Refresh the assets list and check if page becomes empty
                                        try {
                                          const filteredCount = await fetchAssets(null, true);
                                          
                                          // If current page is empty after filtering and we're not on page 0, go to previous page
                                          if (filteredCount === 0 && currentPage > 0) {
                                            const newPage = Math.max(0, currentPage - 1);
                                            setCurrentPage(newPage);
                                            await fetchAssets(newPage);
                                          }
                                        } catch (fetchError) {
                                          console.error('Error refreshing assets after column save:', fetchError);
                                          // Non-critical error - just log it
                                        }
                                      } else {
                                        const errorData = await response.json().catch(() => ({ error: 'Unknown error' }));
                                        throw new Error(errorData.error || 'Failed to save column changes');
                                      }
                                    } catch (error) {
                                      console.error('Error saving column:', error);
                                      const errorMessage = error instanceof Error ? error.message : String(error || 'Unknown error');
                                      alert(`Failed to save column changes: ${errorMessage}`);
                                    } finally {
                                      setSavingColumn(false);
                                    }
                                  };
                                  
                                  const handleFieldChange = (field, value) => {
                                    setColumnEditData({
                                      ...columnEditData,
                                      [column.name]: {
                                        ...editData,
                                        [field]: value
                                      }
                                    });
                                  };
                                  
                                  return (
                                    <TableRow key={index}>
                                      {showColumnCheckboxes && (
                                        <TableCell padding="checkbox">
                                          <Checkbox
                                            checked={selectedColumns.includes(column.name)}
                                            onChange={(e) => {
                                              if (e.target.checked) {
                                                setSelectedColumns([...selectedColumns, column.name]);
                                              } else {
                                                setSelectedColumns(selectedColumns.filter(name => name !== column.name));
                                              }
                                            }}
                                          />
                                        </TableCell>
                                      )}
                                      <TableCell>
                                        <Typography variant="body2" sx={{ fontWeight: 500 }}>
                                          {column.name || 'Unknown'}
                                        </Typography>
                                      </TableCell>
                                      <TableCell>
                                        <Box sx={{ display: 'flex', alignItems: 'center', gap: 0.5 }}>
                                        <Chip label={column.type || 'Unknown'} size="small" variant="outlined" />
                                          {(column.precision || column.scale || column.length) && (
                                            <Typography variant="caption" color="text.secondary" sx={{ ml: 0.5 }}>
                                              {column.precision && column.scale 
                                                ? `(${column.precision},${column.scale})`
                                                : column.length 
                                                  ? `(${column.length})`
                                                  : ''}
                                            </Typography>
                                          )}
                                        </Box>
                                      </TableCell>
                                      <TableCell>
                                        {column.nullable !== undefined ? (column.nullable ? 'Yes' : 'No') : 'N/A'}
                                      </TableCell>
                                      <TableCell>
                                        <Tooltip title={column.description || 'No description'} arrow>
                                          <Typography 
                                            variant="body2" 
                                            color="text.secondary"
                                            sx={{ 
                                              maxWidth: '200px',
                                              overflow: 'hidden',
                                              textOverflow: 'ellipsis',
                                              whiteSpace: 'nowrap'
                                            }}
                                          >
                                            {column.description || 'No description'}
                                          </Typography>
                                        </Tooltip>
                                      </TableCell>
                                      <TableCell>
                                        {column.pii_detected ? (
                                          <Tooltip title={`Click to edit PII status and masking logic. Detected types: ${(column.pii_types && column.pii_types.length > 0) ? column.pii_types.join(', ') : 'Unknown'}`}>
                                            <Chip 
                                              icon={<Warning />}
                                              label={`PII: ${(column.pii_types && column.pii_types.length > 0) ? column.pii_types.join(', ') : 'Unknown'}`} 
                                              color="error" 
                                              size="small"
                                              sx={{ cursor: 'pointer', '&:hover': { opacity: 0.8 } }}
                                              onClick={() => handleOpenPiiDialog(column)}
                                            />
                                          </Tooltip>
                                        ) : (
                                          <Tooltip title="Click to mark this column as containing PII data">
                                            <Chip 
                                              icon={<CheckCircle />}
                                              label="No PII" 
                                              color="success" 
                                              size="small"
                                              sx={{ cursor: 'pointer', '&:hover': { opacity: 0.8 } }}
                                              onClick={() => handleOpenPiiDialog(column)}
                                            />
                                          </Tooltip>
                                        )}
                                      </TableCell>
                                      {/* Custom columns cells */}
                                      {Object.entries(customColumns).map(([columnId, customCol]) => {
                                        const isEditing = editingCustomValue?.columnId === columnId && editingCustomValue?.columnName === column.name;
                                        const currentValue = customCol.values?.[column.name] || '';
                                        
                                        return (
                                          <TableCell key={columnId}>
                                            {isEditing ? (
                                              <Box sx={{ display: 'flex', gap: 0.5, alignItems: 'center' }}>
                                                <TextField
                                                  size="small"
                                                  value={customValueInput}
                                                  onChange={(e) => setCustomValueInput(e.target.value)}
                                                  onBlur={async () => {
                                                    // Save value
                                                    const updatedCustomColumns = {
                                                      ...customColumns,
                                                      [columnId]: {
                                                        ...customCol,
                                                        values: {
                                                          ...(customCol.values || {}),
                                                          [column.name]: customValueInput
                                                        }
                                                      }
                                                    };
                                                    setCustomColumns(updatedCustomColumns);
                                                    setEditingCustomValue(null);
                                                    
                                                    // Save to backend
                                                    if (selectedAsset) {
                                                      try {
                                                        const API_BASE_URL = import.meta.env.VITE_API_BASE_URL;
                                                        await fetch(`${API_BASE_URL}/api/assets/${selectedAsset.id}`, {
                                                          method: 'PUT',
                                                          headers: { 'Content-Type': 'application/json' },
                                                          body: JSON.stringify({ custom_columns: updatedCustomColumns })
                                                        });
                                                        // Refresh asset
                                                        const assetResponse = await fetch(`${API_BASE_URL}/api/assets/${selectedAsset.id}`);
                                                        if (assetResponse.ok) {
                                                          const updatedAsset = await assetResponse.json();
                                                          setSelectedAsset(updatedAsset);
                                                        }
                                                      } catch (err) {
                                                        console.error('Failed to save custom column value:', err);
                                                        alert('Failed to save value');
                                                      }
                                                    }
                                                  }}
                                                  onKeyDown={(e) => {
                                                    if (e.key === 'Enter') {
                                                      e.target.blur();
                                                    } else if (e.key === 'Escape') {
                                                      setEditingCustomValue(null);
                                                      setCustomValueInput('');
                                                    }
                                                  }}
                                                  autoFocus
                                                  sx={{ minWidth: 150 }}
                                                />
                                              </Box>
                                            ) : (
                                              <Typography
                                                variant="body2"
                                                onClick={() => {
                                                  setEditingCustomValue({ columnId, columnName: column.name });
                                                  setCustomValueInput(currentValue);
                                                }}
                                                sx={{
                                                  cursor: 'pointer',
                                                  color: currentValue ? 'text.primary' : 'text.secondary',
                                                  '&:hover': { textDecoration: 'underline' },
                                                  minHeight: '20px'
                                                }}
                                              >
                                                {currentValue || 'Click to add'}
                                              </Typography>
                                            )}
                                          </TableCell>
                                        );
                                      })}
                                      {hasPiiOrChanging && (() => {
                                        if (!selectedAsset) return null;
                                        
                                        const isPii = column.pii_detected || false;
                                        // Use asset-scoped key for all state lookups
                                        const key = `${selectedAsset.id}_${column.name}`;
                                        // Check if this column is being changed from Non-PII to PII in the dialog
                                        const isChangingToPii = piiDialogOpen && 
                                                               selectedColumnForPii?.name === column.name && 
                                                               piiDialogIsPii && 
                                                               (originalPiiStatus[key] === false || !column.pii_detected);
                                        const showMasking = isPii || isChangingToPii;
                                        const maskingLogic = columnMaskingLogic[key] || {
                                          analytical: column.masking_logic_analytical || '',
                                          operational: column.masking_logic_operational || ''
                                        };
                                        
                                        if (!showMasking) {
                                          return (
                                            <>
                                              <TableCell></TableCell>
                                              <TableCell></TableCell>
                                            </>
                                          );
                                        }
                                        
                                        // Get PII types - use dialog types if column is being changed to PII
                                        const currentPiiTypes = (isChangingToPii && piiDialogOpen && selectedColumnForPii?.name === column.name) 
                                          ? piiDialogTypes 
                                          : (column.pii_types || []);
                                        
                                        // Get masking options based on PII types
                                        const analyticalOptions = getMaskingOptions({ pii_types: currentPiiTypes }, 'analytical');
                                        const operationalOptions = getMaskingOptions({ pii_types: currentPiiTypes }, 'operational');
                                        
                                        // Check if there are unsaved changes (asset-scoped)
                                        const hasUnsavedChanges = unsavedMaskingChanges[key] || false;
                                        const isSaving = savingMaskingLogic[key] || false;
                                        
                                        return (
                                          <>
                                            <TableCell>
                                              <Box sx={{ display: 'flex', gap: 1, alignItems: 'center' }}>
                                                <FormControl size="small" fullWidth>
                                                  <Select
                                                    value={maskingLogic.analytical || ''}
                                                    onChange={(e) => {
                                                      if (!selectedAsset) return;
                                                      const key = `${selectedAsset.id}_${column.name}`;
                                                      setColumnMaskingLogic(prev => ({
                                                        ...prev,
                                                        [key]: {
                                                          ...maskingLogic,
                                                          analytical: e.target.value
                                                        }
                                                      }));
                                                      // Mark as having unsaved changes (asset-scoped)
                                                      setUnsavedMaskingChanges(prev => ({
                                                        ...prev,
                                                        [key]: true
                                                      }));
                                                    }}
                                                    displayEmpty
                                                    sx={{ minWidth: 200 }}
                                                  >
                                                    <MenuItem value="">
                                                      <em>Select masking logic</em>
                                                    </MenuItem>
                                                    {analyticalOptions.map((option) => (
                                                      <MenuItem key={option.value} value={option.value}>
                                                        {option.label}
                                                      </MenuItem>
                                                    ))}
                                                  </Select>
                                                </FormControl>
                                                {hasUnsavedChanges && (
                                                  <IconButton
                                                    size="small"
                                                    color="primary"
                                                    onClick={() => handleSaveMaskingLogic(column.name)}
                                                    disabled={isSaving}
                                                    sx={{ flexShrink: 0 }}
                                                  >
                                                    {isSaving ? <CircularProgress size={20} /> : <Save fontSize="small" />}
                                                  </IconButton>
                                                )}
                                              </Box>
                                            </TableCell>
                                            <TableCell>
                                              <Box sx={{ display: 'flex', gap: 1, alignItems: 'center' }}>
                                                <FormControl size="small" fullWidth>
                                                  <Select
                                                    value={maskingLogic.operational || ''}
                                                    onChange={(e) => {
                                                      if (!selectedAsset) return;
                                                      const key = `${selectedAsset.id}_${column.name}`;
                                                      setColumnMaskingLogic(prev => ({
                                                        ...prev,
                                                        [key]: {
                                                          ...maskingLogic,
                                                          operational: e.target.value
                                                        }
                                                      }));
                                                      // Mark as having unsaved changes (asset-scoped)
                                                      setUnsavedMaskingChanges(prev => ({
                                                        ...prev,
                                                        [key]: true
                                                      }));
                                                    }}
                                                    displayEmpty
                                                    sx={{ minWidth: 200 }}
                                                  >
                                                    <MenuItem value="">
                                                      <em>Select masking logic</em>
                                                    </MenuItem>
                                                    {operationalOptions.map((option) => (
                                                      <MenuItem key={option.value} value={option.value}>
                                                        {option.label}
                                                      </MenuItem>
                                                    ))}
                                                  </Select>
                                                </FormControl>
                                                {hasUnsavedChanges && (
                                                  <IconButton
                                                    size="small"
                                                    color="primary"
                                                    onClick={() => handleSaveMaskingLogic(column.name)}
                                                    disabled={isSaving}
                                                    sx={{ flexShrink: 0 }}
                                                  >
                                                    {isSaving ? <CircularProgress size={20} /> : <Save fontSize="small" />}
                                                  </IconButton>
                                                )}
                                              </Box>
                                            </TableCell>
                                          </>
                                        );
                                      })()}
                                    </TableRow>
                                  );
                                })}
                              </TableBody>
                            </Table>
                          </TableContainer>
                        </>
                      );
                    } else {
                      return (
                        <Alert severity="info">
                          No column information available for this asset type.
                        </Alert>
                      );
                    }
                  })()}
                </Box>
              )}
            </DialogContent>
            <DialogActions>
              {}
              {(activeTab === 2 || classification !== originalClassification || sensitivityLevel !== originalSensitivityLevel || department !== originalDepartment || description !== originalDescription) && (
                <>
                  <Button 
                    onClick={handleCloseDialog} 
                    variant="outlined"
                    disabled={savingMetadata}
                  >
                    Cancel
                  </Button>
                  <Button
                    variant="contained"
                    color="primary"
                    onClick={handleSaveMetadata}
                    disabled={savingMetadata}
                    startIcon={savingMetadata ? <CircularProgress size={20} /> : null}
                  >
                    {savingMetadata ? 'Saving...' : 'Save Changes'}
                  </Button>
                </>
              )}
              <Button onClick={handleCloseDialog} variant="outlined">
                Close
              </Button>
            </DialogActions>
          </>
        )}
      </Dialog>

      {}
      <Dialog open={rejectDialogOpen} onClose={() => {
        setRejectDialogOpen(false);
        setSelectedRejectReason('');
        setCustomRejectReason('');
        setRejectReason('');
        setAssetToReject(null);
      }} maxWidth="sm" fullWidth>
        <DialogTitle>Reject Asset</DialogTitle>
        <DialogContent>
          <Typography variant="body2" sx={{ mb: 2 }}>
            Please select a governance reason for rejecting this asset:
          </Typography>
          <FormControl fullWidth sx={{ mb: 2 }}>
            <InputLabel>Rejection Reason</InputLabel>
            <Select
              value={selectedRejectReason}
              onChange={(e) => {
                setSelectedRejectReason(e.target.value);
                if (e.target.value !== '011') {
                  setCustomRejectReason('');
                }
              }}
              label="Rejection Reason"
            >
              {GOVERNANCE_REJECTION_REASONS.map((reason) => (
                <MenuItem key={reason.code} value={reason.code}>
                  {reason.code} - {reason.reason}
                </MenuItem>
              ))}
            </Select>
          </FormControl>
          
          {selectedRejectReason === '011' && (
            <TextField
              autoFocus
              margin="dense"
              label="Custom Rejection Reason"
              fullWidth
              multiline
              rows={4}
              variant="outlined"
              value={customRejectReason}
              onChange={(e) => setCustomRejectReason(e.target.value)}
              placeholder="Please provide a detailed reason for rejection..."
              sx={{ mt: 1 }}
            />
          )}
        </DialogContent>
        <DialogActions>
          <Button onClick={() => {
            setRejectDialogOpen(false);
            setSelectedRejectReason('');
            setCustomRejectReason('');
            setRejectReason('');
            setAssetToReject(null);
          }}>
            Cancel
          </Button>
          <Button 
            onClick={handleRejectConfirm} 
            variant="contained" 
            color="error"
            disabled={!selectedRejectReason || (selectedRejectReason === '011' && !customRejectReason.trim())}
          >
            Reject
          </Button>
        </DialogActions>
      </Dialog>

      {}
      <Dialog open={piiDialogOpen} onClose={handleClosePiiDialog} maxWidth="sm" fullWidth>
        <DialogTitle>
          Change PII Status for Column: {selectedColumnForPii?.name}
        </DialogTitle>
        <DialogContent>
          <Box sx={{ mt: 2, display: 'flex', flexDirection: 'column', gap: 3 }}>
            <FormControl component="fieldset">
              <FormLabel component="legend" sx={{ mb: 1, fontWeight: 600 }}>PII Status</FormLabel>
              <RadioGroup
                value={piiDialogIsPii ? 'yes' : 'no'}
                onChange={(e) => {
                  const isPii = e.target.value === 'yes';
                  setPiiDialogIsPii(isPii);
                  if (!isPii) {
                    setPiiDialogTypes([]);
                  }
                }}
              >
                <FormControlLabel 
                  value="yes" 
                  control={<Radio />} 
                  label="Mark as PII" 
                />
                <FormControlLabel 
                  value="no" 
                  control={<Radio />} 
                  label="Mark as Non-PII" 
                />
              </RadioGroup>
            </FormControl>

            {piiDialogIsPii && (
              <FormControl component="fieldset">
                <FormLabel component="legend" sx={{ mb: 1, fontWeight: 600 }}>
                  PII Types (Select all that apply)
                </FormLabel>
                <FormGroup>
                  <Box sx={{ display: 'flex', flexDirection: 'column', gap: 1, maxHeight: '300px', overflowY: 'auto', p: 1, border: '1px solid #e0e0e0', borderRadius: 1 }}>
                    {PII_TYPES.map((type) => (
                      <FormControlLabel
                        key={type}
                        control={
                          <Checkbox
                            checked={piiDialogTypes.includes(type)}
                            onChange={(e) => {
                              if (e.target.checked) {
                                setPiiDialogTypes([...piiDialogTypes, type]);
                              } else {
                                setPiiDialogTypes(piiDialogTypes.filter(t => t !== type));
                              }
                            }}
                          />
                        }
                        label={type}
                      />
                    ))}
                    {/* Custom PII Types */}
                    {piiDialogTypes.filter(t => !PII_TYPES.includes(t)).map((type) => (
                      <FormControlLabel
                        key={type}
                        control={
                          <Checkbox
                            checked={true}
                            onChange={(e) => {
                              if (!e.target.checked) {
                                setPiiDialogTypes(piiDialogTypes.filter(t => t !== type));
                              }
                            }}
                          />
                        }
                        label={
                          <Box sx={{ display: 'flex', alignItems: 'center', gap: 1 }}>
                            <Typography variant="body2">{type}</Typography>
                            <Chip label="Custom" size="small" color="primary" variant="outlined" sx={{ height: 18, fontSize: '0.65rem' }} />
                          </Box>
                        }
                      />
                    ))}
                  </Box>
                  
                  {/* Add Custom PII Type */}
                  <Box sx={{ mt: 2, display: 'flex', gap: 1, alignItems: 'center' }}>
                    <TextField
                      size="small"
                      placeholder="Enter custom PII type..."
                      value={customPiiType}
                      onChange={(e) => setCustomPiiType(e.target.value)}
                      onKeyPress={(e) => {
                        if (e.key === 'Enter') {
                          e.preventDefault();
                          handleAddCustomPiiType();
                        }
                      }}
                      sx={{ flex: 1 }}
                    />
                    <Button
                      variant="outlined"
                      size="small"
                      startIcon={<Add />}
                      onClick={handleAddCustomPiiType}
                      disabled={!customPiiType.trim() || piiDialogTypes.includes(customPiiType.trim()) || PII_TYPES.includes(customPiiType.trim())}
                    >
                      Add
                    </Button>
                  </Box>
                  
                  {piiDialogTypes.length === 0 && (
                    <Typography variant="body2" color="text.secondary" sx={{ mt: 1, fontStyle: 'italic' }}>
                      Please select at least one PII type or add a custom type
                    </Typography>
                  )}
                </FormGroup>
              </FormControl>
            )}
          </Box>
        </DialogContent>
        <DialogActions>
          <Button onClick={handleClosePiiDialog} disabled={savingPii}>
            Cancel
          </Button>
          <Button 
            onClick={handleSavePii} 
            variant="contained" 
            color="primary"
            disabled={savingPii || (piiDialogIsPii && piiDialogTypes.length === 0)}
            startIcon={savingPii ? <CircularProgress size={20} /> : null}
          >
            {savingPii ? 'Saving...' : 'Save'}
          </Button>
        </DialogActions>
      </Dialog>

      {/* Add Custom Column Dialog */}
      <Dialog open={addColumnDialogOpen} onClose={() => {
        setAddColumnDialogOpen(false);
        setNewColumnLabel('');
      }} maxWidth="sm" fullWidth>
        <DialogTitle>Add Custom Column</DialogTitle>
        <DialogContent>
          <Box sx={{ mt: 2 }}>
            <TextField
              fullWidth
              label="Column Label"
              value={newColumnLabel}
              onChange={(e) => setNewColumnLabel(e.target.value)}
              placeholder="Enter column label (e.g., Notes, Tags, etc.)"
              variant="outlined"
              autoFocus
              onKeyPress={(e) => {
                if (e.key === 'Enter' && newColumnLabel.trim()) {
                  e.preventDefault();
                  handleAddCustomColumn();
                }
              }}
            />
          </Box>
        </DialogContent>
        <DialogActions>
          <Button onClick={() => {
            setAddColumnDialogOpen(false);
            setNewColumnLabel('');
          }}>
            Cancel
          </Button>
          <Button 
            onClick={handleAddCustomColumn}
            variant="contained" 
            color="primary"
            disabled={!newColumnLabel.trim()}
          >
            Add Column
          </Button>
        </DialogActions>
      </Dialog>

      {}
      <Dialog open={discoveryDetailsOpen} onClose={() => setDiscoveryDetailsOpen(false)} maxWidth="md" fullWidth>
        <DialogTitle>Discovery Details</DialogTitle>
        <DialogContent>
          {discoveryDetails && (
            <Box sx={{ mt: 2 }}>
              <Grid container spacing={2}>
                <Grid item xs={12} sm={6}>
                  <Typography variant="subtitle2" color="text.secondary">Discovery ID</Typography>
                  <Typography variant="body1" sx={{ fontWeight: 600 }}>#{discoveryDetails.discovery_id}</Typography>
                </Grid>
                <Grid item xs={12} sm={6}>
                  <Typography variant="subtitle2" color="text.secondary">Status</Typography>
                  <Chip 
                    label={discoveryDetails.status || 'N/A'} 
                    size="small" 
                    color={discoveryDetails.status === 'approved' ? 'success' : discoveryDetails.status === 'rejected' ? 'error' : 'default'}
                  />
                </Grid>
                <Grid item xs={12} sm={6}>
                  <Typography variant="subtitle2" color="text.secondary">Approval Status</Typography>
                  <Chip 
                    label={discoveryDetails.approval_status || 'N/A'} 
                    size="small" 
                    color={discoveryDetails.approval_status === 'approved' ? 'success' : discoveryDetails.approval_status === 'rejected' ? 'error' : 'default'}
                  />
                </Grid>
                <Grid item xs={12} sm={6}>
                  <Typography variant="subtitle2" color="text.secondary">Asset ID</Typography>
                  <Typography variant="body2">{discoveryDetails.asset_id || 'N/A'}</Typography>
                </Grid>
                {discoveryDetails.asset && (
                  <>
                    <Grid item xs={12} sm={6}>
                      <Typography variant="subtitle2" color="text.secondary">Asset Name</Typography>
                      <Typography variant="body2" sx={{ fontWeight: 500 }}>{discoveryDetails.asset.name}</Typography>
                    </Grid>
                    <Grid item xs={12} sm={6}>
                      <Typography variant="subtitle2" color="text.secondary">Asset Type</Typography>
                      <Typography variant="body2">{discoveryDetails.asset.type}</Typography>
                    </Grid>
                  </>
                )}
                <Grid item xs={12} sm={6}>
                  <Typography variant="subtitle2" color="text.secondary">Discovered At</Typography>
                  <Typography variant="body2">
                    {discoveryDetails.discovered_at ? new Date(discoveryDetails.discovered_at).toLocaleString() : 'N/A'}
                  </Typography>
                </Grid>
                {discoveryDetails.published_at && (
                  <Grid item xs={12} sm={6}>
                    <Typography variant="subtitle2" color="text.secondary">Published At</Typography>
                    <Typography variant="body2">
                      {new Date(discoveryDetails.published_at).toLocaleString()}
                    </Typography>
                  </Grid>
                )}
                {discoveryDetails.approval_workflow && (
                  <Grid item xs={12}>
                    <Typography variant="subtitle2" color="text.secondary" sx={{ mb: 1 }}>Approval Workflow</Typography>
                    <Paper variant="outlined" sx={{ p: 2 }}>
                      <Typography variant="body2" component="pre" sx={{ whiteSpace: 'pre-wrap', fontFamily: 'monospace', fontSize: '0.875rem' }}>
                        {JSON.stringify(discoveryDetails.approval_workflow, null, 2)}
                      </Typography>
                    </Paper>
                  </Grid>
                )}
              </Grid>
            </Box>
          )}
        </DialogContent>
        <DialogActions>
          <Button onClick={() => setDiscoveryDetailsOpen(false)} variant="outlined">
            Close
          </Button>
        </DialogActions>
      </Dialog>

      {/* Metadata Visibility Settings Dialog */}
      <Dialog 
        open={metadataSettingsOpen} 
        onClose={() => setMetadataSettingsOpen(false)}
        maxWidth="md"
        fullWidth
      >
        <DialogTitle>
          <Box display="flex" justifyContent="space-between" alignItems="center">
            <Typography variant="h6">Metadata Visibility Settings</Typography>
            <Box>
              <Button
                size="small"
                onClick={resetMetadataVisibility}
                sx={{ mr: 1 }}
              >
                Reset to Default
              </Button>
              <IconButton onClick={() => setMetadataSettingsOpen(false)} size="small">
                <Close />
              </IconButton>
            </Box>
          </Box>
        </DialogTitle>
        <DialogContent>
          <Tabs 
            value={metadataSettingsTab} 
            onChange={(e, newValue) => setMetadataSettingsTab(newValue)} 
            sx={{ mb: 2, borderBottom: 1, borderColor: 'divider' }}
          >
            <Tab label="Technical Metadata" />
            <Tab label="Operational Metadata" />
            <Tab label="Business Metadata" />
          </Tabs>
          
          {/* Data Source Type Selector - only show for Technical and Operational tabs */}
          {(metadataSettingsTab === 0 || metadataSettingsTab === 1) && (
            <Box sx={{ mb: 2 }}>
              <FormControl fullWidth size="small">
                <InputLabel>Data Source Type</InputLabel>
                <Select
                  value={metadataDataSourceType}
                  label="Data Source Type"
                  onChange={(e) => setMetadataDataSourceType(e.target.value)}
                >
                  <MenuItem value="azure">Azure Blob Storage</MenuItem>
                  <MenuItem value="oracle">Oracle Database</MenuItem>
                </Select>
              </FormControl>
            </Box>
          )}
          
          <Box sx={{ mt: 2 }}>
            {metadataSettingsTab === 0 && (
              <Box>
                <Typography variant="subtitle2" sx={{ mb: 2, fontWeight: 600 }}>
                  Toggle visibility of Technical Metadata fields ({metadataDataSourceType === 'azure' ? 'Azure Blob Storage' : 'Oracle Database'})
                </Typography>
                <FormGroup>
                  {technicalFieldsBySource[metadataDataSourceType]
                    .filter(field => metadataVisibility.technical.hasOwnProperty(field))
                    .map((field) => (
                    <FormControlLabel
                      key={field}
                      control={
                        <Switch
                          checked={metadataVisibility.technical[field]}
                          onChange={(e) => {
                            const updated = {
                              ...metadataVisibility,
                              technical: {
                                ...metadataVisibility.technical,
                                [field]: e.target.checked,
                              },
                            };
                            setMetadataVisibility(updated);
                            saveMetadataVisibility(updated);
                          }}
                        />
                      }
                      label={field}
                    />
                  ))}
                </FormGroup>
              </Box>
            )}
            
            {metadataSettingsTab === 1 && (
              <Box>
                <Typography variant="subtitle2" sx={{ mb: 2, fontWeight: 600 }}>
                  Toggle visibility of Operational Metadata fields ({metadataDataSourceType === 'azure' ? 'Azure Blob Storage' : 'Oracle Database'})
                </Typography>
                <FormGroup>
                  {operationalFieldsBySource[metadataDataSourceType]
                    .filter(field => metadataVisibility.operational.hasOwnProperty(field))
                    .map((field) => (
                    <FormControlLabel
                      key={field}
                      control={
                        <Switch
                          checked={metadataVisibility.operational[field]}
                          onChange={(e) => {
                            const updated = {
                              ...metadataVisibility,
                              operational: {
                                ...metadataVisibility.operational,
                                [field]: e.target.checked,
                              },
                            };
                            setMetadataVisibility(updated);
                            saveMetadataVisibility(updated);
                          }}
                        />
                      }
                      label={field}
                    />
                  ))}
                </FormGroup>
              </Box>
            )}
            
            {metadataSettingsTab === 2 && (
              <Box>
                <Typography variant="subtitle2" sx={{ mb: 2, fontWeight: 600 }}>
                  Toggle visibility of Business Metadata fields
                </Typography>
                <FormGroup>
                  {Object.keys(metadataVisibility.business).map((field) => (
                    <FormControlLabel
                      key={field}
                      control={
                        <Switch
                          checked={metadataVisibility.business[field]}
                          onChange={(e) => {
                            const updated = {
                              ...metadataVisibility,
                              business: {
                                ...metadataVisibility.business,
                                [field]: e.target.checked,
                              },
                            };
                            setMetadataVisibility(updated);
                            saveMetadataVisibility(updated);
                          }}
                        />
                      }
                      label={field}
                    />
                  ))}
                </FormGroup>
              </Box>
            )}
          </Box>
        </DialogContent>
        <DialogActions>
          <Button onClick={() => setMetadataSettingsOpen(false)}>Close</Button>
        </DialogActions>
      </Dialog>

      {/* Full-screen loader for publishing */}
      {publishing && (
        <Box
          sx={{
            position: 'fixed',
            top: 0,
            left: 0,
            right: 0,
            bottom: 0,
            backgroundColor: 'rgba(0, 0, 0, 0.5)',
            display: 'flex',
            flexDirection: 'column',
            justifyContent: 'center',
            alignItems: 'center',
            zIndex: 9999,
          }}
        >
          <CircularProgress size={60} sx={{ color: '#fff', mb: 2 }} />
          <Typography variant="h6" sx={{ color: '#fff', fontWeight: 500 }}>
            Publishing asset...
          </Typography>
          <Typography variant="body2" sx={{ color: '#fff', mt: 1, opacity: 0.8 }}>
            Please wait while we redirect you
          </Typography>
        </Box>
      )}
    </Box>
  );
};

export default AssetsPage;
