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
  
  // Business Glossary Tag state
  const [description, setDescription] = useState('');
  const [originalDescription, setOriginalDescription] = useState('');
  const [defaultDescription, setDefaultDescription] = useState('');
  const [selectedTags, setSelectedTags] = useState([]);
  const [tagDialogOpen, setTagDialogOpen] = useState(false);
  const [availableTags, setAvailableTags] = useState([]);
  const [loadingTags, setLoadingTags] = useState(false);
  const [selectedTag, setSelectedTag] = useState('');
  
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

  // Fetch metadata tags when tag dialog opens (always fetch fresh from DB)
  useEffect(() => {
    if (tagDialogOpen) {
      fetchMetadataTags();
    }
  }, [tagDialogOpen]);

  // Initialize masking logic from column data when asset is selected
  useEffect(() => {
    if (selectedAsset?.columns) {
      const initialMaskingLogic = {};
      selectedAsset.columns.forEach(col => {
        if (col.pii_detected) {
          initialMaskingLogic[col.name] = {
            analytical: col.masking_logic_analytical || '',
            operational: col.masking_logic_operational || ''
          };
        }
      });
      setColumnMaskingLogic(prev => {
        // Merge with existing to preserve unsaved changes
        return { ...initialMaskingLogic, ...prev };
      });
    }
  }, [selectedAsset]);

  const fetchMetadataTags = async () => {
    setLoadingTags(true);
    try {
      const API_BASE_URL = import.meta.env.VITE_API_BASE_URL;
      const response = await fetch(`${API_BASE_URL}/api/metadata-tags`);
      if (response.ok) {
        const data = await response.json();
        console.log('Fetched metadata tags:', data);
        setAvailableTags(data.tags || []);
      } else {
        const errorData = await response.json().catch(() => ({ error: 'Unknown error' }));
        console.error('Failed to fetch metadata tags:', response.status, errorData);
        alert(`Failed to fetch tags: ${errorData.error || 'Unknown error'}`);
      }
    } catch (error) {
      console.error('Error fetching metadata tags:', error);
      alert(`Error fetching tags: ${error.message}`);
    } finally {
      setLoadingTags(false);
    }
  };

  const handleAddTagToDescription = () => {
    if (selectedTag) {
      const tag = availableTags.find(t => t.id.toString() === selectedTag);
      if (tag && !selectedTags.find(t => t.id === tag.id)) {
        // Add tag to array if not already present
        setSelectedTags(prev => [...prev, tag]);
        setSelectedTag('');
        setTagDialogOpen(false);
      }
    }
  };

  const handleRemoveTag = (tagId) => {
    setSelectedTags(prev => prev.filter(t => t.id !== tagId));
  };

  useEffect(() => {
    fetchAssets();
  }, [currentPage, pageSize, searchTerm, typeFilter, catalogFilter, approvalStatusFilter, applicationNameFilter]);

  const fetchAssets = async (pageOverride = null, returnFiltered = false) => {
      setLoading(true);
    try {
      const API_BASE_URL = import.meta.env.VITE_API_BASE_URL;
      
      const page = pageOverride !== null ? pageOverride : currentPage;
      const pageParam = page + 1; // Backend uses 1-based pagination
      
      const url = `${API_BASE_URL}/api/assets?page=${pageParam}&per_page=${pageSize}`;
      
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
          // New paginated format
          assetsList = data.assets;
          total = data.pagination.total;
          totalPagesCount = data.pagination.total_pages;
        } else {
          assetsList = [];
          total = 0;
          totalPagesCount = 0;
        }
        
        setAllAssets(assetsList);
        setTotalAssets(total);
        setTotalPages(totalPagesCount);
        
        // Client-side filtering (if needed for search/filters)
        let filtered = assetsList;
        if (searchTerm) {
          filtered = filtered.filter(asset => 
            asset.name.toLowerCase().includes(searchTerm.toLowerCase()) ||
            (asset.catalog && asset.catalog.toLowerCase().includes(searchTerm.toLowerCase()))
          );
        }
        if (typeFilter.length > 0) {
          filtered = filtered.filter(asset => typeFilter.includes(asset.type));
        }
        if (catalogFilter.length > 0) {
          filtered = filtered.filter(asset => catalogFilter.includes(asset.catalog));
        }
        if (approvalStatusFilter.length > 0) {
          filtered = filtered.filter(asset => {
            const status = asset.operational_metadata?.approval_status || 'pending_review';
            return approvalStatusFilter.includes(status);
          });
        }
        if (applicationNameFilter.length > 0) {
          filtered = filtered.filter(asset => {
            const appName = asset.business_metadata?.application_name || '';
            return applicationNameFilter.includes(appName);
          });
        }
        
        setAssets(filtered);
        
        // Return filtered count if requested (for checking empty pages)
        if (returnFiltered) {
          return filtered.length;
        }
        } else {
        setAssets([]);
        setTotalAssets(0);
        setTotalPages(0);
        if (returnFiltered) {
          return 0;
        }
      }
    } catch (error) {
      console.error('Error fetching assets:', error);
      setAssets([]);
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
  const uniqueApplicationNames = allAssets ? [...new Set(allAssets.map(asset => asset.business_metadata?.application_name).filter(Boolean))] : [];

  const getDataSource = (connectorId) => {
    if (!connectorId) return 'Unknown';
    
    
    if (connectorId.startsWith('azure_blob_')) {
      return 'Azure Blob Storage';
    }
    
    
    if (connectorId.startsWith('azure_')) {
      return 'Azure Storage';
    }
    
    
    return connectorId;
  };

  const getDataSourceColor = (connectorId) => {
    if (!connectorId) return 'default';
    
    if (connectorId.startsWith('azure_blob_') || connectorId.startsWith('azure_')) {
      return 'primary';
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

  const handleViewAsset = async (assetId) => {
    // OPTIMIZED: First check if asset is already in state (instant - no API call)
    const cachedAsset = allAssets.find(a => a.id === assetId);
    
    if (cachedAsset) {
      // Asset already loaded - use it directly (instant!)
      setSelectedAsset(cachedAsset);
      setDetailsDialogOpen(true);
        setOriginalClassification(cachedAsset.business_metadata?.classification || 'internal');
        setOriginalSensitivityLevel(cachedAsset.business_metadata?.sensitivity_level || 'medium');
        setOriginalDepartment(cachedAsset.business_metadata?.department || 'Data Engineering');
        setClassification(cachedAsset.business_metadata?.classification || 'internal');
        setSensitivityLevel(cachedAsset.business_metadata?.sensitivity_level || 'medium');
        setDepartment(cachedAsset.business_metadata?.department || 'Data Engineering');
        
        // Set default description
        const defaultDesc = `Azure Blob Storage file: ${cachedAsset.name}`;
        setDefaultDescription(defaultDesc);
        
        // Parse description to extract tags if stored as JSON array
        const desc = cachedAsset.business_metadata?.description || '';
        try {
          const parsedTags = JSON.parse(desc);
          if (Array.isArray(parsedTags) && parsedTags.length > 0) {
            setSelectedTags(parsedTags);
            setDescription('');
          } else {
            setSelectedTags([]);
            setDescription(desc || defaultDesc);
          }
        } catch {
          // Not JSON, check if it's the default format or has tags
          if (desc && desc !== defaultDesc && desc.includes('|')) {
            // Old format with pipe-separated tags - clear it and use default
            setSelectedTags([]);
            setDescription(defaultDesc);
          } else {
            setSelectedTags([]);
            setDescription(desc || defaultDesc);
          }
        }
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
        setOriginalClassification(asset.business_metadata?.classification || 'internal');
        setOriginalSensitivityLevel(asset.business_metadata?.sensitivity_level || 'medium');
        setOriginalDepartment(asset.business_metadata?.department || 'Data Engineering');
        setClassification(asset.business_metadata?.classification || 'internal');
        setSensitivityLevel(asset.business_metadata?.sensitivity_level || 'medium');
        setDepartment(asset.business_metadata?.department || 'Data Engineering');
        
        // Set default description
        const defaultDesc = `Azure Blob Storage file: ${asset.name}`;
        setDefaultDescription(defaultDesc);
        
        // Parse description to extract tags if stored as JSON array
        const desc = asset.business_metadata?.description || '';
        try {
          const parsedTags = JSON.parse(desc);
          if (Array.isArray(parsedTags) && parsedTags.length > 0) {
            setSelectedTags(parsedTags);
            setDescription('');
          } else {
            setSelectedTags([]);
            setDescription(desc || defaultDesc);
          }
        } catch {
          // Not JSON, check if it's the default format or has tags
          if (desc && desc !== defaultDesc && desc.includes('|')) {
            // Old format with pipe-separated tags - clear it and use default
            setSelectedTags([]);
            setDescription(defaultDesc);
          } else {
            setSelectedTags([]);
            setDescription(desc || defaultDesc);
          }
        }
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
    setSelectedTags([]);
    setTagDialogOpen(false);
    setSelectedTag('');
  };

  // PII Dialog handlers
  const handleOpenPiiDialog = (column) => {
    setSelectedColumnForPii(column);
    setPiiDialogIsPii(column.pii_detected || false);
    setPiiDialogTypes(column.pii_types || []);
    setCustomPiiType('');
    // Track original PII status to detect changes
    setOriginalPiiStatus(prev => ({
      ...prev,
      [column.name]: column.pii_detected || false
    }));
    // Initialize masking logic from column data if available
    if (!columnMaskingLogic[column.name]) {
      setColumnMaskingLogic(prev => ({
        ...prev,
        [column.name]: {
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
      const maskingLogic = columnMaskingLogic[selectedColumnForPii.name] || { analytical: '', operational: '' };
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
        // Clear unsaved masking changes for this column
        setUnsavedMaskingChanges(prev => {
          const newState = { ...prev };
          delete newState[selectedColumnForPii.name];
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

  // Handler to save masking logic changes directly from table
  const handleSaveMaskingLogic = async (columnName) => {
    if (!selectedAsset) return;
    
    const column = selectedAsset.columns.find(c => c.name === columnName);
    if (!column || !column.pii_detected) return;
    
    const maskingLogic = columnMaskingLogic[columnName];
    if (!maskingLogic) return;
    
    setSavingMaskingLogic(prev => ({ ...prev, [columnName]: true }));
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
        // Clear unsaved changes flag
        setUnsavedMaskingChanges(prev => {
          const newState = { ...prev };
          delete newState[columnName];
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
        delete newState[columnName];
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
      
      // Save tags as JSON array if tags exist, otherwise save default description
      const descToSave = selectedTags.length > 0 
        ? JSON.stringify(selectedTags)
        : (description || defaultDescription);
      
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

  return (
    <Box sx={{ p: 3 }}>
      <Box sx={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center', mb: 3 }}>
        <Typography variant="h4" component="h1" sx={{ fontWeight: 600, fontFamily: 'Comfortaa' }}>
          Discovered Assets
        </Typography>
        <Box sx={{ display: 'flex', gap: 2, alignItems: 'center' }}>
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
                        containers: [], 
                        folder_path: '',
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
                
                // Wait a moment for discovery to process and save to database
                // This ensures new assets are available when we fetch
                await new Promise(resolve => setTimeout(resolve, 2000));
                
                // Force refresh - fetch fresh data with cache busting
                // Store current page before resetting (for potential navigation back)
                const previousPage = currentPage;
                // Reset to first page for refresh
                setCurrentPage(0);
                
                // Force a fresh fetch by adding timestamp to URL
                const timestamp = new Date().getTime();
                const url = `${API_BASE_URL}/api/assets?page=1&per_page=${pageSize}&_t=${timestamp}`;
                
                const refreshResponse = await fetch(url);
                if (refreshResponse.ok) {
                  const refreshData = await refreshResponse.json();
                  if (refreshData.assets && refreshData.pagination) {
                    setAllAssets(refreshData.assets);
                    setTotalAssets(refreshData.pagination.total);
                    setTotalPages(refreshData.pagination.total_pages);
                    
                    // Apply filters (using array-based multi-select filters)
                    let filtered = refreshData.assets;
                    if (searchTerm) {
                      filtered = filtered.filter(asset => 
                        asset.name.toLowerCase().includes(searchTerm.toLowerCase()) ||
                        (asset.catalog && asset.catalog.toLowerCase().includes(searchTerm.toLowerCase()))
                      );
                    }
                    if (typeFilter.length > 0) {
                      filtered = filtered.filter(asset => typeFilter.includes(asset.type));
                    }
                    if (catalogFilter.length > 0) {
                      filtered = filtered.filter(asset => catalogFilter.includes(asset.catalog));
                    }
                    if (approvalStatusFilter.length > 0) {
                      filtered = filtered.filter(asset => {
                        const status = asset.operational_metadata?.approval_status || 'pending_review';
                        return approvalStatusFilter.includes(status);
                      });
                    }
                    if (applicationNameFilter.length > 0) {
                      filtered = filtered.filter(asset => {
                        const appName = asset.business_metadata?.application_name || '';
                        return applicationNameFilter.includes(appName);
                      });
                    }
                    
                    // If filters result in empty page and we were on a different page, try to stay on that page
                    if (filtered.length === 0 && previousPage > 0) {
                      // Try fetching the previous page to see if it has filtered results
                      const previousPageUrl = `${API_BASE_URL}/api/assets?page=${previousPage + 1}&per_page=${pageSize}&_t=${timestamp}`;
                      const previousPageResponse = await fetch(previousPageUrl);
                      if (previousPageResponse.ok) {
                        const previousPageData = await previousPageResponse.json();
                        if (previousPageData.assets && previousPageData.pagination) {
                          setAllAssets(previousPageData.assets);
                          setCurrentPage(previousPage);
                          // Re-apply filters
                          let previousFiltered = previousPageData.assets;
                          if (searchTerm) {
                            previousFiltered = previousFiltered.filter(asset => 
                              asset.name.toLowerCase().includes(searchTerm.toLowerCase()) ||
                              (asset.catalog && asset.catalog.toLowerCase().includes(searchTerm.toLowerCase()))
                            );
                          }
                          if (typeFilter.length > 0) {
                            previousFiltered = previousFiltered.filter(asset => typeFilter.includes(asset.type));
                          }
                          if (catalogFilter.length > 0) {
                            previousFiltered = previousFiltered.filter(asset => catalogFilter.includes(asset.catalog));
                          }
                          if (approvalStatusFilter.length > 0) {
                            previousFiltered = previousFiltered.filter(asset => {
                              const status = asset.operational_metadata?.approval_status || 'pending_review';
                              return approvalStatusFilter.includes(status);
                            });
                          }
                          if (applicationNameFilter.length > 0) {
                            previousFiltered = previousFiltered.filter(asset => {
                              const appName = asset.business_metadata?.application_name || '';
                              return applicationNameFilter.includes(appName);
                            });
                          }
                          setAssets(previousFiltered);
                        } else {
                          // If previous page also has no results, show empty (already on page 0)
                          setAssets([]);
                        }
                      } else {
                        // If fetch fails, show empty (already on page 0)
                        setAssets([]);
                      }
                    } else {
                      // Normal case: set filtered assets (page 0 or has results)
                      setAssets(filtered);
                    }
                  }
                }
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
        </Box>
      </Box>

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
              <Button
                fullWidth
                variant="outlined"
                onClick={(e) => setTypeMenuAnchor(e.currentTarget)}
                sx={{ justifyContent: 'space-between', textTransform: 'none' }}
              >
                {typeFilter.length === 0 ? 'All Types' : `${typeFilter.length} Selected`}
                <FilterList fontSize="small" />
              </Button>
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
              <Button
                fullWidth
                variant="outlined"
                onClick={(e) => setCatalogMenuAnchor(e.currentTarget)}
                sx={{ justifyContent: 'space-between', textTransform: 'none' }}
              >
                {catalogFilter.length === 0 ? 'All Catalogs' : `${catalogFilter.length} Selected`}
                <FilterList fontSize="small" />
              </Button>
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
              <Button
                fullWidth
                variant="outlined"
                onClick={(e) => setStatusMenuAnchor(e.currentTarget)}
                sx={{ justifyContent: 'space-between', textTransform: 'none' }}
              >
                {approvalStatusFilter.length === 0 ? 'All Statuses' : `${approvalStatusFilter.length} Selected`}
                <FilterList fontSize="small" />
              </Button>
              <Menu
                anchorEl={statusMenuAnchor}
                open={Boolean(statusMenuAnchor)}
                onClose={() => setStatusMenuAnchor(null)}
                PaperProps={{
                  style: {
                    maxHeight: 300,
                    width: 200,
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
              <Button
                fullWidth
                variant="outlined"
                onClick={(e) => setApplicationMenuAnchor(e.currentTarget)}
                sx={{ justifyContent: 'space-between', textTransform: 'none' }}
              >
                {applicationNameFilter.length === 0 ? 'All Applications' : `${applicationNameFilter.length} Selected`}
                <FilterList fontSize="small" />
              </Button>
              <Menu
                anchorEl={applicationMenuAnchor}
                open={Boolean(applicationMenuAnchor)}
                onClose={() => setApplicationMenuAnchor(null)}
                PaperProps={{
                  style: {
                    maxHeight: 300,
                    width: 200,
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
                  <TableCell>Data Source</TableCell>
                  <TableCell>Discovered</TableCell>
                  <TableCell>Actions</TableCell>
                </TableRow>
              </TableHead>
              <TableBody>
                {assets.map((asset, index) => (
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
                        <Button
                          size="small"
                          startIcon={<Visibility />}
                          variant="outlined"
                          onClick={() => handleViewAsset(asset.id)}
                        >
                          View
                        </Button>
                        {(asset.operational_metadata?.approval_status === 'pending_review' || 
                          !asset.operational_metadata?.approval_status ||
                          asset.operational_metadata?.approval_status === 'pending') && (
                          <>
                            <Button
                              size="small"
                              startIcon={<ThumbUp />}
                              variant="contained"
                              color="success"
                              onClick={() => handleApproveAsset(asset.id)}
                            >
                              Approve
                            </Button>
                            <Button
                              size="small"
                              startIcon={<ThumbDown />}
                              variant="contained"
                              color="error"
                              onClick={() => handleRejectClick(asset.id)}
                            >
                              Reject
                            </Button>
                          </>
                        )}
                        {asset.operational_metadata?.approval_status === 'approved' && (
                          <>
                          <Chip
                            icon={<CheckCircle />}
                            label="Approved"
                            color="success"
                            size="small"
                          />
                            {asset.operational_metadata?.publish_status !== 'published' && (
                              <Button
                                size="small"
                                startIcon={<Publish />}
                                variant="contained"
                                color="primary"
                                onClick={() => handlePublishAsset(asset.id)}
                              >
                                Publish
                              </Button>
                            )}
                          </>
                        )}
                        {asset.operational_metadata?.approval_status === 'rejected' && (
                          <Box sx={{ display: 'flex', flexDirection: 'column', gap: 0.5 }}>
                            <Chip
                              icon={<Close />}
                              label="Rejected"
                              color="error"
                              size="small"
                            />
                            {asset.operational_metadata?.rejection_reason && (
                              <Typography variant="caption" color="error" sx={{ fontSize: '0.7rem', fontStyle: 'italic', maxWidth: '250px', wordWrap: 'break-word' }}>
                                Reason: {asset.operational_metadata.rejection_reason}
                              </Typography>
                            )}
                          </Box>
                        )}
                        {asset.operational_metadata?.publish_status === 'published' && (
                          <Chip
                            icon={<Publish />}
                            label="Published"
                            color="info"
                            size="small"
                          />
                        )}
                      </Box>
                    </TableCell>
                  </TableRow>
                ))}
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
            
            <Pagination
              count={totalPages}
              page={currentPage + 1}
              onChange={handlePageChange}
              color="primary"
              showFirstButton
              showLastButton
              disabled={loading}
            />
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
                    const safeLocation = technicalMetadata.location || 'N/A';
                    
                    
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
                    
                    
                    const connectorId = selectedAsset?.connector_id || '';
                    
                    
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
                        
                        {}
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
                        
                        {}
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
                        {safeNumRows > 0 && (
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
                        {safeFileExtension !== 'N/A' && (
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
                    
                    const operationalMetadata = selectedAsset?.operational_metadata || {};
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
                    
                    return (
                      <Grid container spacing={2}>
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
                        <Grid item xs={12}>
                          <Card variant="outlined">
                            <CardContent>
                              <Box sx={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center', mb: 1 }}>
                                <Typography color="text.secondary" gutterBottom sx={{ mb: 0 }}>
                                  Description
                                </Typography>
                                <Button
                                  size="small"
                                  variant="outlined"
                                  startIcon={<Add />}
                                  onClick={() => setTagDialogOpen(true)}
                                  sx={{ 
                                    minWidth: '100px',
                                    maxWidth: '100px',
                                    width: '100px',
                                    padding: '2px 8px',
                                    fontSize: '0.7rem',
                                    ml: 'auto',
                                    height: '24px',
                                    whiteSpace: 'nowrap'
                                  }}
                                >
                                  Add Tag
                                </Button>
                              </Box>
                              {selectedTags.length > 0 ? (
                                <Box sx={{ 
                                  minHeight: '80px',
                                  p: 1.5,
                                  border: '1px solid',
                                  borderColor: 'divider',
                                  borderRadius: 1,
                                  bgcolor: 'background.paper'
                                }}>
                                  <Stack direction="column" spacing={1}>
                                    {selectedTags.map((tag) => (
                                      <Box key={tag.id} sx={{ display: 'flex', alignItems: 'flex-start', gap: 1 }}>
                                        <Chip
                                          label={
                                            <Box>
                                              <Typography variant="body2" sx={{ fontWeight: 600 }}>
                                                {tag.tag_name}
                                              </Typography>
                                              {tag.description && (
                                                <Typography variant="caption" color="text.secondary" sx={{ display: 'block', mt: 0.5 }}>
                                                  {tag.description}
                                                </Typography>
                                              )}
                                            </Box>
                                          }
                                          onDelete={() => handleRemoveTag(tag.id)}
                                          deleteIcon={<Close fontSize="small" />}
                                          color="primary"
                                          variant="outlined"
                                          sx={{ 
                                            height: 'auto',
                                            py: 0.5,
                                            '& .MuiChip-label': {
                                              display: 'block',
                                              whiteSpace: 'normal',
                                              py: 0.5
                                            }
                                          }}
                                        />
                                      </Box>
                                    ))}
                                  </Stack>
                                </Box>
                              ) : (
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
                              )}
                            </CardContent>
                          </Card>
                        </Grid>
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
                  <Typography variant="h6" sx={{ mb: 2, fontWeight: 600 }}>
                    Columns & PII Detection
                  </Typography>
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
                                  <TableCell>Column Name</TableCell>
                                  <TableCell>Data Type</TableCell>
                                  <TableCell>Nullable</TableCell>
                                  <TableCell>Description</TableCell>
                                  <TableCell>PII Status</TableCell>
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
                                      <TableCell>
                                        <Typography variant="body2" sx={{ fontWeight: 500 }}>
                                          {column.name || 'Unknown'}
                                        </Typography>
                                      </TableCell>
                                      <TableCell>
                                        <Chip label={column.type || 'Unknown'} size="small" variant="outlined" />
                                      </TableCell>
                                      <TableCell>
                                        {column.nullable ? 'Yes' : 'No'}
                                      </TableCell>
                                      <TableCell>
                                        <Typography variant="body2" color="text.secondary">
                                          {column.description || 'No description'}
                                        </Typography>
                                      </TableCell>
                                      <TableCell>
                                        {column.pii_detected ? (
                                          <Chip 
                                            icon={<Warning />}
                                            label={`PII: ${(column.pii_types && column.pii_types.length > 0) ? column.pii_types.join(', ') : 'Unknown'}`} 
                                            color="error" 
                                            size="small"
                                            sx={{ cursor: 'pointer', '&:hover': { opacity: 0.8 } }}
                                            onClick={() => handleOpenPiiDialog(column)}
                                          />
                                        ) : (
                                          <Chip 
                                            icon={<CheckCircle />}
                                            label="No PII" 
                                            color="success" 
                                            size="small"
                                            sx={{ cursor: 'pointer', '&:hover': { opacity: 0.8 } }}
                                            onClick={() => handleOpenPiiDialog(column)}
                                          />
                                        )}
                                      </TableCell>
                                      {hasPiiOrChanging && (() => {
                                        const isPii = column.pii_detected || false;
                                        // Check if this column is being changed from Non-PII to PII in the dialog
                                        const isChangingToPii = piiDialogOpen && 
                                                               selectedColumnForPii?.name === column.name && 
                                                               piiDialogIsPii && 
                                                               (originalPiiStatus[column.name] === false || !column.pii_detected);
                                        const showMasking = isPii || isChangingToPii;
                                        const maskingLogic = columnMaskingLogic[column.name] || {
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
                                        
                                        // Check if there are unsaved changes
                                        const hasUnsavedChanges = unsavedMaskingChanges[column.name] || false;
                                        const isSaving = savingMaskingLogic[column.name] || false;
                                        
                                        return (
                                          <>
                                            <TableCell>
                                              <Box sx={{ display: 'flex', gap: 1, alignItems: 'center' }}>
                                                <FormControl size="small" fullWidth>
                                                  <Select
                                                    value={maskingLogic.analytical || ''}
                                                    onChange={(e) => {
                                                      setColumnMaskingLogic(prev => ({
                                                        ...prev,
                                                        [column.name]: {
                                                          ...maskingLogic,
                                                          analytical: e.target.value
                                                        }
                                                      }));
                                                      // Mark as having unsaved changes
                                                      setUnsavedMaskingChanges(prev => ({
                                                        ...prev,
                                                        [column.name]: true
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
                                                      setColumnMaskingLogic(prev => ({
                                                        ...prev,
                                                        [column.name]: {
                                                          ...maskingLogic,
                                                          operational: e.target.value
                                                        }
                                                      }));
                                                      // Mark as having unsaved changes
                                                      setUnsavedMaskingChanges(prev => ({
                                                        ...prev,
                                                        [column.name]: true
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
      <Dialog open={tagDialogOpen} onClose={() => {
        setTagDialogOpen(false);
        setSelectedTag('');
      }} maxWidth="sm" fullWidth>
        <DialogTitle>Add Business Glossary Tag</DialogTitle>
        <DialogContent>
          <Typography variant="body2" sx={{ mb: 2 }}>
            Select a tag from the business glossary to add to the description:
          </Typography>
          {loadingTags ? (
            <Box sx={{ display: 'flex', justifyContent: 'center', p: 3 }}>
              <CircularProgress />
            </Box>
          ) : (
            <FormControl fullWidth sx={{ mt: 1 }}>
              <InputLabel id="tag-select-label" shrink={true}>Select Glossary Tag</InputLabel>
              <Select
                labelId="tag-select-label"
                value={selectedTag}
                onChange={(e) => setSelectedTag(e.target.value)}
                label="Select Glossary Tag"
                displayEmpty
                renderValue={(value) => {
                  if (!value) {
                    return <span style={{ color: '#999' }}>Select a tag...</span>;
                  }
                  const tag = availableTags.find(t => t.id.toString() === value);
                  return tag ? tag.tag_name : '';
                }}
                sx={{
                  '& .MuiSelect-select': {
                    display: 'flex',
                    alignItems: 'center'
                  }
                }}
              >
                {availableTags.map((tag) => (
                  <MenuItem key={tag.id} value={tag.id.toString()}>
                    <Box sx={{ width: '100%' }}>
                      <Typography variant="body1" sx={{ fontWeight: 500 }}>
                        {tag.tag_name}
                      </Typography>
                      {tag.description && (
                        <Typography variant="body2" color="text.secondary" sx={{ fontSize: '0.75rem' }}>
                          {tag.description}
                        </Typography>
                      )}
                    </Box>
                  </MenuItem>
                ))}
              </Select>
            </FormControl>
          )}
          {availableTags.length === 0 && !loadingTags && (
            <Typography variant="body2" color="text.secondary" sx={{ mt: 2, fontStyle: 'italic' }}>
              No tags available. Tags need to be created in the metadata_tags table first.
            </Typography>
          )}
        </DialogContent>
        <DialogActions>
          <Button onClick={() => {
            setTagDialogOpen(false);
            setSelectedTag('');
          }}>
            Cancel
          </Button>
          <Button 
            onClick={handleAddTagToDescription} 
            variant="contained" 
            color="primary"
            disabled={!selectedTag || loadingTags}
          >
            Add Tag
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
