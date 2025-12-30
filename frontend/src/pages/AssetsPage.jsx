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
} from '@mui/icons-material';

const AssetsPage = () => {
  const navigate = useNavigate();
  const [assets, setAssets] = useState([]);
  const [loading, setLoading] = useState(false);
  const [searchTerm, setSearchTerm] = useState('');
  const [typeFilter, setTypeFilter] = useState('');
  const [catalogFilter, setCatalogFilter] = useState('');
  const [approvalStatusFilter, setApprovalStatusFilter] = useState('');
  const [selectedAsset, setSelectedAsset] = useState(null);
  const [detailsDialogOpen, setDetailsDialogOpen] = useState(false);
  const [activeTab, setActiveTab] = useState(0);
  const [classification, setClassification] = useState('internal');
  const [sensitivityLevel, setSensitivityLevel] = useState('medium');
  const [originalClassification, setOriginalClassification] = useState('internal');
  const [originalSensitivityLevel, setOriginalSensitivityLevel] = useState('medium');
  const [savingMetadata, setSavingMetadata] = useState(false);
  
  
  const [rejectDialogOpen, setRejectDialogOpen] = useState(false);
  const [rejectReason, setRejectReason] = useState('');
  const [assetToReject, setAssetToReject] = useState(null);
  
  
  const [discoveryDetailsOpen, setDiscoveryDetailsOpen] = useState(false);
  const [discoveryDetails, setDiscoveryDetails] = useState(null);
  
  const [currentPage, setCurrentPage] = useState(0);
  const [pageSize, setPageSize] = useState(50);
  const [totalAssets, setTotalAssets] = useState(0);
  const [totalPages, setTotalPages] = useState(0);
  const [allAssets, setAllAssets] = useState([]); 

  useEffect(() => {
    fetchAssets();
  }, [currentPage, pageSize, searchTerm, typeFilter, catalogFilter, approvalStatusFilter]);

  const fetchAssets = async (pageOverride = null) => {
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
        if (typeFilter) {
          filtered = filtered.filter(asset => asset.type === typeFilter);
        }
        if (catalogFilter) {
          filtered = filtered.filter(asset => asset.catalog === catalogFilter);
        }
        if (approvalStatusFilter) {
          filtered = filtered.filter(asset => {
            const status = asset.operational_metadata?.approval_status || 'pending_review';
            return status === approvalStatusFilter;
          });
        }
        
        setAssets(filtered);
        } else {
        setAssets([]);
        setTotalAssets(0);
        setTotalPages(0);
      }
    } catch (error) {
      console.error('Error fetching assets:', error);
      setAssets([]);
      setTotalAssets(0);
      setTotalPages(0);
    } finally {
      setLoading(false);
    }
  };

  
  const uniqueTypes = [...new Set(allAssets.map(asset => asset.type))];
  const uniqueCatalogs = [...new Set(allAssets.map(asset => asset.catalog))];

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
        
        const updatedAssetFromServer = {
          ...asset,
          ...result,
          operational_metadata: {
            ...(asset?.operational_metadata || {}),
            ...(result.operational_metadata || {}),
            approval_status: 'approved',
            approved_at: result.updated_at || result.operational_metadata?.approved_at || new Date().toISOString()
          }
        };
        
        if (import.meta.env.DEV) {
          console.log('Server response - Updated asset:', assetId, updatedAssetFromServer.operational_metadata);
        }
        
        
        const updatedAllAssets = allAssets.map(a => a.id === assetId ? updatedAssetFromServer : a);
        setAllAssets(updatedAllAssets);
        
        
        let filtered = updatedAllAssets;
        if (searchTerm) {
          filtered = filtered.filter(a => 
            a.name.toLowerCase().includes(searchTerm.toLowerCase()) ||
            (a.catalog && a.catalog.toLowerCase().includes(searchTerm.toLowerCase()))
          );
        }
        if (typeFilter) {
          filtered = filtered.filter(a => a.type === typeFilter);
        }
        if (catalogFilter) {
          filtered = filtered.filter(a => a.catalog === catalogFilter);
        }
        if (approvalStatusFilter) {
          filtered = filtered.filter(a => {
            const status = a.operational_metadata?.approval_status || 'pending_review';
            return status === approvalStatusFilter;
          });
        }
        const page = currentPage;
        const start = page * pageSize;
        const end = start + pageSize;
        setAssets(filtered.slice(start, end));
      } else {
        
        await fetchAssets();
        const errorData = await response.json();
        throw new Error(errorData.error || 'Failed to approve asset');
      }
    } catch (error) {
      
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
    setRejectDialogOpen(true);
  };

  const handleRejectConfirm = async () => {
    if (!assetToReject) return;
    if (!rejectReason.trim()) {
      alert('Please provide a reason for rejection');
      return;
    }
    
    setRejectDialogOpen(false);
    
    
    const asset = allAssets.find(a => a.id === assetToReject);
    if (asset) {
      const updatedAsset = {
        ...asset,
        operational_metadata: {
          ...asset.operational_metadata,
          approval_status: 'rejected',
          rejected_at: new Date().toISOString(),
          rejection_reason: rejectReason
        }
      };
      setAllAssets(prev => prev.map(a => a.id === assetToReject ? updatedAsset : a));
      setAssets(prev => prev.map(a => a.id === assetToReject ? updatedAsset : a));
    }
    
    try {
      const API_BASE_URL = import.meta.env.VITE_API_BASE_URL;
      const response = await fetch(`${API_BASE_URL}/api/assets/${assetToReject}/reject`, {
        method: 'POST',
        headers: {
          'Content-Type': 'application/json',
        },
        body: JSON.stringify({ reason: rejectReason }),
      });
      
      if (response.ok) {
        const result = await response.json();
        await fetchAssets();
      } else {
        await fetchAssets();
        const errorData = await response.json();
        throw new Error(errorData.error || 'Failed to reject asset');
      }
    } catch (error) {
      await fetchAssets();
      if (import.meta.env.DEV) {
        console.error('Error rejecting asset:', error);
      }
      alert(`Failed to reject asset: ${error.message}`);
    } finally {
      setAssetToReject(null);
      setRejectReason('');
    }
  };

  const handlePublishAsset = async (assetId) => {
    const asset = allAssets.find(a => a.id === assetId);
    if (!asset) {
      alert('Asset not found');
      return;
    }

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
          throw new Error('Discovery ID not returned from publish endpoint');
        }
        
        const discoveryResponse = await fetch(`${API_BASE_URL}/api/discovery/${discoveryId}`, {
          method: 'GET',
          headers: {
            'Content-Type': 'application/json',
          },
        });
        
        if (!discoveryResponse.ok) {
          throw new Error('Failed to fetch discovery details');
        }
        
        const discoveryData = await discoveryResponse.json();
        
        if (import.meta.env.DEV) {
          console.log('Full discovery data fetched:', discoveryData);
          console.log('Discovery data keys:', Object.keys(discoveryData));
        }
        
        await fetchAssets();
        
        navigate(`/app/dataOnboarding?id=${discoveryId}`, {
          state: { 
            discovery: discoveryData,
            asset: updatedAsset,
            discoveryId: discoveryId
          }
        });
      } else {
        await fetchAssets();
        const errorData = await response.json();
        throw new Error(errorData.error || 'Failed to publish asset');
      }
    } catch (error) {
      await fetchAssets();
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
      setClassification(cachedAsset.business_metadata?.classification || 'internal');
      setSensitivityLevel(cachedAsset.business_metadata?.sensitivity_level || 'medium');
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
        setClassification(asset.business_metadata?.classification || 'internal');
        setSensitivityLevel(asset.business_metadata?.sensitivity_level || 'medium');
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
    setOriginalClassification('internal');
    setOriginalSensitivityLevel('medium');
  };

  const handleSaveMetadata = async () => {
    if (!selectedAsset) return;
    setSavingMetadata(true);
    try {
      const API_BASE_URL = import.meta.env.VITE_API_BASE_URL;
      const response = await fetch(`${API_BASE_URL}/api/assets/${selectedAsset.id}`, {
        method: 'PUT',
        headers: {
          'Content-Type': 'application/json',
        },
        body: JSON.stringify({
          business_metadata: {
            ...selectedAsset.business_metadata,
          classification: classification,
          sensitivity_level: sensitivityLevel,
          }
        }),
      });

      if (response.ok) {
        const updatedAsset = await response.json();
        
        setAllAssets(prev => prev.map(a => a.id === updatedAsset.id ? updatedAsset : a));
        setSelectedAsset(updatedAsset);
        setOriginalClassification(classification);
        setOriginalSensitivityLevel(sensitivityLevel);
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

  const handleTypeFilterChange = (event) => {
    setTypeFilter(event.target.value);
    setCurrentPage(0); 
  };

  const handleCatalogFilterChange = (event) => {
    setCatalogFilter(event.target.value);
    setCurrentPage(0); 
  };

  const handleApprovalStatusFilterChange = (event) => {
    setApprovalStatusFilter(event.target.value);
    setCurrentPage(0);
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
                
                
                await fetchAssets();
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
              <FormControl fullWidth>
                <InputLabel id="type-filter-label" shrink>Type</InputLabel>
                <Select
                  labelId="type-filter-label"
                  value={typeFilter}
                  label="Type"
                  onChange={handleTypeFilterChange}
                  displayEmpty
                  notched
                  renderValue={(selected) => {
                    if (selected === '' || !selected) {
                      return 'All Types';
                    }
                    return selected;
                  }}
                >
                  <MenuItem value="">All Types</MenuItem>
                  {uniqueTypes.map(type => (
                    <MenuItem key={type} value={type}>{type}</MenuItem>
                  ))}
                </Select>
              </FormControl>
            </Grid>
            <Grid item xs={12} md={2.5}>
              <FormControl fullWidth>
                <InputLabel id="catalog-filter-label" shrink>Catalog</InputLabel>
                <Select
                  labelId="catalog-filter-label"
                  value={catalogFilter}
                  label="Catalog"
                  onChange={handleCatalogFilterChange}
                  displayEmpty
                  notched
                  renderValue={(selected) => {
                    if (selected === '' || !selected) {
                      return 'All Catalogs';
                    }
                    return selected;
                  }}
                >
                  <MenuItem value="">All Catalogs</MenuItem>
                  {uniqueCatalogs.map(catalog => (
                    <MenuItem key={catalog} value={catalog}>{catalog}</MenuItem>
                  ))}
                </Select>
              </FormControl>
            </Grid>
            <Grid item xs={12} md={2.5}>
              <FormControl fullWidth>
                <InputLabel id="status-filter-label" shrink>Status</InputLabel>
                <Select
                  labelId="status-filter-label"
                  value={approvalStatusFilter}
                  label="Status"
                  onChange={handleApprovalStatusFilterChange}
                  displayEmpty
                  notched
                  renderValue={(selected) => {
                    if (selected === '' || !selected) {
                      return 'All Statuses';
                    }
                    if (selected === 'pending_review') {
                      return 'Pending Review';
                    }
                    return selected.charAt(0).toUpperCase() + selected.slice(1);
                  }}
                >
                  <MenuItem value="">All Statuses</MenuItem>
                  <MenuItem value="pending_review">Pending Review</MenuItem>
                  <MenuItem value="approved">Approved</MenuItem>
                  <MenuItem value="rejected">Rejected</MenuItem>
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
                  setTypeFilter('');
                  setCatalogFilter('');
                  setApprovalStatusFilter('');
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
                  <TableCell>Format</TableCell>
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
                        {asset.technical_metadata?.format || asset.technical_metadata?.content_type || 'Unknown'}
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
                          <Chip
                            icon={<Close />}
                            label="Rejected"
                            color="error"
                            size="small"
                          />
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
        maxWidth="lg"
        fullWidth
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
                      : operationalMetadata.owner || 'account_admin';
                    const safeLastModified = operationalMetadata.last_modified || operationalMetadata.last_updated_at || selectedAsset?.discovered_at || new Date().toISOString();
                    const safeLastAccessed = operationalMetadata.last_accessed || operationalMetadata.last_updated_at || new Date().toISOString();
                    const safeAccessCount = operationalMetadata.access_count || operationalMetadata.access_level || 'internal';
                    const safeDataQualityScore = operationalMetadata.data_quality_score || 95;
                    
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
                                Data Quality Score
                              </Typography>
                              <Typography variant="body1">
                                {safeDataQualityScore}%
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
                    const safeBusinessOwner = businessMetadata.business_owner || 'Unknown';
                    const safeDepartment = businessMetadata.department || 'N/A';
                    const safeClassification = businessMetadata.classification || 'internal';
                    const safeSensitivityLevel = businessMetadata.sensitivity_level || 'medium';
                    const safeTags = businessMetadata.tags || [];
                    
                    return (
                      <Grid container spacing={2}>
                        <Grid item xs={12}>
                          <Card variant="outlined">
                            <CardContent>
                              <Typography color="text.secondary" gutterBottom>
                                Description
                              </Typography>
                              <Typography variant="body1">
                                {safeDescription}
                              </Typography>
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
                              <Typography variant="body1">
                                {safeDepartment}
                              </Typography>
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
                                {selectedAsset?.business_metadata?.tags && selectedAsset.business_metadata.tags.length > 0 ? (
                                  selectedAsset.business_metadata.tags.map((tag, index) => (
                                    <Chip 
                                      key={index} 
                                      label={tag} 
                                      size="small" 
                                      variant="outlined"
                                      sx={{ 
                                        backgroundColor: '#e3f2fd', 
                                        color: '#1565c0', 
                                        border: '1px solid #90caf9',
                                        fontWeight: 600
                                      }}
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
                                {selectedAsset?.columns && selectedAsset.columns.length > 0 ? (() => {
                                  
                                  const allColumnTags = [];
                                  selectedAsset.columns.forEach(column => {
                                    const columnTags = column.tags || [];
                                    columnTags.forEach(tag => {
                                      if (!allColumnTags.includes(tag)) {
                                        allColumnTags.push(tag);
                                      }
                                    });
                                  });
                                  
                                  if (allColumnTags.length > 0) {
                                      return (
                                          <Box sx={{ display: 'flex', gap: 1, flexWrap: 'wrap' }}>
                                        {allColumnTags.map((tag, tagIndex) => (
                                              <Chip 
                                                key={tagIndex} 
                                                label={tag} 
                                                size="small" 
                                                variant="outlined"
                                                sx={{ 
                                                  backgroundColor: '#f3e5f5', 
                                                  color: '#7b1fa2', 
                                                  border: '1px solid #ce93d8',
                                                  fontWeight: 600
                                                }}
                                              />
                                            ))}
                                        </Box>
                                      );
                                    }
                                  return (
                                    <Typography variant="body2" color="text.secondary" sx={{ fontStyle: 'italic' }}>
                                      No column tags
                                    </Typography>
                                  );
                                })() : (
                                  <Typography variant="body2" color="text.secondary" sx={{ fontStyle: 'italic' }}>
                                    No column tags
                                  </Typography>
                                )}
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
                                </TableRow>
                              </TableHead>
                              <TableBody>
                                {columns.map((column, index) => (
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
                                        />
                                      ) : (
                                        <Chip 
                                          icon={<CheckCircle />}
                                          label="No PII" 
                                          color="success" 
                                          size="small"
                                        />
                                      )}
                                    </TableCell>
                                  </TableRow>
                                ))}
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
              {(activeTab === 2 || classification !== originalClassification || sensitivityLevel !== originalSensitivityLevel) && (
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
      <Dialog open={rejectDialogOpen} onClose={() => setRejectDialogOpen(false)} maxWidth="sm" fullWidth>
        <DialogTitle>Reject Asset</DialogTitle>
        <DialogContent>
          <Typography variant="body2" sx={{ mb: 2 }}>
            Please provide a reason for rejecting this asset:
          </Typography>
          <TextField
            autoFocus
            margin="dense"
            label="Rejection Reason"
            fullWidth
            multiline
            rows={4}
            variant="outlined"
            value={rejectReason}
            onChange={(e) => setRejectReason(e.target.value)}
            placeholder="Enter the reason for rejection..."
          />
        </DialogContent>
        <DialogActions>
          <Button onClick={() => {
            setRejectDialogOpen(false);
            setRejectReason('');
            setAssetToReject(null);
          }}>
            Cancel
          </Button>
          <Button 
            onClick={handleRejectConfirm} 
            variant="contained" 
            color="error"
            disabled={!rejectReason.trim()}
          >
            Reject
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

    </Box>
  );
};

export default AssetsPage;
