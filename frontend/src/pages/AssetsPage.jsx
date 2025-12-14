import React, { useState, useEffect } from 'react';
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
} from '@mui/icons-material';

const AssetsPage = () => {
  const [assets, setAssets] = useState([]);
  const [loading, setLoading] = useState(false);
  const [searchTerm, setSearchTerm] = useState('');
  const [typeFilter, setTypeFilter] = useState('');
  const [catalogFilter, setCatalogFilter] = useState('');
  const [selectedAsset, setSelectedAsset] = useState(null);
  const [detailsDialogOpen, setDetailsDialogOpen] = useState(false);
  const [activeTab, setActiveTab] = useState(0);
  const [classification, setClassification] = useState('internal');
  const [sensitivityLevel, setSensitivityLevel] = useState('medium');
  const [originalClassification, setOriginalClassification] = useState('internal');
  const [originalSensitivityLevel, setOriginalSensitivityLevel] = useState('medium');
  const [savingMetadata, setSavingMetadata] = useState(false);
  
  // Pagination state
  const [currentPage, setCurrentPage] = useState(0);
  const [pageSize, setPageSize] = useState(50);
  const [totalAssets, setTotalAssets] = useState(0);
  const [totalPages, setTotalPages] = useState(0);
  const [allAssets, setAllAssets] = useState([]); // For filters

  useEffect(() => {
    fetchAssets();
  }, [currentPage, pageSize, searchTerm, typeFilter, catalogFilter]);

  const fetchAssets = async (pageOverride = null) => {
      setLoading(true);
    try {
      const response = await fetch('http://localhost:8099/api/assets');
      if (response.ok) {
      const data = await response.json();
        setAllAssets(data);
        setTotalAssets(data.length);
        
        // Apply filters if any
        let filtered = data;
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
        
        // Pagination
        const page = pageOverride !== null ? pageOverride : currentPage;
        const start = page * pageSize;
        const end = start + pageSize;
        setAssets(filtered.slice(start, end));
        setTotalPages(Math.ceil(filtered.length / pageSize));
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

  // Get unique types and catalogs for filter dropdowns
  const uniqueTypes = [...new Set(allAssets.map(asset => asset.type))];
  const uniqueCatalogs = [...new Set(allAssets.map(asset => asset.catalog))];

  const getDataSource = (connectorId) => {
    if (!connectorId) return 'Unknown';
    if (connectorId.startsWith('parquet_test_')) return 'Parquet Files';
    return 'Unknown';
  };

  const getDataSourceColor = (connectorId) => {
    if (connectorId && connectorId.startsWith('parquet_test_')) return 'primary';
    return 'default';
  };

  const handleViewAsset = async (assetId) => {
    // Fetch latest asset data from backend to ensure we have the most up-to-date tags
    try {
      const API_BASE_URL = import.meta.env.VITE_API_BASE_URL || 'http://localhost:8099';
      const response = await fetch(`${API_BASE_URL}/api/assets`);
      if (response.ok) {
        const allAssetsData = await response.json();
        const asset = allAssetsData.find(a => a.id === assetId);
        if (asset) {
          // Update the allAssets state with latest data
          setAllAssets(allAssetsData);
          setSelectedAsset(asset);
      setDetailsDialogOpen(true);
          setOriginalClassification(asset.business_metadata?.classification || 'internal');
          setOriginalSensitivityLevel(asset.business_metadata?.sensitivity_level || 'medium');
          setClassification(asset.business_metadata?.classification || 'internal');
          setSensitivityLevel(asset.business_metadata?.sensitivity_level || 'medium');
        }
      } else {
        // Fallback to cached data if fetch fails
        const asset = allAssets.find(a => a.id === assetId);
        if (asset) {
          setSelectedAsset(asset);
          setDetailsDialogOpen(true);
          setOriginalClassification(asset.business_metadata?.classification || 'internal');
          setOriginalSensitivityLevel(asset.business_metadata?.sensitivity_level || 'medium');
          setClassification(asset.business_metadata?.classification || 'internal');
          setSensitivityLevel(asset.business_metadata?.sensitivity_level || 'medium');
        }
      }
    } catch (error) {
      // Log error in development only
      if (import.meta.env.DEV) {
        console.error('Error fetching asset:', error);
      }
      // Fallback to cached data if fetch fails
      const asset = allAssets.find(a => a.id === assetId);
      if (asset) {
        setSelectedAsset(asset);
        setDetailsDialogOpen(true);
        setOriginalClassification(asset.business_metadata?.classification || 'internal');
        setOriginalSensitivityLevel(asset.business_metadata?.sensitivity_level || 'medium');
        setClassification(asset.business_metadata?.classification || 'internal');
        setSensitivityLevel(asset.business_metadata?.sensitivity_level || 'medium');
      }
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
      const API_BASE_URL = import.meta.env.VITE_API_BASE_URL || 'http://localhost:8099';
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
        // Update in local state
        setAllAssets(prev => prev.map(a => a.id === updatedAsset.id ? updatedAsset : a));
        setSelectedAsset(updatedAsset);
        setOriginalClassification(classification);
        setOriginalSensitivityLevel(sensitivityLevel);
      alert('Metadata saved successfully!');
      } else {
        throw new Error('Failed to save metadata');
      }
    } catch (error) {
      // Log error in development only
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

  // Pagination handlers
  const handlePageChange = (event, page) => {
    setCurrentPage(page - 1); // Convert to 0-based
  };

  const handlePageSizeChange = (event) => {
    const newSize = parseInt(event.target.value, 10);
    setPageSize(newSize);
    setCurrentPage(0); // Reset to first page
  };

  // Search and filter handlers
  const handleSearchChange = (event) => {
    setSearchTerm(event.target.value);
    setCurrentPage(0); // Reset to first page
  };

  const handleTypeFilterChange = (event) => {
    setTypeFilter(event.target.value);
    setCurrentPage(0); // Reset to first page
  };

  const handleCatalogFilterChange = (event) => {
    setCatalogFilter(event.target.value);
    setCurrentPage(0); // Reset to first page
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
            onClick={() => {
              fetchAssets();
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
            <Grid item xs={12} md={4}>
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
            <Grid item xs={12} md={3}>
              <FormControl fullWidth>
                <InputLabel>Type</InputLabel>
                <Select
                  value={typeFilter}
                  label="Type"
                  onChange={handleTypeFilterChange}
                >
                  <MenuItem value="">All Types</MenuItem>
                  {uniqueTypes.map(type => (
                    <MenuItem key={type} value={type}>{type}</MenuItem>
                  ))}
                </Select>
              </FormControl>
            </Grid>
            <Grid item xs={12} md={3}>
              <FormControl fullWidth>
                <InputLabel>Catalog</InputLabel>
                <Select
                  value={catalogFilter}
                  label="Catalog"
                  onChange={handleCatalogFilterChange}
                >
                  <MenuItem value="">All Catalogs</MenuItem>
                  {uniqueCatalogs.map(catalog => (
                    <MenuItem key={catalog} value={catalog}>{catalog}</MenuItem>
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
                  setTypeFilter('');
                  setCatalogFilter('');
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
                      <Typography variant="body2" color="text.secondary" sx={{ fontFamily: 'Roboto' }}>
                        {new Date(asset.discovered_at).toLocaleDateString()}
                      </Typography>
                    </TableCell>
                    <TableCell>
                      <Button
                        size="small"
                        startIcon={<Visibility />}
                        variant="outlined"
                        onClick={() => handleViewAsset(asset.id)}
                      >
                        View
                      </Button>
                    </TableCell>
                  </TableRow>
                ))}
              </TableBody>
            </Table>
          </TableContainer>
          
          {/* Pagination Controls */}
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

      {/* Asset Details Dialog */}
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

              {/* Technical Metadata Tab */}
              {activeTab === 0 && (
                <Box>
                  <Typography variant="h6" sx={{ mb: 2, fontWeight: 600 }}>
                    Technical Metadata
                  </Typography>
                  {(() => {
                    // Defensive programming: ensure technical_metadata exists and has all required fields
                    const technicalMetadata = selectedAsset?.technical_metadata || {};
                    const safeAssetId = technicalMetadata.asset_id || selectedAsset?.id || 'N/A';
                    const safeAssetType = technicalMetadata.asset_type || selectedAsset?.type || 'Unknown';
                    const safeLocation = technicalMetadata.location || 'N/A';
                    const safeFormat = technicalMetadata.format || technicalMetadata.content_type || 'Unknown';
                    const safeSizeBytes = technicalMetadata.size_bytes || 0;
                    const safeNumRows = technicalMetadata.num_rows || 0;
                    const safeCreatedAt = technicalMetadata.created_at || selectedAsset?.discovered_at || new Date().toISOString();
                    const safeFileExtension = technicalMetadata.file_extension || 'N/A';
                    
                    return (
                      <Grid container spacing={2}>
                        <Grid item xs={6}>
                          <Card variant="outlined">
                            <CardContent>
                              <Typography color="text.secondary" gutterBottom>
                                Asset ID
                              </Typography>
                              <Typography variant="body1" sx={{ wordBreak: 'break-all' }}>
                                {safeAssetId}
                              </Typography>
                            </CardContent>
                          </Card>
                        </Grid>
                        <Grid item xs={6}>
                          <Card variant="outlined">
                            <CardContent>
                              <Typography color="text.secondary" gutterBottom>
                                Asset Type
                              </Typography>
                              <Typography variant="body1">
                                {safeAssetType}
                              </Typography>
                            </CardContent>
                          </Card>
                        </Grid>
                        <Grid item xs={6}>
                          <Card variant="outlined">
                            <CardContent>
                              <Typography color="text.secondary" gutterBottom>
                                Location
                              </Typography>
                              <Typography variant="body1">
                                {safeLocation}
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
                                Number of Rows
                              </Typography>
                              <Typography variant="body1">
                                {formatNumber(safeNumRows)}
                                  </Typography>
                                </CardContent>
                              </Card>
                            </Grid>
                            {safeFileExtension !== 'N/A' && (
                              <Grid item xs={6}>
                                <Card variant="outlined">
                                  <CardContent>
                                    <Typography color="text.secondary" gutterBottom>
                                      File Extension
                                    </Typography>
                                    <Typography variant="body1">
                                      .{safeFileExtension}
                                    </Typography>
                                  </CardContent>
                                </Card>
                              </Grid>
                            )}
                            {technicalMetadata.content_type && (
                              <Grid item xs={6}>
                                <Card variant="outlined">
                                  <CardContent>
                                    <Typography color="text.secondary" gutterBottom>
                                      Content Type (MIME)
                                    </Typography>
                                    <Typography variant="body1" sx={{ fontSize: '0.875rem' }}>
                                      {technicalMetadata.content_type}
                                    </Typography>
                                  </CardContent>
                                </Card>
                              </Grid>
                            )}
                        <Grid item xs={12}>
                          <Card variant="outlined">
                            <CardContent>
                              <Typography color="text.secondary" gutterBottom>
                                Created At
                              </Typography>
                              <Typography variant="body1">
                                {new Date(safeCreatedAt).toLocaleString()}
                              </Typography>
                            </CardContent>
                          </Card>
                        </Grid>
                      </Grid>
                    );
                  })()}
                </Box>
              )}

              {/* Operational Metadata Tab */}
              {activeTab === 1 && (
                <Box>
                  <Typography variant="h6" sx={{ mb: 2, fontWeight: 600 }}>
                    Operational Metadata
                  </Typography>
                  {(() => {
                    // Defensive programming: ensure operational_metadata exists and has all required fields
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

              {/* Business Metadata Tab */}
              {activeTab === 2 && (
                <Box>
                  <Typography variant="h6" sx={{ mb: 2, fontWeight: 600 }}>
                    Business Metadata
                  </Typography>
                  {(() => {
                    // Defensive programming: ensure business_metadata exists and has all required fields
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
                                  // Collect all tags from all columns
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

              {/* Columns & PII Tab */}
              {activeTab === 3 && (
                <Box>
                  <Typography variant="h6" sx={{ mb: 2, fontWeight: 600 }}>
                    Columns & PII Detection
                  </Typography>
                  {(() => {
                    // Defensive programming: ensure columns exist and handle missing data
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
                                          label={`PII: ${column.pii_type || 'Unknown'}`} 
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
              {/* Show Cancel and Save Changes only when Business Metadata tab is active OR values have changed */}
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


    </Box>
  );
};

export default AssetsPage;
