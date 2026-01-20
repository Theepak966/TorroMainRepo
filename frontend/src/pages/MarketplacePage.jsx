import React, { useState, useEffect } from 'react';
import {
  Box,
  Typography,
  Card,
  CardContent,
  Grid,
  Button,
  TextField,
  FormControl,
  FormLabel,
  RadioGroup,
  FormControlLabel,
  Radio,
  Link,
  Table,
  TableBody,
  TableCell,
  TableContainer,
  TableHead,
  TableRow,
  Paper,
  Chip,
  CircularProgress,
  Alert,
  Dialog,
  DialogTitle,
  DialogContent,
  DialogActions,
  List,
  ListItem,
  ListItemText,
  ListItemSecondaryAction,
  IconButton,
  Snackbar,
  Select,
  MenuItem,
  InputLabel,
  Autocomplete,
  Menu,
  ListItemIcon,
  Tooltip,
} from '@mui/material';
import {
  ArrowBack,
  Search,
  Label,
  TableChart,
  Add,
  Delete,
  Publish,
  AutoAwesome,
  Security,
  ArrowDropDown,
  Folder,
  Schema,
  Archive,
  Info,
} from '@mui/icons-material';

const MarketplacePage = () => {
  const [resourceType, setResourceType] = useState('');
  const [catalog, setCatalog] = useState('');
  const [fileName, setFileName] = useState('');
  const [loading, setLoading] = useState(false);
  const [tableData, setTableData] = useState(null);
  const [error, setError] = useState(null);
  
  
  const [columnTagDialogOpen, setColumnTagDialogOpen] = useState(false);
  const [tableTagDialogOpen, setTableTagDialogOpen] = useState(false);
  const [selectedColumn, setSelectedColumn] = useState(null);
  const [selectedColumnForTag, setSelectedColumnForTag] = useState('');
  const [newTag, setNewTag] = useState('');
  const [snackbarOpen, setSnackbarOpen] = useState(false);
  const [snackbarMessage, setSnackbarMessage] = useState('');
  const [publishing, setPublishing] = useState(false);
  const [sqlDialogOpen, setSqlDialogOpen] = useState(false);
  const [billingInfo, setBillingInfo] = useState({ requiresBilling: false, message: '' });
  const [piiDialogOpen, setPiiDialogOpen] = useState(false);
  const [selectedColumnForPii, setSelectedColumnForPii] = useState(null);
  const [recommendedTagsDialogOpen, setRecommendedTagsDialogOpen] = useState(false);
  const [recommendedTags, setRecommendedTags] = useState({});
  const [tagMenuAnchor, setTagMenuAnchor] = useState(null);

  // Fetch all assets across all pages (pagination-safe)
  const fetchAllAssets = async () => {
    const API_BASE_URL = import.meta.env.VITE_API_BASE_URL;
    
    // Prefer paginated endpoint; fall back to old array response.
    const firstResp = await fetch(`${API_BASE_URL}/api/assets?page=1&per_page=500`);
    if (!firstResp.ok) return [];

    const firstData = await firstResp.json();
    if (Array.isArray(firstData)) {
      return firstData;
    }

    if (!firstData || !Array.isArray(firstData.assets) || !firstData.pagination) {
      return [];
    }

    const all = [...firstData.assets];
    const totalPages = Number(firstData.pagination.total_pages || 1);

    // Safety cap to avoid accidental infinite/huge loops
    const cappedTotalPages = Math.min(totalPages, 200);

    for (let p = 2; p <= cappedTotalPages; p++) {
      const resp = await fetch(`${API_BASE_URL}/api/assets?page=${p}&per_page=500`);
      if (!resp.ok) break;
      const data = await resp.json();
      if (data && Array.isArray(data.assets)) {
        all.push(...data.assets);
      } else {
        break;
      }
    }

    return all;
  };

  const handleSearch = async () => {
    
    if (!catalog || !fileName) {
      setError('Please fill in Catalog and File Name');
      return;
    }

    setLoading(true);
    setError(null);
    setTableData(null);

    try {
      
      // Fetch all assets across all pages to get all catalogs from all connections
      const allAssets = await fetchAllAssets();
      
      let matchingAsset = null;
      
      if (catalog && fileName) {
        matchingAsset = allAssets.find(asset => {
          const catalogMatch = asset.catalog?.toLowerCase() === catalog.toLowerCase();
          const fileNameLower = fileName.toLowerCase();
          const assetNameLower = asset.name?.toLowerCase() || '';
          const nameMatch = assetNameLower === fileNameLower ||
            assetNameLower.includes(fileNameLower) ||
            fileNameLower.includes(assetNameLower);
          return catalogMatch && nameMatch;
        });
        
        if (!matchingAsset) {
          matchingAsset = allAssets.find(asset => {
            const catalogMatch = asset.catalog?.toLowerCase().includes(catalog.toLowerCase()) ||
              catalog.toLowerCase().includes(asset.catalog?.toLowerCase() || '');
            const fileNameLower = fileName.toLowerCase();
            const assetNameLower = asset.name?.toLowerCase() || '';
            const nameMatch = assetNameLower.includes(fileNameLower) ||
              fileNameLower.includes(assetNameLower);
            return catalogMatch && nameMatch;
          });
        }
      } else if (fileName) {
        const fileNameLower = fileName.toLowerCase();
        matchingAsset = allAssets.find(asset => {
          const assetNameLower = asset.name?.toLowerCase() || '';
          return assetNameLower.includes(fileNameLower) ||
            fileNameLower.includes(assetNameLower);
        });
      }

      if (!matchingAsset) {
        const availableCatalogs = [...new Set(allAssets.map(a => a.catalog))];
        const availableFiles = allAssets.map(a => `${a.catalog}/${a.name}`);
        
        let errorMsg = `Asset not found: ${catalog || '(no catalog)'}/${fileName || '(no file name)'}`;
        if (availableCatalogs.length > 0) {
          errorMsg += `\n\nAvailable catalogs: ${availableCatalogs.join(', ')}`;
        }
        if (availableFiles.length > 0 && availableFiles.length <= 10) {
          errorMsg += `\n\nAvailable files:\n${availableFiles.slice(0, 10).join('\n')}`;
        }
        setError(errorMsg);
      setLoading(false);
        return;
      }

      
      const columns = (matchingAsset.columns || []).map(col => ({
        name: col.name,
        type: col.type || 'string',
        nullable: col.nullable !== undefined ? col.nullable : true,
        description: col.description || '',
        tags: col.tags || [],
        piiFound: col.pii_detected || false,
        piiType: col.pii_type || ''
      }));

      setTableData({
        id: matchingAsset.id, 
        name: matchingAsset.name,
        type: matchingAsset.type,
        catalog: matchingAsset.catalog,
        columns: columns,
        tableTags: matchingAsset.business_metadata?.tags || [],
        technical_metadata: matchingAsset.technical_metadata || {},
        operational_metadata: matchingAsset.operational_metadata || {},
        business_metadata: matchingAsset.business_metadata || {}
      });

      setLoading(false);
    } catch (err) {
      if (import.meta.env.DEV) {
      console.error('API call failed:', err.message);
      }
      setError(`Failed to fetch asset details: ${err.message}`);
      setLoading(false);
    }
  };

  
  const handleAddColumnTag = (columnName = null) => {
    if (columnName && typeof columnName === 'string') {
      setSelectedColumnForTag(columnName);
    } else {
      setSelectedColumnForTag('');
    }
    setColumnTagDialogOpen(true);
  };

  const handleAddTableTag = () => {
    setTableTagDialogOpen(true);
  };

  const handleAddTag = async () => {
    if (!newTag.trim() || !tableData) return;

    try {
    
      if (selectedColumnForTag) {
      
      const updatedColumns = tableData.columns.map(col => 
        col.name === selectedColumnForTag 
          ? { ...col, tags: [...(col.tags || []), newTag.trim()] }
          : col
      );
        
        
        const API_BASE_URL = import.meta.env.VITE_API_BASE_URL;
        const response = await fetch(`${API_BASE_URL}/api/assets/${tableData.id}`, {
          method: 'PUT',
          headers: { 'Content-Type': 'application/json' },
          body: JSON.stringify({
            columns: updatedColumns
          }),
        });

        if (response.ok) {
      setTableData({ ...tableData, columns: updatedColumns });
      setSnackbarMessage(`Tag "${newTag}" added to column "${selectedColumnForTag}"`);
        } else {
          throw new Error('Failed to update column tag');
        }
      setColumnTagDialogOpen(false);
    } else {
        
      const tableTags = tableData.tableTags || [];
        const updatedTableTags = [...tableTags, newTag.trim()];
        
        
        const API_BASE_URL = import.meta.env.VITE_API_BASE_URL;
        const response = await fetch(`${API_BASE_URL}/api/assets/${tableData.id}`, {
          method: 'PUT',
          headers: { 'Content-Type': 'application/json' },
          body: JSON.stringify({
            business_metadata: {
              ...tableData.business_metadata,
              tags: updatedTableTags
            }
          }),
        });

        if (response.ok) {
      const updatedTableData = {
        ...tableData,
            tableTags: updatedTableTags,
            business_metadata: {
              ...tableData.business_metadata,
              tags: updatedTableTags
            }
      };
      setTableData(updatedTableData);
          setSnackbarMessage(`Tag "${newTag}" added to table "${tableData.name}"`);
        } else {
          throw new Error('Failed to update table tag');
        }
        setTableTagDialogOpen(true);
    }

    setNewTag('');
    setSelectedColumnForTag('');
    setSnackbarOpen(true);
    } catch (err) {
      if (import.meta.env.DEV) {
        console.error('Error adding tag:', err);
      }
      setSnackbarMessage(`Error: ${err.message}`);
      setSnackbarOpen(true);
    }
  };

  const handleArchiveAndDuplicates = async () => {
    if (!tableData || !tableData.id) return;

    setPublishing(true);
    try {
      
      const currentTags = tableData.tableTags || [];
      if (currentTags.includes('archive')) {
        setSnackbarMessage('Archive tag already exists');
        setSnackbarOpen(true);
        setPublishing(false);
        return;
      }

      
      const updatedTableTags = [...currentTags, 'archive'];
      
      
      const API_BASE_URL = import.meta.env.VITE_API_BASE_URL;
      const response = await fetch(`${API_BASE_URL}/api/assets/${tableData.id}`, {
        method: 'PUT',
          headers: {
            'Content-Type': 'application/json',
          },
        body: JSON.stringify({
          business_metadata: {
            ...tableData.business_metadata,
            tags: updatedTableTags
          }
        }),
      });

      if (response.ok) {
        const updatedTableData = {
          ...tableData,
          tableTags: updatedTableTags,
          business_metadata: {
            ...tableData.business_metadata,
            tags: updatedTableTags
          }
        };
        setTableData(updatedTableData);
        setSnackbarMessage(`Successfully applied table tag 'archive' with description 'Archive and Duplicates' to ${catalog}/${fileName}`);
        setSnackbarOpen(true);
      } else {
        throw new Error('Failed to update asset');
      }
    } catch (err) {
      setSnackbarMessage(`Error applying archive tag: ${err.message}`);
      setSnackbarOpen(true);
      if (import.meta.env.DEV) {
      console.error('Error publishing archive tag:', err);
      }
    } finally {
      setPublishing(false);
    }
  };

  const handlePublishTags = async () => {
    if (!tableData || !tableData.id) return;

    setPublishing(true);
    setSqlDialogOpen(false); 
    
    try {
      
      // Removed hardcoded delay - no longer needed
      
      
      const API_BASE_URL = import.meta.env.VITE_API_BASE_URL;
      const response = await fetch(`${API_BASE_URL}/api/assets/${tableData.id}`, {
        method: 'PUT',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({
          columns: tableData.columns,
          business_metadata: {
            ...tableData.business_metadata,
            tags: tableData.tableTags || []
          }
        }),
      });

      if (!response.ok) {
        throw new Error('Failed to save tags to backend');
      }

      
      const columnTagsInfo = [];
      tableData.columns.forEach(col => {
        if (col.tags && col.tags.length > 0) {
          columnTagsInfo.push({
            columnName: col.name,
            tags: col.tags
          });
        }
      });

      
      const piiColumns = tableData.columns.filter(col => col.piiFound);
      const tableName = tableData.name;
      const fullTableName = `${tableData.catalog}.${tableName}`;
      
      let sqlCommands = [];
      if (piiColumns.length > 0) {
        
        const analyticalSelects = tableData.columns.map(col => {
          if (col.piiFound) {
            return `    '***MASKED***' AS ${col.name}`;
          }
          return `    ${col.name}`;
        });
        const analyticalSQL = `CREATE OR REPLACE VIEW ${fullTableName}_masked_analytical AS\nSELECT\n${analyticalSelects.join(',\n')}\nFROM ${fullTableName};`;
        
        
        const operationalSelects = tableData.columns.map(col => {
          if (col.piiFound && col.piiType !== 'Email') {
            return `    '***MASKED***' AS ${col.name}`;
          }
          return `    ${col.name}`;
        });
        const operationalSQL = `CREATE OR REPLACE VIEW ${fullTableName}_masked_operational AS\nSELECT\n${operationalSelects.join(',\n')}\nFROM ${fullTableName};`;
        
        sqlCommands = [analyticalSQL, operationalSQL];
      }

      
        setBillingInfo({
          requiresBilling: false,
        message: 'Operation successful',
        columnTagsInfo: columnTagsInfo,
        sqlCommands: sqlCommands
      });
      
      setPublishing(false);
      setSqlDialogOpen(true);
    } catch (err) {
      if (import.meta.env.DEV) {
        console.error('Error publishing tags:', err);
      }
      setSnackbarMessage(`Error: ${err.message}`);
        setSnackbarOpen(true);
      setPublishing(false);
    }
  };

  const handleRemoveTag = async (columnName, tagToRemove) => {
    if (!tableData) return;
    
    try {
      const updatedColumns = tableData.columns.map(col => 
        col.name === columnName 
          ? { ...col, tags: col.tags.filter(tag => tag !== tagToRemove) }
          : col
      );
      
      
      const API_BASE_URL = import.meta.env.VITE_API_BASE_URL;
      const response = await fetch(`${API_BASE_URL}/api/assets/${tableData.id}`, {
        method: 'PUT',
        headers: { 'Content-Type': 'application/json' },
          body: JSON.stringify({
          columns: updatedColumns
          }),
        });
        
      if (response.ok) {
        setTableData({ ...tableData, columns: updatedColumns });
        setSnackbarMessage(`✅ Tag "${tagToRemove}" removed from column "${columnName}"`);
      } else {
        throw new Error('Failed to remove column tag');
      }
      setSnackbarOpen(true);
    } catch (err) {
      console.error('Error removing column tag:', err);
      setSnackbarMessage(`Error: ${err.message}`);
      setSnackbarOpen(true);
    }
  };

  const handleShowRecommendedTags = () => {
    if (!tableData || !tableData.columns) {
      setSnackbarMessage('No table data available');
      setSnackbarOpen(true);
      return;
    }
    
    
    const recommendations = {};
    tableData.columns.forEach(col => {
      if (col.piiFound) {
        const name = col.name.toLowerCase();
        const tags = ['PII', 'SENSITIVE', 'DATA_PRIVACY'];
        let sensitivityLevel = 3; 
        
        
        if (name.includes('ssn') || name.includes('social') || name.includes('social_security')) {
          sensitivityLevel = 5; 
          tags.push('SSN', 'CRITICAL_PII', 'ENCRYPT_AT_REST', 'ENCRYPT_IN_TRANSIT');
        } else if (name.includes('credit') || name.includes('card') || name.includes('payment')) {
          sensitivityLevel = 5; 
          tags.push('FINANCIAL', 'PAYMENT_INFO', 'PCI_DSS', 'ENCRYPT_AT_REST', 'ENCRYPT_IN_TRANSIT');
        } else if (name.includes('password') || name.includes('secret') || name.includes('token')) {
          sensitivityLevel = 5; 
          tags.push('CREDENTIALS', 'AUTH_TOKEN', 'HASH_AT_REST', 'NEVER_LOG');
        } else if (name.includes('bank') || name.includes('routing') || name.includes('account')) {
          sensitivityLevel = 5; 
          tags.push('BANKING_INFO', 'FINANCIAL', 'ENCRYPT_AT_REST');
        } else if (name.includes('date') && (name.includes('birth') || name.includes('dob'))) {
          sensitivityLevel = 4; 
          tags.push('DATE_OF_BIRTH', 'PERSONAL_INFO', 'ENCRYPT_AT_REST');
        } else if (name.includes('address') || name.includes('street') || name.includes('zipcode') || name.includes('postal')) {
          sensitivityLevel = 4; 
          tags.push('ADDRESS', 'LOCATION', 'PERSONAL_INFO', 'ENCRYPT_AT_REST');
        } else if (name.includes('passport') || name.includes('license') || name.includes('national_id')) {
          sensitivityLevel = 5; 
          tags.push('GOVERNMENT_ID', 'CRITICAL_PII', 'ENCRYPT_AT_REST', 'ENCRYPT_IN_TRANSIT');
        } else if (name.includes('email') || name.includes('e_mail')) {
          sensitivityLevel = 3; 
          tags.push('EMAIL', 'PERSONAL_INFO', 'MASK_IN_LOGS');
        } else if (name.includes('phone') || name.includes('mobile') || name.includes('cell')) {
          sensitivityLevel = 3; 
          tags.push('PHONE', 'PERSONAL_INFO', 'MASK_IN_LOGS');
        } else if (name.includes('name') && (name.includes('first') || name.includes('last') || name.includes('full'))) {
          sensitivityLevel = 2; 
          tags.push('NAME', 'PERSONAL_INFO', 'MASK_IN_LOGS');
        } else if (name.includes('id') && (name.includes('user') || name.includes('customer') || name.includes('person'))) {
          sensitivityLevel = 2; 
          tags.push('IDENTIFIER', 'PERSONAL_INFO');
        } else {
          sensitivityLevel = 2; 
          tags.push('GENERAL_PII');
        }
        
        
        tags.push(`PII_SENSITIVITY_LEVEL_${sensitivityLevel}`);
        
        recommendations[col.name] = {
          tags: tags,
          sensitivityLevel: sensitivityLevel
        };
      }
    });
    
    setRecommendedTags(recommendations);
    setRecommendedTagsDialogOpen(true);
  };

  const handleApplyRecommendedTag = (columnName, tag) => {
    
    const updatedColumns = tableData.columns.map(col => 
      col.name === columnName 
        ? { ...col, tags: [...new Set([...(col.tags || []), tag])] }
        : col
    );
    setTableData({ ...tableData, columns: updatedColumns });
    
    
    const updatedRecs = { ...recommendedTags };
    if (updatedRecs[columnName] && updatedRecs[columnName].tags) {
      const rec = updatedRecs[columnName];
      rec.tags = rec.tags.filter(t => t !== tag);
      if (rec.tags.length === 0) {
        delete updatedRecs[columnName];
      }
    }
    setRecommendedTags(updatedRecs);
    
    setSnackbarMessage(`Tag "${tag}" added to column "${columnName}"`);
    setSnackbarOpen(true);
  };

  const handleApplyAllRecommendedTags = () => {
    
    const updatedColumns = tableData.columns.map(col => {
      const rec = recommendedTags[col.name];
      const recommendations = rec ? rec.tags : [];
      return {
        ...col,
        tags: [...new Set([...(col.tags || []), ...recommendations])]
      };
    });
    
    setTableData({ ...tableData, columns: updatedColumns });
    setRecommendedTags({});
    setRecommendedTagsDialogOpen(false);
    setSnackbarMessage(`All recommended tags added to PII columns`);
    setSnackbarOpen(true);
  };

  const handleOpenTagMenu = (event) => {
    setTagMenuAnchor(event.currentTarget);
  };

  const handleCloseTagMenu = () => {
    setTagMenuAnchor(null);
  };

  const handleTagMenuClick = (action) => {
    handleCloseTagMenu();
    if (action === 'table') {
      handleAddTableTag();
    } else if (action === 'column') {
      handleAddColumnTag();
    }
  };

  const handleTogglePii = (columnName) => {
    if (!tableData) return;
    
    const column = tableData.columns.find(col => col.name === columnName);
    if (column) {
      setSelectedColumnForPii(column);
      setPiiDialogOpen(true);
    }
  };

  const handleSavePiiChange = () => {
    if (!tableData || !selectedColumnForPii) return;

    const updatedColumns = tableData.columns.map(col => 
      col.name === selectedColumnForPii.name 
        ? { ...col, piiFound: selectedColumnForPii.piiFound, piiType: selectedColumnForPii.piiType || '' }
        : col
    );
    setTableData({ ...tableData, columns: updatedColumns });
    setSnackbarMessage(`✅ PII status updated for column "${selectedColumnForPii.name}"`);
    setSnackbarOpen(true);
    setPiiDialogOpen(false);
    setSelectedColumnForPii(null);
  };

  const handleCloseDialogs = () => {
    setColumnTagDialogOpen(false);
    setTableTagDialogOpen(false);
    setRecommendedTagsDialogOpen(false);
    setSelectedColumn(null);
    setSelectedColumnForTag('');
    setNewTag('');
  };

  return (
    <Box sx={{ 
      width: '100%',
      minHeight: '100vh',
      p: 3,
      bgcolor: '#fafafa'
    }}>
      <Card sx={{ 
        width: '100%',
        boxShadow: '0 4px 12px rgba(0, 0, 0, 0.1)',
        borderRadius: 2
      }}>
        <CardContent sx={{ p: 4 }}>
          {}
          <Box sx={{ display: 'flex', alignItems: 'center', gap: 1, mb: 4 }}>
          <Typography 
            variant="h4" 
            component="h1" 
            sx={{ 
              fontWeight: 600, 
              color: '#1976d2',
              textAlign: 'left'
            }}
          >
            Publish Data Assets to Marketplace
          </Typography>
            <Tooltip
              title={
                <Box>
                  <Typography variant="body2" sx={{ fontWeight: 600, mb: 1 }}>
                    Field Definitions:
                  </Typography>
                  <Typography variant="body2" sx={{ mb: 0.5 }}>
                    <strong>Catalog:</strong> The connection name you gave when creating the connection
                  </Typography>
                  <Typography variant="body2" sx={{ mb: 1, fontStyle: 'italic' }}>
                    Example: "Test" or "My Connection"
                  </Typography>
                  <Typography variant="body2" sx={{ mb: 0.5 }}>
                    <strong>File Name:</strong> The actual name of the asset file
                  </Typography>
                  <Typography variant="body2" sx={{ fontStyle: 'italic' }}>
                    Example: "users_premium" or "sales_q1_2024"
                  </Typography>
                </Box>
              }
              arrow
              placement="right"
            >
              <IconButton size="small" sx={{ color: '#1976d2' }}>
                <Info fontSize="small" />
              </IconButton>
            </Tooltip>
          </Box>

          {}
          <Grid container spacing={3} sx={{ mb: 4 }}>
            {(
              <>
                <Grid item xs={12} sm={5}>
                  <FormControl fullWidth>
                    <Typography 
                      variant="body2" 
                      sx={{ 
                        color: '#1976d2', 
                        fontWeight: 600, 
                        mb: 1,
                        fontSize: '0.875rem'
                      }}
                    >
                      Catalog *
              </Typography>
                    <TextField
                      fullWidth
                      placeholder="Catalog"
                      value={catalog}
                      onChange={(e) => setCatalog(e.target.value)}
                      variant="outlined"
                      size="small"
                      style={{ width: '100%', minWidth: '300px' }}
                      sx={{
                        width: '100% !important',
                        minWidth: '300px !important',
                        '& .MuiOutlinedInput-root': {
                          backgroundColor: '#f5f5f5',
                          width: '100% !important',
                          minWidth: '300px !important',
                          '& fieldset': {
                            borderColor: '#e0e0e0',
                          },
                          '&:hover fieldset': {
                            borderColor: '#e0e0e0',
                          },
                          '&.Mui-focused fieldset': {
                            borderColor: '#1976d2',
                          },
                        },
                      }}
                    />
                  </FormControl>
        </Grid>
                <Grid item xs={12} sm={5}>
                  <FormControl fullWidth>
                    <Typography 
                      variant="body2" 
                      sx={{ 
                        color: '#1976d2', 
                        fontWeight: 600, 
                        mb: 1,
                        fontSize: '0.875rem'
                      }}
                    >
                      File Name *
                    </Typography>
              <TextField
                fullWidth
                      placeholder="File Name"
                      value={fileName}
                      onChange={(e) => setFileName(e.target.value)}
                      variant="outlined"
                      size="small"
                      style={{ width: '100%', minWidth: '300px' }}
                      sx={{
                        width: '100% !important',
                        minWidth: '300px !important',
                        '& .MuiOutlinedInput-root': {
                          backgroundColor: '#f5f5f5',
                          width: '100% !important',
                          minWidth: '300px !important',
                          '& fieldset': {
                            borderColor: '#e0e0e0',
                          },
                          '&:hover fieldset': {
                            borderColor: '#e0e0e0',
                          },
                          '&.Mui-focused fieldset': {
                            borderColor: '#1976d2',
                          },
                        },
                      }}
                    />
              </FormControl>
            </Grid>
              </>
            )}
          </Grid>

          {}
          <Box sx={{ 
            display: 'flex', 
            justifyContent: 'space-between', 
            alignItems: 'center' 
          }}>
            <Button
              variant="outlined"
              startIcon={loading ? <CircularProgress size={20} /> : <Search />}
              onClick={handleSearch}
              disabled={loading}
              sx={{
                borderColor: '#1976d2',
                color: '#1976d2',
                fontWeight: 500,
                px: 3,
                py: 1,
                '&:hover': {
                  borderColor: '#1565c0',
                  backgroundColor: 'rgba(25, 118, 210, 0.04)',
                },
                '&:disabled': {
                  borderColor: '#e0e0e0',
                  color: '#9e9e9e',
                },
              }}
            >
              {loading ? 'Searching...' : 'Search'}
            </Button>
          </Box>

          {}
          {error && (
            <Alert severity="error" sx={{ mt: 2 }}>
              {error}
            </Alert>
          )}

          {}
          {tableData && (
            <Box sx={{ mt: 4 }}>
              <Box sx={{ mb: 2 }}>
                <Box sx={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center', mb: 1 }}>
                  <Typography variant="h6" sx={{ fontWeight: 600 }}>
                    Table: {tableData.name}
                  </Typography>
                  <Box sx={{ display: 'flex', gap: 1 }}>
                  <Button
                    variant="outlined"
                    startIcon={<Label />}
                    endIcon={<ArrowDropDown />}
                    onClick={handleOpenTagMenu}
                    sx={{
                      color: '#1976d2',
                      borderColor: '#1976d2',
                      px: 2,
                      py: 1,
                      fontSize: '0.875rem',
                      fontWeight: 500,
                      textTransform: 'none',
                      '&:hover': {
                        backgroundColor: '#f5f5f5',
                        borderColor: '#1976d2',
                      },
                    }}
                  >
                    Add Tags
                  </Button>
                  <Menu
                    anchorEl={tagMenuAnchor}
                    open={Boolean(tagMenuAnchor)}
                    onClose={handleCloseTagMenu}
                  >
                    <MenuItem onClick={() => handleTagMenuClick('table')}>
                      <ListItemIcon>
                        <Label fontSize="small" />
                      </ListItemIcon>
                      <ListItemText>Add Table Tags</ListItemText>
                    </MenuItem>
                    <MenuItem onClick={() => handleTagMenuClick('column')}>
                      <ListItemIcon>
                        <TableChart fontSize="small" />
                      </ListItemIcon>
                      <ListItemText>Add Column Tags</ListItemText>
                    </MenuItem>
                  </Menu>
                  <Button
                    variant="outlined"
                    startIcon={<Archive />}
                    onClick={handleArchiveAndDuplicates}
                    sx={{
                      color: '#1976d2',
                      borderColor: '#1976d2',
                      px: 2,
                      py: 1,
                      fontSize: '0.875rem',
                      fontWeight: 500,
                      textTransform: 'none',
                      '&:hover': {
                        backgroundColor: '#f5f5f5',
                        borderColor: '#1976d2',
                      },
                    }}
                  >
                    Archive and Duplicates
                  </Button>
                  <Button
                    variant="outlined"
                    startIcon={<Security />}
                    onClick={handleShowRecommendedTags}
                    sx={{
                      color: '#d32f2f',
                      borderColor: '#d32f2f',
                      px: 2,
                      py: 1,
                      fontSize: '0.875rem',
                      fontWeight: 500,
                      textTransform: 'none',
                      '&:hover': {
                        backgroundColor: '#ffebee',
                        borderColor: '#d32f2f',
                      },
                    }}
                  >
                    Recommended PII Tags
                  </Button>
                  <Button
                    variant="contained"
                    startIcon={publishing ? <CircularProgress size={20} /> : <Publish />}
                    onClick={handlePublishTags}
                    disabled={publishing || !tableData}
                    sx={{
                      backgroundColor: '#4caf50',
                      color: 'white',
                      px: 2,
                      py: 1,
                      fontSize: '0.875rem',
                      fontWeight: 500,
                      textTransform: 'none',
                      '&:hover': {
                        backgroundColor: '#45a049',
                      },
                      '&:disabled': {
                        backgroundColor: '#e0e0e0',
                        color: '#9e9e9e',
                      },
                    }}
                  >
                    {publishing ? 'Publishing...' : 'Publish'}
                  </Button>
                </Box>
                </Box>
                
                {}

                {(tableData.tableTags && tableData.tableTags.length > 0) && (
                  <Box sx={{ mb: 2, display: 'flex', gap: 0.5, alignItems: 'center', flexWrap: 'wrap' }}>
                    <Typography variant="body2" sx={{ fontWeight: 600, mr: 1 }}>
                      Table Tags:
                    </Typography>
                    {tableData.tableTags.map((tag, idx) => (
                      <Chip 
                        key={idx}
                        label={tag} 
                        size="small"
                        onDelete={async () => {
                          try {
                          const updatedTableTags = tableData.tableTags.filter((_, i) => i !== idx);
                            
                            
                            const API_BASE_URL = import.meta.env.VITE_API_BASE_URL;
        const response = await fetch(`${API_BASE_URL}/api/assets/${tableData.id}`, {
                              method: 'PUT',
                              headers: { 'Content-Type': 'application/json' },
                              body: JSON.stringify({
                                business_metadata: {
                                  ...tableData.business_metadata,
                                  tags: updatedTableTags
                                }
                              }),
                            });

                            if (response.ok) {
                              setTableData({ 
                                ...tableData, 
                                tableTags: updatedTableTags,
                                business_metadata: {
                                  ...tableData.business_metadata,
                                  tags: updatedTableTags
                                }
                              });
                          setSnackbarMessage(`Tag "${tag}" removed from table`);
                            } else {
                              throw new Error('Failed to remove table tag');
                            }
                          setSnackbarOpen(true);
                          } catch (err) {
                            console.error('Error removing tag:', err);
                            setSnackbarMessage(`Error: ${err.message}`);
                            setSnackbarOpen(true);
                          }
                        }}
                        deleteIcon={<Delete fontSize="small" />}
                        sx={{
                          backgroundColor: '#e3f2fd',
                          color: '#1565c0',
                          border: '1px solid #90caf9',
                          fontWeight: 600,
                          fontSize: '0.75rem',
                          '&:hover': {
                            backgroundColor: '#bbdefb',
                          },
                        }}
                        variant="filled"
                      />
                    ))}
                  </Box>
                )}
              </Box>
              <TableContainer component={Paper} sx={{ mt: 2 }}>
            <Table>
              <TableHead>
                    <TableRow sx={{ backgroundColor: '#f5f5f5' }}>
                      <TableCell sx={{ fontWeight: 600 }}>Column Name</TableCell>
                      <TableCell sx={{ fontWeight: 600 }}>Type</TableCell>
                      <TableCell sx={{ fontWeight: 600 }}>Mode</TableCell>
                      <TableCell sx={{ fontWeight: 600 }}>Description</TableCell>
                      <TableCell sx={{ fontWeight: 600 }}>PII Found</TableCell>
                      <TableCell sx={{ fontWeight: 600 }}>Tags</TableCell>
                </TableRow>
              </TableHead>
              <TableBody>
                    {tableData.columns.map((column, index) => (
                      <TableRow key={index}>
                        <TableCell sx={{ fontWeight: 500 }}>{column.name}</TableCell>
                    <TableCell>
                      <Chip 
                            label={column.type} 
                        size="small" 
                            color="primary" 
                        variant="outlined"
                      />
                    </TableCell>
                    <TableCell>
                      <Chip 
                            label={column.nullable !== false ? 'NULLABLE' : 'REQUIRED'} 
                        size="small" 
                            color={column.nullable === false ? 'error' : 'default'}
                            variant="outlined"
                      />
                    </TableCell>
                        <TableCell>
                          <Typography variant="body2" color={column.description ? 'text.primary' : 'text.secondary'}>
                            {column.description || 'NIL'}
                          </Typography>
                        </TableCell>
                    <TableCell>
                          {column.piiFound ? (
                            <Chip 
                              label={column.piiType || 'PII'} 
                              size="small" 
                              color="error"
                              variant="filled"
                              onClick={() => handleTogglePii(column.name)}
                              sx={{ cursor: 'pointer', '&:hover': { opacity: 0.8 } }}
                            />
                          ) : (
                            <Chip 
                              label="No" 
                              size="small" 
                              color="success"
                              variant="filled"
                              onClick={() => handleTogglePii(column.name)}
                              sx={{ cursor: 'pointer', '&:hover': { opacity: 0.8 } }}
                            />
                          )}
                    </TableCell>
                    <TableCell>
                          <Box sx={{ display: 'flex', gap: 0.5, flexWrap: 'wrap', alignItems: 'center' }}>
                            {column.tags && column.tags.length > 0 ? (
                              <>
                                {column.tags.map((tag, tagIndex) => (
                                    <Chip 
                                    key={tagIndex}
                                      label={tag} 
                                      size="small"
                                      onDelete={() => handleRemoveTag(column.name, tag)}
                                      deleteIcon={<Delete fontSize="small" />}
                                      sx={{
                                        backgroundColor: '#e3f2fd',
                                        color: '#1565c0',
                                        border: '1px solid #90caf9',
                                        fontWeight: 600,
                                        fontSize: '0.75rem',
                                        '&:hover': {
                                          backgroundColor: '#bbdefb',
                                        },
                                        '& .MuiChip-deleteIcon': {
                                          color: '#1565c0',
                                          '&:hover': {
                                            color: '#d32f2f',
                                          }
                                        }
                                      }}
                                      variant="filled"
                                    />
                                ))}
                                <IconButton
                                  size="small"
                                  onClick={() => handleAddColumnTag(column.name)}
                                  sx={{
                                    ml: 0.5,
                                    color: '#1976d2',
                                    '&:hover': {
                                      backgroundColor: 'rgba(25, 118, 210, 0.04)',
                                    }
                                  }}
                                >
                                  <Add fontSize="small" />
                                </IconButton>
                              </>
                            ) : (
                              <>
                              <Typography 
                                variant="body2" 
                                sx={{ 
                                  color: '#666', 
                                  fontStyle: 'italic',
                                  fontSize: '0.75rem'
                                }}
                              >
                                NIL
                              </Typography>
                            <IconButton
                          size="small"
                              onClick={() => handleAddColumnTag(column.name)}
                              sx={{
                                    ml: 0.5,
                                color: '#1976d2',
                                '&:hover': {
                                  backgroundColor: 'rgba(25, 118, 210, 0.04)',
                                }
                              }}
                            >
                              <Add fontSize="small" />
                            </IconButton>
                              </>
                            )}
                      </Box>
                    </TableCell>
                  </TableRow>
                ))}
              </TableBody>
            </Table>
          </TableContainer>
            </Box>
          )}

          {}
          <Dialog open={columnTagDialogOpen} onClose={handleCloseDialogs} maxWidth="sm" fullWidth>
            <DialogTitle>
              {selectedColumnForTag && selectedColumnForTag !== '' ? `Add Tag to Column: ${selectedColumnForTag}` : 'Add Tag to Column'}
            </DialogTitle>
            <DialogContent>
              <Box sx={{ mt: 2, display: 'flex', flexDirection: 'column', gap: 2 }}>
                <FormControl fullWidth>
                  <InputLabel>Select Column</InputLabel>
                  <Select
                    value={selectedColumnForTag}
                    onChange={(e) => setSelectedColumnForTag(e.target.value)}
                    label="Select Column"
                  >
                    {tableData?.columns.map((column) => (
                      <MenuItem key={column.name} value={column.name}>
                        {column.name}
                      </MenuItem>
                    ))}
                  </Select>
                </FormControl>
                <Autocomplete
                  freeSolo
                  options={[]}
                  value={newTag}
                  onInputChange={(e, newValue) => setNewTag(newValue)}
                  renderInput={(params) => (
                    <TextField
                      {...params}
                      label="Tag Name"
                      placeholder="Enter tag name (e.g., PII, SENSITIVE, REQUIRED)"
                      variant="outlined"
                      onKeyPress={(e) => {
                        if (e.key === 'Enter') {
                          handleAddTag();
                        }
                      }}
                    />
                  )}
                />
              </Box>
            </DialogContent>
            <DialogActions>
              <Button onClick={handleCloseDialogs}>Cancel</Button>
              <Button 
                onClick={handleAddTag} 
                variant="contained"
                disabled={!newTag.trim() || !selectedColumnForTag}
              >
                Add Tag
              </Button>
            </DialogActions>
          </Dialog>

          {}
          <Dialog open={tableTagDialogOpen} onClose={handleCloseDialogs} maxWidth="sm" fullWidth>
            <DialogTitle>Add Tag to Table</DialogTitle>
            <DialogContent>
              <Box sx={{ mt: 2 }}>
                <Autocomplete
                  freeSolo
                  options={[]}
                  value={newTag}
                  onInputChange={(e, newValue) => setNewTag(newValue)}
                  renderInput={(params) => (
                    <TextField
                      {...params}
                      label="Tag Name"
                      placeholder="Enter tag name (e.g., PRODUCTION, ANALYTICS, COMPLIANCE)"
                      variant="outlined"
                      onKeyPress={(e) => {
                        if (e.key === 'Enter') {
                          handleAddTag();
                        }
                      }}
                    />
                  )}
                />
                <Typography variant="body2" sx={{ mt: 1, color: '#666' }}>
                  This tag will be added to the asset.
                </Typography>
              </Box>
            </DialogContent>
            <DialogActions>
              <Button onClick={handleCloseDialogs}>Cancel</Button>
              <Button 
                onClick={handleAddTag} 
                variant="contained"
                disabled={!newTag.trim()}
              >
                Add Table Tag
              </Button>
            </DialogActions>
          </Dialog>

          {}
          <Dialog open={recommendedTagsDialogOpen} onClose={handleCloseDialogs} maxWidth="md" fullWidth>
            <DialogTitle>
              <Box sx={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center' }}>
                <Box sx={{ display: 'flex', alignItems: 'center', gap: 1 }}>
                  <Security sx={{ color: '#d32f2f', fontSize: 28 }} />
                  <Typography variant="h6">Recommended PII Security Tags</Typography>
                </Box>
                <Button
                  variant="contained"
                  color="error"
                  startIcon={<AutoAwesome />}
                  onClick={handleApplyAllRecommendedTags}
                  size="small"
                  sx={{
                    textTransform: 'none',
                    px: 2,
                  }}
                >
                  Apply All
                </Button>
              </Box>
            </DialogTitle>
            <DialogContent>
              <Box sx={{ mt: 2 }}>
                <Alert severity="warning" sx={{ mb: 2 }}>
                  <Typography variant="body2">
                    These security tags are recommended for columns containing Personally Identifiable Information (PII) with sensitivity levels (1-5). 
                    Tags include classification levels and encryption requirements for proper data governance and compliance.
                  </Typography>
                </Alert>
                
                <Box sx={{ mb: 2, p: 2, backgroundColor: '#f5f5f5', borderRadius: 1 }}>
                  <Typography variant="body2" sx={{ fontWeight: 600, mb: 1 }}>
                    PII Sensitivity Levels:
                  </Typography>
                  <Box sx={{ display: 'flex', gap: 1, flexWrap: 'wrap' }}>
                    <Chip label="Level 5: Critical (SSN, Credit Cards, Passwords)" size="small" sx={{ backgroundColor: '#ffebee', color: '#c62828', fontWeight: 600 }} />
                    <Chip label="Level 4: High (DOB, Physical Address)" size="small" sx={{ backgroundColor: '#fff3e0', color: '#e65100', fontWeight: 600 }} />
                    <Chip label="Level 3: Medium-High (Email, Phone)" size="small" sx={{ backgroundColor: '#fff9c4', color: '#f57c00', fontWeight: 600 }} />
                    <Chip label="Level 2: Medium (Names, User IDs)" size="small" sx={{ backgroundColor: '#e3f2fd', color: '#1565c0', fontWeight: 600 }} />
                  </Box>
                </Box>
                
                <Typography variant="body2" sx={{ mb: 2, color: '#666', fontWeight: 600 }}>
                  Click on tags to apply them to columns. Click "Apply All" to apply all recommended tags at once.
                </Typography>
                
                <Box sx={{ maxHeight: '400px', overflowY: 'auto' }}>
                  {Object.entries(recommendedTags).map(([columnName, rec]) => {
                    const tags = rec.tags || [];
                    const sensitivityLevel = rec.sensitivityLevel || 0;
                    if (tags.length > 0) {
                      return (
                        <Box key={columnName} sx={{ mb: 3, pb: 2, borderBottom: '1px solid #e0e0e0' }}>
                          <Box sx={{ display: 'flex', alignItems: 'center', mb: 1, gap: 1 }}>
                            <Typography variant="subtitle2" sx={{ fontWeight: 600, color: '#d32f2f' }}>
                              🔒 PII Column: {columnName}
                            </Typography>
                            <Chip 
                              label={`Level ${sensitivityLevel}`} 
                              size="small" 
                              sx={{ 
                                backgroundColor: sensitivityLevel >= 5 ? '#ffebee' : 
                                                sensitivityLevel >= 4 ? '#fff3e0' :
                                                sensitivityLevel >= 3 ? '#fff9c4' : '#e3f2fd',
                                color: sensitivityLevel >= 5 ? '#c62828' :
                                       sensitivityLevel >= 4 ? '#e65100' :
                                       sensitivityLevel >= 3 ? '#f57c00' : '#1565c0',
                                fontWeight: 700,
                                fontSize: '0.7rem'
                              }}
                            />
                          </Box>
                          <Box sx={{ display: 'flex', gap: 0.5, flexWrap: 'wrap' }}>
                            {tags.map((tag, idx) => (
                              <Chip
                                key={idx}
                                label={tag}
                                size="small"
                                onClick={() => handleApplyRecommendedTag(columnName, tag)}
                                sx={{
                                  backgroundColor: '#fff3e0',
                                  color: '#f57c00',
                                  border: '1px solid #ffb74d',
                                  fontWeight: 600,
                                  fontSize: '0.75rem',
                                  cursor: 'pointer',
                                  '&:hover': {
                                    backgroundColor: '#ffe0b2',
                                    borderColor: '#ff9800',
                                  },
                                }}
                              />
                            ))}
                          </Box>
                        </Box>
                      );
                    }
                    return null;
                  })}
                  {Object.entries(recommendedTags).length === 0 && (
                    <Typography variant="body2" sx={{ color: '#999', fontStyle: 'italic', textAlign: 'center', py: 4 }}>
                      No PII columns detected. Recommendations are only shown for columns with potentially sensitive data.
                    </Typography>
                  )}
                </Box>
              </Box>
            </DialogContent>
            <DialogActions>
              <Button onClick={handleCloseDialogs}>Close</Button>
            </DialogActions>
          </Dialog>

          {}
          <Dialog open={piiDialogOpen} onClose={handleCloseDialogs} maxWidth="sm" fullWidth>
            <DialogTitle>
              Change PII Status for Column: {selectedColumnForPii?.name}
            </DialogTitle>
            <DialogContent>
              <Box sx={{ mt: 2, display: 'flex', flexDirection: 'column', gap: 2 }}>
                <FormControl component="fieldset">
                  <FormLabel component="legend">PII Status</FormLabel>
                  <RadioGroup
                    value={selectedColumnForPii?.piiFound ? 'yes' : 'no'}
                    onChange={(e) => {
                      const isPii = e.target.value === 'yes';
                      setSelectedColumnForPii({
                        ...selectedColumnForPii,
                        piiFound: isPii,
                        piiType: isPii ? (selectedColumnForPii?.piiType || 'PII') : ''
                      });
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
                {selectedColumnForPii?.piiFound && (
                  <TextField
                    fullWidth
                    label="PII Type (e.g., Email, SSN, Phone)"
                    placeholder="Enter PII type"
                    value={selectedColumnForPii?.piiType || ''}
                    onChange={(e) => {
                      setSelectedColumnForPii({
                        ...selectedColumnForPii,
                        piiType: e.target.value
                      });
                    }}
                    variant="outlined"
                    helperText="Describe the type of PII this column contains"
                  />
                )}
              </Box>
            </DialogContent>
            <DialogActions>
              <Button onClick={handleCloseDialogs}>Cancel</Button>
              <Button 
                onClick={handleSavePiiChange} 
                variant="contained"
              >
                Save Changes
              </Button>
            </DialogActions>
          </Dialog>

          {}
          <Dialog open={sqlDialogOpen} onClose={() => setSqlDialogOpen(false)} maxWidth="md" fullWidth>
            <DialogTitle>
              Operation Successful
            </DialogTitle>
            <DialogContent>
              <Box sx={{ mt: 2 }}>
                <Alert severity="success" sx={{ mb: 3 }}>
                  <Typography variant="h6" sx={{ fontWeight: 600, mb: 2 }}>
                    Operation Successful
                  </Typography>
                  
                  {}
                  {billingInfo.columnTagsInfo && billingInfo.columnTagsInfo.length > 0 && (
                    <Box sx={{ mb: 3 }}>
                      <Typography variant="body2" sx={{ fontWeight: 600, mb: 2, fontSize: '1rem' }}>
                        Tags Applied to Columns:
                              </Typography>
                      {billingInfo.columnTagsInfo.map((info, idx) => (
                        <Box key={idx} sx={{ mb: 1.5, pl: 2, display: 'flex', alignItems: 'center', gap: 1, flexWrap: 'wrap' }}>
                                  <Typography 
                                    variant="body2" 
                                    sx={{ 
                              fontWeight: 600, 
                              color: '#1976d2',
                              fontSize: '0.95rem',
                              textTransform: 'none'
                            }}
                          >
                            {info.columnName} :
                                  </Typography>
                          <Box sx={{ display: 'flex', gap: 1, flexWrap: 'wrap' }}>
                            {info.tags.map((tag, tagIdx) => (
                              <Chip 
                                key={tagIdx}
                                label={tag} 
                                size="medium"
                                sx={{ 
                                  backgroundColor: '#e3f2fd', 
                                  color: '#1565c0',
                                  fontWeight: 600,
                                  fontSize: '0.875rem',
                                  height: '32px',
                                  '& .MuiChip-label': {
                                    padding: '0 12px'
                                  }
                                }}
                              />
                                ))}
                              </Box>
                            </Box>
                      ))}
                    </Box>
                  )}

                  {}
                  {tableData && tableData.tableTags && tableData.tableTags.length > 0 && (
                    <Box sx={{ mb: 3 }}>
                        <Typography variant="body2" sx={{ fontWeight: 600, mb: 1 }}>
                        Table Tags:
                        </Typography>
                      <Box sx={{ display: 'flex', gap: 0.5, flexWrap: 'wrap', pl: 2 }}>
                        {tableData.tableTags.map((tag, idx) => (
                          <Chip 
                            key={idx}
                            label={tag} 
                            size="small" 
                            sx={{ 
                              backgroundColor: '#e3f2fd', 
                              color: '#1565c0',
                              fontWeight: 600
                            }}
                          />
                        ))}
                        </Box>
                        </Box>
                  )}

                  {}
                  {billingInfo.sqlCommands && billingInfo.sqlCommands.length > 0 && (
                    <Box sx={{ mb: 2 }}>
                      <Typography variant="body2" sx={{ fontWeight: 600, color: '#2e7d32' }}>
                        Views have been created for operational and analytical users
                        </Typography>
                  </Box>
                )}
                </Alert>

                {}
                {billingInfo.sqlCommands && Array.isArray(billingInfo.sqlCommands) && billingInfo.sqlCommands.length > 0 && (
                  <Box sx={{ mt: 3 }}>
                    <Typography variant="h6" sx={{ fontWeight: 600, mb: 2 }}>
                      📋 SQL Commands:
                    </Typography>
                    <Typography variant="body2" sx={{ mb: 2, color: 'text.secondary' }}>
                      Copy and execute these SQL commands to create the masked views:
                    </Typography>
                    <Paper sx={{ p: 2, backgroundColor: '#f5f5f5', maxHeight: '400px', overflow: 'auto' }}>
                      {billingInfo.sqlCommands.map((command, index) => {
                        if (!command) return null;
                        const isMaskedView = command.includes && command.includes('CREATE OR REPLACE VIEW') && command.includes('_masked');
                        const viewType = command.includes && command.includes('_analytical') ? 'Analytical' : command.includes && command.includes('_operational') ? 'Operational' : '';
                        return (
                        <Box key={index} sx={{ mb: 2 }}>
                            {isMaskedView && viewType && (
                              <Typography variant="body2" sx={{ fontWeight: 600, mb: 1, fontSize: '0.875rem', color: 'primary.main' }}>
                                {viewType} View SQL:
                              </Typography>
                            )}
                          <Typography variant="body2" component="pre" sx={{ 
                            fontFamily: 'monospace', 
                            fontSize: '0.875rem',
                            whiteSpace: 'pre-wrap',
                            wordBreak: 'break-word',
                            backgroundColor: 'white',
                            p: 2,
                            borderRadius: 1,
                            border: '1px solid #e0e0e0'
                          }}>
                            {command}
                          </Typography>
                        </Box>
                        );
                      })}
                    </Paper>
                  </Box>
                )}
              </Box>
            </DialogContent>
            <DialogActions>
              <Button onClick={() => setSqlDialogOpen(false)} variant="contained">
                Close
              </Button>
            </DialogActions>
          </Dialog>

          {}
          <Snackbar
            open={snackbarOpen}
            autoHideDuration={3000}
            onClose={() => setSnackbarOpen(false)}
            message={snackbarMessage}
          />
        </CardContent>
      </Card>
    </Box>
  );
};

export default MarketplacePage;
