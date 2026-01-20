import React, { useState } from 'react';
import {
  Dialog,
  DialogTitle,
  DialogContent,
  DialogActions,
  Button,
  TextField,
  Box,
  Typography,
  Alert,
  Autocomplete,
  IconButton,
  Paper,
  Table,
  TableBody,
  TableCell,
  TableContainer,
  TableHead,
  TableRow,
} from '@mui/material';
import {
  Close,
  Add,
  Delete,
  Description,
} from '@mui/icons-material';

const ManualLineageDialog = ({ open, onClose, onSuccess, assets: assetsProp = null }) => {
  const [sourceAsset, setSourceAsset] = useState(null);
  const [targetAsset, setTargetAsset] = useState(null);
  const [relationship, setRelationship] = useState('manual');
  const [notes, setNotes] = useState('');
  const [columnMappings, setColumnMappings] = useState([]);
  const [sourceColumn, setSourceColumn] = useState('');
  const [targetColumn, setTargetColumn] = useState('');
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState(null);
  const [success, setSuccess] = useState(null);
  const [assets, setAssets] = useState([]);

  const fetchAllAssets = async () => {
    try {
      const API_BASE_URL = import.meta.env.VITE_API_BASE_URL;

      // Prefer paginated endpoint; fall back to old array response.
      const firstResp = await fetch(`${API_BASE_URL}/api/assets?page=1&per_page=500`);
      if (!firstResp.ok) {
        console.error('Failed to fetch first page of assets:', firstResp.status, firstResp.statusText);
        return [];
      }

      let firstData;
      try {
        firstData = await firstResp.json();
      } catch (jsonError) {
        console.error('Failed to parse JSON response:', jsonError);
        return [];
      }

      if (Array.isArray(firstData)) {
        return firstData;
      }

      if (!firstData || !Array.isArray(firstData.assets) || !firstData.pagination) {
        console.warn('Invalid response format from assets API');
        return [];
      }

      const all = [...firstData.assets];
      const totalPages = Number(firstData.pagination.total_pages || 1);
      const cappedTotalPages = Math.min(totalPages, 200);

      for (let p = 2; p <= cappedTotalPages; p++) {
        try {
          const resp = await fetch(`${API_BASE_URL}/api/assets?page=${p}&per_page=500`);
          if (!resp.ok) {
            console.warn(`Failed to fetch page ${p}:`, resp.status, resp.statusText);
            break;
          }
          let data;
          try {
            data = await resp.json();
          } catch (jsonError) {
            console.error(`Failed to parse JSON for page ${p}:`, jsonError);
            break;
          }
          if (data && Array.isArray(data.assets)) {
            all.push(...data.assets);
          } else {
            break;
          }
        } catch (pageError) {
          console.error(`Error fetching page ${p}:`, pageError);
          break;
        }
      }

      return all;
    } catch (error) {
      console.error('Error in fetchAllAssets:', error);
      return [];
    }
  };

  React.useEffect(() => {
    if (!open) return;

    // If parent provides assets (already pagination-safe), use them.
    if (Array.isArray(assetsProp) && assetsProp.length >= 0) {
      setAssets(assetsProp);
      return;
    }

    // Otherwise fetch all assets (pagination-safe)
    (async () => {
      try {
        const allAssets = await fetchAllAssets();
        setAssets(allAssets);
      } catch (error) {
        console.error('Error fetching assets:', error);
      }
    })();
  }, [open, assetsProp]);

  // NOTE: legacy fetchAssets removed; pagination-safe fetchAllAssets is used instead.

  const handleAddColumnMapping = () => {
    if (sourceColumn && targetColumn) {
      setColumnMappings([...columnMappings, {
        source_column: sourceColumn,
        target_column: targetColumn,
        relationship_type: 'direct_match'
      }]);
      setSourceColumn('');
      setTargetColumn('');
    }
  };

  const handleRemoveColumnMapping = (index) => {
    setColumnMappings(columnMappings.filter((_, i) => i !== index));
  };

  const handleSubmit = async () => {
    if (!sourceAsset || !targetAsset) {
      setError('Please select both source and target assets');
      return;
    }

    setLoading(true);
    setError(null);
    setSuccess(null);

    try {
      const API_BASE_URL = import.meta.env.VITE_API_BASE_URL;

      // If user typed a mapping but forgot to click "Add", include it automatically.
      const trimmedSource = (sourceColumn || '').trim();
      const trimmedTarget = (targetColumn || '').trim();
      const finalColumnMappings = [...columnMappings];
      if (trimmedSource && trimmedTarget) {
        finalColumnMappings.push({
          source_column: trimmedSource,
          target_column: trimmedTarget,
          relationship_type: 'direct_match',
        });
      }

      // IMPORTANT: Use backend's canonical URN generation so manual lineage always shows up.
      // Frontend-side URN guessing can drift (esp. for Azure folder_path schema extraction).
      const fetchDatasetUrn = async (assetId) => {
        const resp = await fetch(`${API_BASE_URL}/api/lineage/asset/${assetId}/dataset-urn`);
        if (!resp.ok) {
          let msg = `Failed to get dataset URN for asset ${assetId}`;
          try {
            const data = await resp.json();
            msg = data.error || msg;
          } catch {
            msg = `HTTP ${resp.status}: ${resp.statusText || msg}`;
          }
          throw new Error(msg);
        }
        const data = await resp.json();
        return data.dataset_urn;
      };

      const [sourceUrn, targetUrn] = await Promise.all([
        fetchDatasetUrn(sourceAsset.id),
        fetchDatasetUrn(targetAsset.id),
      ]);
      
      const response = await fetch(`${API_BASE_URL}/api/lineage/manual/table-level`, {
        method: 'POST',
        headers: {
          'Content-Type': 'application/json',
        },
        body: JSON.stringify({
          source_tables: [sourceUrn],
          target_tables: [targetUrn],
          process_name: notes || `Manual lineage: ${sourceAsset.name} -> ${targetAsset.name}`,
          relationship_type: relationship,
          column_mappings: finalColumnMappings.length > 0 ? finalColumnMappings.map(m => ({
            source_column: m.source_column,
            target_column: m.target_column,
            transformation_type: 'pass_through'
          })) : []
        }),
      });

      if (response.ok) {
        await response.json();
        setSuccess('Manual lineage relation created successfully!');
        // Clear any typed-but-not-added mapping fields once submitted
        setSourceColumn('');
        setTargetColumn('');
        setTimeout(() => {
          onSuccess && onSuccess();
          handleClose();
        }, 1500);
      } else {
        let errorMessage = 'Failed to create lineage relationship';
        try {
          const errorData = await response.json();
          errorMessage = errorData.error || errorData.message || errorMessage;
        } catch (jsonError) {
          // If response is not JSON, use status text
          errorMessage = `HTTP ${response.status}: ${response.statusText || errorMessage}`;
        }
        throw new Error(errorMessage);
      }
    } catch (err) {
      console.error('Error submitting lineage proposal:', err);
      setError(err.message || 'Failed to submit lineage proposal. Please check your connection and try again.');
    } finally {
      setLoading(false);
    }
  };

  const handleClose = () => {
    setSourceAsset(null);
    setTargetAsset(null);
    setRelationship('manual');
    setNotes('');
    setColumnMappings([]);
    setSourceColumn('');
    setTargetColumn('');
    setError(null);
    setSuccess(null);
    onClose();
  };

  const getAssetLabel = (asset) => {
    if (!asset) return '';
    return `${asset.name} (${asset.type}) - ${asset.catalog || 'Unknown'}`;
  };

  return (
    <Dialog open={open} onClose={handleClose} maxWidth="md" fullWidth>
      <DialogTitle>
        <Box sx={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center' }}>
          <Box sx={{ display: 'flex', alignItems: 'center', gap: 1 }}>
            <Description />
            <Typography variant="h6">Manual Lineage Entry</Typography>
          </Box>
          <IconButton onClick={handleClose} size="small">
            <Close />
          </IconButton>
        </Box>
      </DialogTitle>
      
      <DialogContent dividers>
        <Box sx={{ display: 'flex', flexDirection: 'column', gap: 3 }}>
            <Autocomplete
              options={assets}
              getOptionLabel={getAssetLabel}
              value={sourceAsset}
              onChange={(e, value) => setSourceAsset(value)}
              renderInput={(params) => (
                <TextField {...params} label="Source Asset" placeholder="Select source table/view" />
              )}
              renderOption={(props, option) => (
                <li {...props}>
                  <Box>
                    <Typography variant="body2" sx={{ fontWeight: 600 }}>
                      {option.name}
                    </Typography>
                    <Typography variant="caption" color="text.secondary">
                      {option.type} • {option.catalog || 'Unknown'}
                    </Typography>
                  </Box>
                </li>
              )}
            />

            <Autocomplete
              options={assets}
              getOptionLabel={getAssetLabel}
              value={targetAsset}
              onChange={(e, value) => setTargetAsset(value)}
              renderInput={(params) => (
                <TextField {...params} label="Target Asset" placeholder="Select target table/view" />
              )}
              renderOption={(props, option) => (
                <li {...props}>
                  <Box>
                    <Typography variant="body2" sx={{ fontWeight: 600 }}>
                      {option.name}
                    </Typography>
                    <Typography variant="caption" color="text.secondary">
                      {option.type} • {option.catalog || 'Unknown'}
                    </Typography>
                  </Box>
                </li>
              )}
            />

            <TextField
              select
              label="Relationship Type"
              value={relationship}
              onChange={(e) => setRelationship(e.target.value)}
              SelectProps={{ native: true }}
            >
              <option value="manual">Manual</option>
              <option value="etl_pipeline">ETL Pipeline</option>
              <option value="elt_pipeline">ELT Pipeline</option>
              <option value="feeds_into">Feeds Into</option>
              <option value="derives_from">Derives From</option>
              <option value="schema_level">Schema Level</option>
            </TextField>

            <Box>
              <Typography variant="subtitle2" sx={{ mb: 1 }}>
                Column Mappings (Optional)
              </Typography>
              <Box sx={{ display: 'flex', gap: 1, mb: 2 }}>
                <TextField
                  size="small"
                  placeholder="Source Column"
                  value={sourceColumn}
                  onChange={(e) => setSourceColumn(e.target.value)}
                  sx={{ flex: 1 }}
                />
                <TextField
                  size="small"
                  placeholder="Target Column"
                  value={targetColumn}
                  onChange={(e) => setTargetColumn(e.target.value)}
                  sx={{ flex: 1 }}
                />
                <Button
                  size="small"
                  variant="outlined"
                  startIcon={<Add />}
                  onClick={handleAddColumnMapping}
                  disabled={!sourceColumn || !targetColumn}
                >
                  Add
                </Button>
              </Box>
              
              {columnMappings.length > 0 && (
                <TableContainer component={Paper} variant="outlined">
                  <Table size="small">
                    <TableHead>
                      <TableRow>
                        <TableCell>Source Column</TableCell>
                        <TableCell>Target Column</TableCell>
                        <TableCell width={80}>Actions</TableCell>
                      </TableRow>
                    </TableHead>
                    <TableBody>
                      {columnMappings.map((mapping, index) => (
                        <TableRow key={index}>
                          <TableCell>{mapping.source_column}</TableCell>
                          <TableCell>{mapping.target_column}</TableCell>
                          <TableCell>
                            <IconButton
                              size="small"
                              onClick={() => handleRemoveColumnMapping(index)}
                            >
                              <Delete fontSize="small" />
                            </IconButton>
                          </TableCell>
                        </TableRow>
                      ))}
                    </TableBody>
                  </Table>
                </TableContainer>
              )}
            </Box>

            <TextField
              multiline
              rows={3}
              label="Notes (Optional)"
              value={notes}
              onChange={(e) => setNotes(e.target.value)}
              placeholder="Add any additional notes about this lineage relationship..."
            />
        </Box>

        {error && (
          <Alert severity="error" sx={{ mt: 2 }}>
            {error}
          </Alert>
        )}

        {success && (
          <Alert severity="success" sx={{ mt: 2 }}>
            {success}
          </Alert>
        )}
      </DialogContent>

      <DialogActions>
        <Button onClick={handleClose}>Cancel</Button>
        <Button
          variant="contained"
          onClick={handleSubmit}
          disabled={loading || !sourceAsset || !targetAsset}
        >
          {loading ? 'Creating...' : 'Create Lineage'}
        </Button>
      </DialogActions>
    </Dialog>
  );
};

export default ManualLineageDialog;

