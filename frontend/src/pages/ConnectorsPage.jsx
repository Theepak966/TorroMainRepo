import React, { useState, useEffect, useRef } from 'react';
import { createPortal } from 'react-dom';
import {
  Box,
  Typography,
  Card,
  CardContent,
  Grid,
  Button,
  Avatar,
  Chip,
  Divider,
  Dialog,
  DialogContent,
  IconButton,
  Stepper,
  Step,
  StepLabel,
  TextField,
  FormControl,
  InputLabel,
  Select,
  MenuItem,
  RadioGroup,
  FormControlLabel,
  Radio,
  Alert,
  CircularProgress,
  Tooltip,
} from '@mui/material';
import {
  Add,
  Refresh,
  CloudSync,
  CheckCircle,
  CloudQueue,
  Storage,
  Cloud,
  Close,
  ArrowBack,
  ArrowForward,
  Delete,
  Visibility,
  Replay,
} from '@mui/icons-material';




// Removed HARDCODED_AZURE_CREDENTIALS - now using manual input from form

const ConnectorsPage = () => {
  const [myConnections, setMyConnections] = useState([]);
  const [loading, setLoading] = useState(false);
  const [dialogOpen, setDialogOpen] = useState(false);
  const [deleteDialogOpen, setDeleteDialogOpen] = useState(false);
  const [connectionToDelete, setConnectionToDelete] = useState(null);
  
  
  const [wizardOpen, setWizardOpen] = useState(false);
  const [selectedConnector, setSelectedConnector] = useState(null);
  const [activeStep, setActiveStep] = useState(0);
  const [connectionType, setConnectionType] = useState('');
  const [config, setConfig] = useState({});
  const [testing, setTesting] = useState(false);
  const [testResult, setTestResult] = useState(null);
  const [discoveryProgress, setDiscoveryProgress] = useState([]);
  const [saveProgress, setSaveProgress] = useState({ active: false, percent: 0, message: '' });
  const saveProgressIntervalRef = useRef(null);
  const lastSaveProgressBatchRef = useRef(null);
  const oracleDiscoveryIntervalRef = useRef(null);
  const lastOracleProgressMessageRef = useRef('');
  const [logoErrors, setLogoErrors] = useState(new Set());
  


  useEffect(() => {
    fetchMyConnections();

    
    return () => {
      if (saveProgressIntervalRef.current) {
        clearInterval(saveProgressIntervalRef.current);
        saveProgressIntervalRef.current = null;
      }
      if (oracleDiscoveryIntervalRef.current) {
        clearInterval(oracleDiscoveryIntervalRef.current);
        oracleDiscoveryIntervalRef.current = null;
      }
    };
  }, []); 

  const stopSaveProgressPolling = (resetUi = true) => {
    if (saveProgressIntervalRef.current) {
      clearInterval(saveProgressIntervalRef.current);
      saveProgressIntervalRef.current = null;
    }
    lastSaveProgressBatchRef.current = null;
    if (resetUi) {
      setSaveProgress({ active: false, percent: 0, message: '' });
    }
  };

  const startSaveProgressPolling = (connectionId) => {
    stopSaveProgressPolling(true);
    setSaveProgress({ active: true, percent: 0, message: 'Saving assets to database...' });

    const API_BASE_URL = import.meta.env.VITE_API_BASE_URL;
    saveProgressIntervalRef.current = setInterval(async () => {
      try {
        const res = await fetch(`${API_BASE_URL}/api/connections/${connectionId}/discover-progress`);
        if (!res.ok) return;
        const p = await res.json();
        if (!p || !p.status) return;

        if (p.status === 'idle') return;

        const percent = typeof p.percent === 'number' ? p.percent : 0;
        const msg = p.message || 'Saving assets to database...';
        setSaveProgress({ active: true, percent, message: msg });

        // Append batch commits to the log (only when batch changes)
        if (typeof p.batch_num === 'number' && typeof p.total_batches === 'number') {
          const key = `${p.batch_num}/${p.total_batches}`;
          if (lastSaveProgressBatchRef.current !== key && p.batch_num > 0) {
            lastSaveProgressBatchRef.current = key;
            setDiscoveryProgress(prev => [...prev, `✅ Committed batch ${p.batch_num}/${p.total_batches} (${percent}%)`]);
          }
        }

        if (p.status === 'done' || p.status === 'error') {
          // Stop polling once backend reports completion/failure, but keep the last UI state
          stopSaveProgressPolling(false);
        }
      } catch (e) {
        // Ignore polling errors; the main /discover request will surface failures
      }
    }, 1000);
  };

  const stopOracleDiscoveryPolling = (resetUi = true) => {
    if (oracleDiscoveryIntervalRef.current) {
      clearInterval(oracleDiscoveryIntervalRef.current);
      oracleDiscoveryIntervalRef.current = null;
    }
    lastOracleProgressMessageRef.current = '';
  };

  const startOracleDiscoveryPolling = (connectionId) => {
    stopOracleDiscoveryPolling(true);
    
    const API_BASE_URL = import.meta.env.VITE_API_BASE_URL;
    oracleDiscoveryIntervalRef.current = setInterval(async () => {
      try {
        const res = await fetch(`${API_BASE_URL}/api/connections/${connectionId}/discover-progress`);
        if (!res.ok) return;
        const p = await res.json();
        if (!p || !p.status) return;

        if (p.status === 'idle') return;

        const msg = p.message || '';
        
        // Only add new messages (avoid duplicates)
        if (msg && msg !== lastOracleProgressMessageRef.current) {
          lastOracleProgressMessageRef.current = msg;
          setDiscoveryProgress(prev => {
            // Avoid duplicate messages
            if (prev.length === 0 || prev[prev.length - 1] !== msg) {
              return [...prev, msg];
            }
            return prev;
          });
        }

        // Update test result when discovery completes
        if (p.status === 'done') {
          const created = p.created_count || 0;
          const updated = p.updated_count || 0;
          const skipped = p.skipped_count || 0;
          const total = created + updated;
          
          // Clean message - remove emoji and lineage
          let cleanMessage = p.message || '';
          if (cleanMessage) {
            cleanMessage = cleanMessage.replace(/✅\s*/g, '').replace(/\.\s*Extracted \d+ lineage relationships\./g, '.');
          }
          
          const finalMessage = cleanMessage || (total > 0 
            ? `Discovery complete! Found ${total} assets (${created} new, ${updated} updated, ${skipped} skipped).`
            : skipped > 0
              ? `Discovery complete! All ${skipped} assets already exist (no new assets).`
              : `Discovery complete! No assets found.`);
          
          setTestResult({
            success: true,
            message: finalMessage,
            connectionId: connectionId,
            discoveredAssets: total,
            totalContainers: undefined, // Oracle doesn't use containers
          });
          setTesting(false);  // Stop testing when discovery completes
          stopOracleDiscoveryPolling(false);
        } else if (p.status === 'error') {
          setTestResult({
            success: false,
            message: p.message || 'Discovery failed',
            connectionId: connectionId,
          });
          setTesting(false);  // Stop testing on error
          stopOracleDiscoveryPolling(false);
        }
      } catch (e) {
        // Ignore polling errors
      }
    }, 500); // Poll every 500ms for Oracle discovery
  };

  const fetchMyConnections = async () => {
      setLoading(true);
    try {
      const API_BASE_URL = import.meta.env.VITE_API_BASE_URL;
      const response = await fetch(`${API_BASE_URL}/api/connections`);
      if (response.ok) {
        const connections = await response.json();
        
        const formattedConnections = connections.map(conn => ({
          id: conn.id,
          name: conn.name,
          type: `${conn.connector_type} - ${conn.connection_type || 'N/A'}`,
          status: conn.status || 'active',
          last_run: conn.created_at,
          assets_count: 0, 
        }));
        setMyConnections(formattedConnections);
      } else {
        setMyConnections([]);
      }
    } catch (error) {
      console.error('Error fetching connections:', error);
      setMyConnections([]);
    } finally {
      setLoading(false);
    }
  };

  const handleDeleteClick = (connection) => {
    setConnectionToDelete(connection);
    setDeleteDialogOpen(true);
  };

  const handleDeleteConfirm = async () => {
    if (!connectionToDelete) return;
    
    try {
      const API_BASE_URL = import.meta.env.VITE_API_BASE_URL;
      const connectionId = connectionToDelete.id;
      
      if (!connectionId) {
        throw new Error('Connection ID is missing');
      }
      
      const deleteUrl = `${API_BASE_URL}/api/connections/${connectionId}`;
      console.log(`FN:handleDeleteConfirm connection_id:${connectionId} url:${deleteUrl}`);
      
      const response = await fetch(deleteUrl, {
        method: 'DELETE',
        headers: {
          'Content-Type': 'application/json',
        },
      });
      
      if (response.ok) {
        const result = await response.json();
        const deletedCount = result.deleted_assets || 0;
        
        
        setMyConnections(prev => prev.filter(conn => conn.id !== connectionId));
        setDeleteDialogOpen(false);
        setConnectionToDelete(null);
        
        
        alert(`Connection deleted successfully. ${deletedCount} associated asset(s) were also removed.`);
      } else {
        const errorData = await response.json().catch(() => ({ error: 'Unknown error' }));
        throw new Error(errorData.error || `HTTP ${response.status}: Failed to delete connection`);
      }
    } catch (error) {
      console.error(`FN:handleDeleteConfirm error:${error.message || error}`);
      alert(`Failed to delete connection: ${error.message || 'Please try again.'}`);
    }
  };

  const handleDeleteCancel = () => {
    setDeleteDialogOpen(false);
    setConnectionToDelete(null);
  };

  const availableConnectors = [
    {
      id: 'azure_blob',
      name: 'Azure Data Blob Storage',
      description: 'Connect to Azure Blob Storage and Azure Data Lake Gen2',
      logo: 'https://azure.microsoft.com/svghandler/storage/',
      fallbackIcon: <Cloud />,
      color: '#0078D4',
      connectionTypes: ['Connection String', 'Service Principal'],
    },
    {
      id: 'oracle_db',
      name: 'Oracle Database',
      description: 'Connect to on-premises Oracle Database',
      logo: 'https://www.oracle.com/a/ocom/img/rh03-oracle-logo.png',
      fallbackIcon: <Storage />,
      color: '#F80000',
      connectionTypes: ['Standard Connection', 'JDBC'],
    },
  ];

  const wizardSteps = [
    'Connection Type',
    'Configuration',
    'Test Connection',
    'Summary'
  ];

  const handleConnectClick = (connector) => {
    setSelectedConnector(connector);
    setActiveStep(0);
    setConnectionType(''); 
    setConfig({});
    setTestResult(null);
    setWizardOpen(true);
  };

  const handleNext = () => {
    setActiveStep((prevActiveStep) => prevActiveStep + 1);
  };

  const handleBack = () => {
    setActiveStep((prevActiveStep) => prevActiveStep - 1);
  };

  const handleWizardClose = () => {
    setWizardOpen(false);
    setSelectedConnector(null);
    setActiveStep(0);
    setConnectionType('');
    setConfig({});
    setTestResult(null);
    setDiscoveryProgress([]);
  };

  const handleFinish = async () => {
    if (!config.name || !selectedConnector) return;
    
    try {
      const API_BASE_URL = import.meta.env.VITE_API_BASE_URL;
      
      
      // Use config values directly from form (manual input)
      const finalConfig = connectionType === 'Service Principal' ? {
        ...config,
        // Ensure use_dfs_endpoint is set for Service Principal
        use_dfs_endpoint: config.use_dfs_endpoint !== undefined ? config.use_dfs_endpoint : true,
        storage_type: config.storage_type || 'datalake',
      } : config;
      
      
      let connection = null;
      const existingConnectionsResponse = await fetch(`${API_BASE_URL}/api/connections`);
      if (existingConnectionsResponse.ok) {
        const existingConnections = await existingConnectionsResponse.json();
        connection = existingConnections.find(
          conn => conn.name === finalConfig.name && conn.connector_type === selectedConnector.id
        );
      }
      
      let response;
      if (connection) {
        
        response = await fetch(`${API_BASE_URL}/api/connections/${connection.id}`, {
          method: 'PUT',
          headers: {
            'Content-Type': 'application/json',
          },
          body: JSON.stringify({
            connection_type: connectionType,
            config: finalConfig,
            status: 'active',
          }),
        });
      } else {
        
        response = await fetch(`${API_BASE_URL}/api/connections`, {
          method: 'POST',
          headers: {
            'Content-Type': 'application/json',
          },
          body: JSON.stringify({
            name: finalConfig.name,
            connector_type: selectedConnector.id,
            connection_type: connectionType,
            config: finalConfig,
            status: 'active',
          }),
        });
      }
      
      if (response.ok) {
        await fetchMyConnections();
        handleWizardClose();
      } else {
        let errorMessage = connection ? 'Failed to update connection' : 'Failed to create connection';
        try {
          const errorData = await response.json();
          errorMessage = errorData.error || errorMessage;
        } catch (e) {
          errorMessage = response.statusText || errorMessage;
        }
        
        if (response.status === 409) {
          alert(`Connection with name "${config.name}" already exists. Please choose a different name.`);
        } else {
          alert(`Failed to ${connection ? 'update' : 'create'} connection: ${errorMessage}`);
        }
      }
    } catch (error) {
      if (import.meta.env.DEV) {
        console.error('Error creating connection:', error);
      }
      const errorMessage = error instanceof Error ? error.message : String(error || 'Unknown error');
      alert(`Failed to create connection: ${errorMessage}`);
    }
  };

  const handleTestConnection = async () => {
    if (selectedConnector?.id === 'azure_blob') {
      
      if (!config.name) {
        setTestResult({ 
          success: false, 
          message: 'Please provide connection name',
        });
        return;
      }
    
    setTesting(true);
    setTestResult(null);
    setDiscoveryProgress([]);
    
      try {
        const API_BASE_URL = import.meta.env.VITE_API_BASE_URL;
        
        
        // Use config values directly from form (manual input)
        const finalConfig = connectionType === 'Service Principal' ? {
          ...config,
          // Ensure use_dfs_endpoint is set for Service Principal
          use_dfs_endpoint: config.use_dfs_endpoint !== undefined ? config.use_dfs_endpoint : true,
          storage_type: config.storage_type || 'datalake',
        } : config;
        
        
        setDiscoveryProgress(prev => [...prev, 'Testing Azure Blob Storage connection...']);
        const testResponse = await fetch(`${API_BASE_URL}/api/connections/test-config`, {
          method: 'POST',
          headers: {
            'Content-Type': 'application/json',
          },
          body: JSON.stringify({
            config: finalConfig,
          }),
        });
        
        
        if (!testResponse.ok) {
          const contentType = testResponse.headers.get('content-type');
          let errorMessage = `Server returned ${testResponse.status} ${testResponse.statusText}`;
          
          if (contentType && contentType.includes('application/json')) {
            try {
              const errorData = await testResponse.json();
              errorMessage = errorData.error || errorData.message || errorMessage;
            } catch (e) {
              
            }
          } else {
            
            const textResponse = await testResponse.text();
            errorMessage = `Backend error: ${testResponse.status}. Please check if the backend service is running.`;
          }
          
          setTestResult({
            success: false,
            message: errorMessage,
          });
          setTesting(false);
          return;
        }
        
        
        const contentType = testResponse.headers.get('content-type');
        if (!contentType || !contentType.includes('application/json')) {
          const textResponse = await testResponse.text();
          setTestResult({
            success: false,
            message: `Invalid response format. Expected JSON but received ${contentType || 'unknown'}. Please check if the backend service is running correctly.`,
          });
          setTesting(false);
          return;
        }
        
        
        let testData;
        try {
          testData = await testResponse.json();
        } catch (jsonError) {
          
          const textResponse = await testResponse.text();
          setTestResult({
            success: false,
            message: `Failed to parse response as JSON. The backend may be returning an error page. Please check if the backend service is running on port 8099. Error: ${jsonError.message}`,
          });
          return;
        }
        
        if (!testData.success) {
          
          setTestResult({
            success: false,
            message: testData.message || 'Connection test failed',
          });
          setTesting(false);
          return;
        }
        
        
        setDiscoveryProgress(prev => [...prev, 'Connection test successful']);
        
        
        let connection = null;
        const existingConnectionsResponse = await fetch(`${API_BASE_URL}/api/connections`);
        if (existingConnectionsResponse.ok) {
          const existingConnections = await existingConnectionsResponse.json();
          connection = existingConnections.find(
            conn => conn.name === finalConfig.name && conn.connector_type === 'azure_blob'
          );
        }
        
        if (connection) {
          
          setDiscoveryProgress(prev => [...prev, 'Updating existing connection...']);
          const updateResponse = await fetch(`${API_BASE_URL}/api/connections/${connection.id}`, {
            method: 'PUT',
            headers: {
              'Content-Type': 'application/json',
            },
            body: JSON.stringify({
              connection_type: connectionType,
              config: finalConfig,
              status: 'active',
            }),
          });
          
          if (!updateResponse.ok) {
            const errorData = await updateResponse.json();
            throw new Error(errorData.error || 'Failed to update connection');
          }
          
          connection = await updateResponse.json();
          setDiscoveryProgress(prev => [...prev, 'Connection updated successfully']);
        } else {
          
          setDiscoveryProgress(prev => [...prev, 'Saving connection...']);
          const createResponse = await fetch(`${API_BASE_URL}/api/connections`, {
            method: 'POST',
            headers: {
              'Content-Type': 'application/json',
            },
            body: JSON.stringify({
              name: finalConfig.name,
              connector_type: 'azure_blob',
              connection_type: connectionType,
              config: finalConfig,
              status: 'active',
            }),
          });
          
          if (!createResponse.ok) {
            const errorData = await createResponse.json();
            throw new Error(errorData.error || 'Failed to create connection');
          }
          
          connection = await createResponse.json();
          setDiscoveryProgress(prev => [...prev, 'Connection saved successfully']);
        }
        
        
        if (testData.success) {
          // Get containers list, but let backend stream control all progress messages
          const containersResponse = await fetch(`${API_BASE_URL}/api/connections/${connection.id}/containers`);
          const containersData = await containersResponse.json();
          
          if (containersData.containers && containersData.containers.length > 0) {
            const containerNames = containersData.containers.map(c => c.name);
            setConfig({...config, containers: containerNames});
            
            // Start streaming - backend handles: auth → containers → files (in order)
            try {
              
              const response = await fetch(`${API_BASE_URL}/api/connections/${connection.id}/discover-stream`, {
            method: 'POST',
            headers: {
              'Content-Type': 'application/json',
            },
                body: JSON.stringify({
                  containers: containerNames,
                  folder_path: config.folder_path || '',
                }),
              });
              
              if (!response.ok) {
                throw new Error(`HTTP ${response.status}: ${response.statusText}`);
              }
              
              const reader = response.body.getReader();
              const decoder = new TextDecoder();
              let buffer = '';
              
              let totalDiscovered = 0;
              let totalUpdated = 0;
              let totalSkipped = 0;
              let currentContainer = null;
              let fileCount = 0;
              let actualContainersProcessed = new Set(); // Track actual containers processed
              
              while (true) {
                const { done, value } = await reader.read();
                if (done) break;
                
                buffer += decoder.decode(value, { stream: true });
                const lines = buffer.split('\n');
                buffer = lines.pop() || '';
                
                for (const line of lines) {
                  if (line.startsWith('data: ')) {
                    try {
                      const data = JSON.parse(line.slice(6));
                      
                      if (data.type === 'progress') {
                        setDiscoveryProgress(prev => [...prev, data.message]);
                      } else if (data.type === 'container') {
                        currentContainer = data.container;
                        actualContainersProcessed.add(data.container); // Track container
                        setDiscoveryProgress(prev => [...prev, `[CONTAINER] ${data.container}`]);
                      } else if (data.type === 'file') {
                        fileCount++;
                        // Show all files for small datasets, or first 20 + every 50th for large datasets
                        const shouldShow = data.total < 100 || data.index <= 20 || data.index % 50 === 0;
                        if (shouldShow) {
                          const displayPath = data.full_path || data.file;
                          setDiscoveryProgress(prev => [...prev, `  [FILE] ${displayPath} (${data.index}/${data.total})`]);
                        }
                      } else if (data.type === 'progress' && data.percentage !== undefined) {
                        setDiscoveryProgress(prev => [...prev, data.message]);
                      } else if (data.type === 'complete') {
                        totalDiscovered = data.discovered || 0;
                        totalUpdated = data.updated || 0;
                        totalSkipped = data.skipped || 0;
                        setDiscoveryProgress(prev => [...prev, `Discovery complete! Found ${totalDiscovered} files. Saving assets to database...`]);
                        startSaveProgressPolling(connection.id);
                        
                        // Trigger actual discovery to save assets
                        fetch(`${API_BASE_URL}/api/connections/${connection.id}/discover`, {
                          method: 'POST',
                          headers: {
                            'Content-Type': 'application/json',
                          },
                          body: JSON.stringify({
                            containers: containerNames,
                            folder_path: config.folder_path || '',
                            skip_deduplication: true,  // Skip deduplication for test discoveries
                          }),
                        })
                        .then(async res => {
                          if (!res.ok) {
                            let errorMessage = `HTTP ${res.status}: ${res.statusText}`;
                            try {
                              const errorData = await res.json();
                              errorMessage = errorData.error || errorMessage;
                            } catch (e) {
                              // If response is not JSON, use status text
                            }
                            return Promise.reject(new Error(errorMessage));
                          }
                          try {
                            return await res.json();
                          } catch (e) {
                            return Promise.reject(new Error('Invalid JSON response from server'));
                          }
                        })
                        .then(discoveryResult => {
                          stopSaveProgressPolling(true);
                          const saved = discoveryResult.created_count || discoveryResult.discovered_count || 0;
                          const updated = discoveryResult.updated_count || 0;
                          const skipped = discoveryResult.skipped_count || 0;
                          const total = saved + updated;
                          
                          // Always show the result, even if all were skipped
                          if (total > 0) {
                            setDiscoveryProgress(prev => [...prev, `Saved ${total} assets to database (${saved} new, ${updated} updated, ${skipped} skipped)`]);
                          } else if (skipped > 0) {
                            setDiscoveryProgress(prev => [...prev, `All ${skipped} assets already exist in database (skipped duplicates)`]);
                          } else {
                            setDiscoveryProgress(prev => [...prev, `No new assets to save`]);
                          }
                          
                          // Use actual containers processed, not the initial container count
                          const actualContainerCount = actualContainersProcessed.size || 1;
                          
          setTestResult({
            success: true,
                            message: total > 0 
                              ? `Connection successful! Discovered ${total} assets in ${actualContainerCount} container(s)`
                              : skipped > 0
                                ? `Connection successful! All ${skipped} assets already exist (no new assets)`
                                : `Connection successful! No assets found`,
                            discoveredAssets: total,
                            totalContainers: actualContainerCount,
                  connectionId: connection.id,
                  containers: containersData.containers || [],
                });
                          
                          // Set testing to false only after everything is complete
                          setTesting(false);
                        })
                        .catch(err => {
                          stopSaveProgressPolling(true);
                          const errorMessage = err instanceof Error ? err.message : String(err || 'Unknown error');
                          setDiscoveryProgress(prev => [...prev, `Error saving assets: ${errorMessage}`]);
                          const actualContainerCount = actualContainersProcessed.size || 1;
                          setTestResult({
                            success: true,
                            message: `Connection successful! Found ${totalDiscovered} files but error saving: ${errorMessage}`,
                            discoveredAssets: 0,
                            totalContainers: actualContainerCount,
                            connectionId: connection.id,
                            containers: containersData.containers || [],
                          });
                          
                          // Set testing to false even on error
                          setTesting(false);
                        })
                        .catch(finalError => {
                          stopSaveProgressPolling(true);
                          // Final catch for any unhandled errors in the promise chain
                          const errorMessage = finalError instanceof Error ? finalError.message : String(finalError || 'Unknown error');
                          console.error('Unhandled error in discovery process:', finalError);
                          setDiscoveryProgress(prev => [...prev, `Unexpected error: ${errorMessage}`]);
                          setTestResult({
                            success: false,
                            message: `Error during discovery: ${errorMessage}`,
                            discoveredAssets: 0,
                            totalContainers: 0,
                          });
                          setTesting(false);
                        });
                      } else if (data.type === 'error') {
                        setDiscoveryProgress(prev => [...prev, `Error: ${data.message}`]);
                        const actualContainerCount = actualContainersProcessed.size || 1;
                        setTestResult({
                          success: false,
                          message: `Discovery error: ${data.message}`,
                          discoveredAssets: totalDiscovered,
                          totalContainers: actualContainerCount,
                        });
                        setTesting(false);
                        return;
                      } else if (data.type === 'warning') {
                        setDiscoveryProgress(prev => [...prev, `Warning: ${data.message}`]);
                      }
                    } catch (parseError) {
                      console.error('Error parsing SSE data:', parseError, line);
                    }
                  }
                }
              }
              
            } catch (discoverError) {
              setDiscoveryProgress(prev => [...prev, `Discovery error: ${discoverError.message}`]);
              console.error(`FN:handleTestConnection message:Discovery error error:${discoverError.message || discoverError}`);
              const actualContainerCount = actualContainersProcessed.size || 1;
          setTestResult({
            success: true,
                message: `Connection successful! Found ${actualContainerCount} container(s). Discovery error: ${discoverError.message}`,
                discoveredAssets: 0,
                totalContainers: actualContainerCount,
                connectionId: connection.id,
                containers: containersData.containers || [],
              });
              setTesting(false);
            }
          } else {
            setTestResult({
              success: true,
              message: `Connection successful! Found ${testData.container_count} container(s)`,
              discoveredAssets: 0,
              totalContainers: testData.container_count,
              connectionId: connection.id,
              containers: containersData.containers || [],
            });
            setTesting(false);
          }
        }
      } catch (error) {
        
        let errorMessage = 'Failed to test connection';
        
        if (error.message) {
          
          if (error.message.includes('JSON') || error.message.includes('unexpected token')) {
            errorMessage = `Backend returned invalid response. Please check if the backend service is running on port 8099. Error: ${error.message}`;
          } else {
            errorMessage = error.message;
          }
        }
        
        setTestResult({ 
          success: false, 
          message: errorMessage,
        });
      setTesting(false);
    }
    } else if (selectedConnector?.id === 'oracle_db') {
      if (!config.name) {
        setTestResult({ 
          success: false, 
          message: 'Please provide connection name',
        });
        return;
      }
    
      setTesting(true);
      setTestResult(null);
      setDiscoveryProgress([]);
      
      try {
        const API_BASE_URL = import.meta.env.VITE_API_BASE_URL;
        
        setDiscoveryProgress(prev => [...prev, 'Testing Oracle database connection...']);
        const testResponse = await fetch(`${API_BASE_URL}/api/connections/test-config`, {
          method: 'POST',
          headers: {
            'Content-Type': 'application/json',
          },
          body: JSON.stringify({
            config: config,
          }),
        });
        
        if (!testResponse.ok) {
          const contentType = testResponse.headers.get('content-type');
          let errorMessage = `Server returned ${testResponse.status} ${testResponse.statusText}`;
          
          if (contentType && contentType.includes('application/json')) {
            try {
              const errorData = await testResponse.json();
              errorMessage = errorData.error || errorData.message || errorMessage;
            } catch (e) {
              // Ignore JSON parse errors
            }
          }
          
          setTestResult({
            success: false,
            message: errorMessage,
          });
          setTesting(false);
          return;
        }
        
        const testData = await testResponse.json();
        
        if (!testData.success) {
          setTestResult({
            success: false,
            message: testData.message || 'Connection test failed',
          });
          setTesting(false);
          return;
        }
        
        setDiscoveryProgress(prev => [...prev, 'Connection test successful']);
        
        // Save or update connection
        let connection = null;
        const existingConnectionsResponse = await fetch(`${API_BASE_URL}/api/connections`);
        if (existingConnectionsResponse.ok) {
          const existingConnections = await existingConnectionsResponse.json();
          connection = existingConnections.find(
            conn => conn.name === config.name && conn.connector_type === 'oracle_db'
          );
        }
        
        if (connection) {
          setDiscoveryProgress(prev => [...prev, 'Updating existing connection...']);
          const updateResponse = await fetch(`${API_BASE_URL}/api/connections/${connection.id}`, {
            method: 'PUT',
            headers: {
              'Content-Type': 'application/json',
            },
            body: JSON.stringify({
              connection_type: connectionType,
              config: config,
              status: 'active',
            }),
          });
          
          if (!updateResponse.ok) {
            const errorData = await updateResponse.json();
            throw new Error(errorData.error || 'Failed to update connection');
          }
          
          connection = await updateResponse.json();
          setDiscoveryProgress(prev => [...prev, 'Connection updated successfully']);
        } else {
          setDiscoveryProgress(prev => [...prev, 'Saving connection...']);
          const createResponse = await fetch(`${API_BASE_URL}/api/connections`, {
            method: 'POST',
            headers: {
              'Content-Type': 'application/json',
            },
            body: JSON.stringify({
              name: config.name,
              connector_type: 'oracle_db',
              connection_type: connectionType,
              config: config,
              status: 'active',
            }),
          });
          
          if (!createResponse.ok) {
            const errorData = await createResponse.json();
            throw new Error(errorData.error || 'Failed to create connection');
          }
          
          connection = await createResponse.json();
          setDiscoveryProgress(prev => [...prev, 'Connection saved successfully']);
        }
        
        // Start Oracle discovery stream (SSE) - like Azure Blob Storage.
        // Keep testing=true until stream completes/errors.
        setDiscoveryProgress(prev => [...prev, 'Starting Oracle database discovery...']);

        try {
          const response = await fetch(`${API_BASE_URL}/api/connections/${connection.id}/discover-stream`, {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({
              schema_filter: config.schema_filter || '',
              include_lineage: false, // assets-only by default; lineage can be run separately
            }),
          });

          if (!response.ok) {
            let msg = `HTTP ${response.status}: ${response.statusText}`;
            try {
              const err = await response.json();
              msg = err.error || err.message || msg;
            } catch (e) {}
            throw new Error(msg);
          }

          const reader = response.body.getReader();
          const decoder = new TextDecoder();
          let buffer = '';

          while (true) {
            const { done, value } = await reader.read();
            if (done) break;

            buffer += decoder.decode(value, { stream: true });
            const lines = buffer.split('\n');
            buffer = lines.pop() || '';

            for (const line of lines) {
              if (!line.startsWith('data: ')) continue;
              try {
                const data = JSON.parse(line.slice(6));
                if (data.type === 'progress') {
                  setDiscoveryProgress(prev => [...prev, data.message]);
                } else if (data.type === 'complete') {
                  const created = data.created || 0;
                  const updated = data.updated || 0;
                  const skipped = data.skipped || 0;
                  const total = created + updated;
                  
                  // Check if it's Oracle DB (from connection or connector type)
                  const isOracleDB = connection?.connector_type === 'oracle_db' || selectedConnector?.id === 'oracle_db';
                  
                  // For Oracle, remove emoji and lineage from message
                  let cleanMessage = data.message || `Discovery complete! Found ${total} assets (${created} new, ${updated} updated, ${skipped} skipped).`;
                  // Remove emoji
                  cleanMessage = cleanMessage.replace(/✅\s*/g, '');
                  // Remove lineage relationships line for Oracle
                  if (isOracleDB) {
                    cleanMessage = cleanMessage.replace(/\.\s*Extracted \d+ lineage relationships\./g, '.');
                  }
                  
                  setDiscoveryProgress(prev => [...prev, cleanMessage]);
                  setTestResult({
                    success: true,
                    message: cleanMessage,
                    connectionId: connection.id,
                    discoveredAssets: total,
                    totalContainers: isOracleDB ? undefined : (testResult?.totalContainers || 0),
                  });
                  setTesting(false);
                  return;
                } else if (data.type === 'error') {
                  setDiscoveryProgress(prev => [...prev, `Error: ${data.message}`]);
                  setTestResult({
                    success: false,
                    message: `Discovery failed: ${data.message}`,
                    connectionId: connection.id,
                  });
                  setTesting(false);
                  return;
                } else if (data.type === 'warning') {
                  setDiscoveryProgress(prev => [...prev, `Warning: ${data.message}`]);
                }
              } catch (e) {
                // ignore parse errors
              }
            }
          }

          // If stream ended without explicit complete/error, fall back to polling once.
          startOracleDiscoveryPolling(connection.id);
        } catch (error) {
          setDiscoveryProgress(prev => [...prev, `Error: ${error.message}`]);
          setTestResult({
            success: false,
            message: `Discovery failed: ${error.message}`,
            connectionId: connection.id,
          });
          setTesting(false);
        }
      } catch (error) {
        let errorMessage = 'Failed to test connection';
        
        if (error.message) {
          errorMessage = error.message;
        }
        
        setTestResult({ 
          success: false, 
          message: errorMessage,
        });
        setTesting(false);
      }
    } else {
      setTestResult({ 
        success: false, 
        message: 'Test connection not available for this connector type',
      });
    }
  };

  const renderStepContent = (step) => {
    switch (step) {
      case 0:
        return (
          <Box>
            <Typography variant="h6" sx={{ mb: 2 }}>
              Select Connection Type for {selectedConnector?.name}
            </Typography>
            <FormControl component="fieldset">
              <RadioGroup
                value={connectionType}
                onChange={(e) => {
                  const selectedType = e.target.value;
                  setConnectionType(selectedType);
                  
                  if (selectedType === 'Service Principal') {
                    // Initialize empty config for manual input
                    setConfig({
                      account_name: '',
                      tenant_id: '',
                      client_id: '',
                      client_secret: '',
                      folder_path: '',
                      storage_type: 'datalake',
                      use_dfs_endpoint: true,
                    });
                  } else if (selectedType === 'Standard Connection' && selectedConnector?.id === 'oracle_db') {
                    setConfig({
                      host: '',
                      port: '1521',
                      service_name: '',
                      username: '',
                      password: '',
                      schema_filter: '',
                    });
                  } else if (selectedType === 'JDBC' && selectedConnector?.id === 'oracle_db') {
                    setConfig({
                      jdbc_url: '',
                      username: '',
                      password: '',
                      schema_filter: '',
                    });
                  } else {
                    // Connection String type - clear config
                    setConfig({});
                  }
                }}
              >
                {selectedConnector?.connectionTypes.map((type) => (
                  <FormControlLabel
                    key={type}
                    value={type}
                    control={<Radio />}
                    label={type}
                  />
                ))}
              </RadioGroup>
            </FormControl>
          </Box>
        );
      
      case 1:
        return (
          <Box>
            <Typography variant="h6" sx={{ mb: 2 }}>
              Configuration
            </Typography>
            <Grid container spacing={2}>
              <Grid item xs={12}>
                <Tooltip 
                  title="Don't use the same connection name as an existing connection. Each connection name must be unique."
                  arrow
                  placement="top"
                >
                <TextField
                  fullWidth
                    required
                  label="Connection Name"
                  value={config.name || ''}
                  onChange={(e) => setConfig({...config, name: e.target.value})}
                />
                </Tooltip>
              </Grid>
              <Grid item xs={12}>
                <TextField
                  fullWidth
                  required
                  label="Application Name"
                  value={config.application_name || ''}
                  onChange={(e) => setConfig({...config, application_name: e.target.value})}
                  helperText="Name of the application/system this connection belongs to (used for filtering assets)"
                />
              </Grid>
              {connectionType === 'Connection String' && selectedConnector?.id === 'azure_blob' && (
                <>
                  <Grid item xs={12}>
                    <TextField
                      fullWidth
                      required
                      label="Azure Storage Connection String"
                      type="password"
                      value={config.connection_string || ''}
                      onChange={(e) => setConfig({...config, connection_string: e.target.value})}
                      helperText="Enter your Azure Storage account connection string"
                    />
                  </Grid>
                  <Grid item xs={12}>
                    <TextField
                      fullWidth
                      label="Storage Account Name (optional)"
                      value={config.account_name || ''}
                      onChange={(e) => setConfig({...config, account_name: e.target.value})}
                      helperText="Optional: Name of your Azure Storage account"
                    />
                  </Grid>
                  <Grid item xs={12}>
                    <TextField
                      fullWidth
                      label="Folder Path (optional)"
                      value={config.folder_path || ''}
                      onChange={(e) => setConfig({...config, folder_path: e.target.value})}
                      helperText="Optional: Specific folder path to scan (leave empty for root)"
                    />
                  </Grid>
                </>
              )}
              {connectionType === 'Service Principal' && selectedConnector?.id === 'azure_blob' && (
                <>
                  <Grid item xs={12}>
                    <TextField
                      fullWidth
                      required
                      label="Storage Account Name"
                      value={config.account_name || ''}
                      onChange={(e) => setConfig({...config, account_name: e.target.value})}
                      helperText="Your Azure Storage account name"
                    />
                  </Grid>
                  <Grid item xs={12}>
                    <TextField
                      fullWidth
                      required
                      label="Tenant ID"
                      value={config.tenant_id || ''}
                      onChange={(e) => setConfig({...config, tenant_id: e.target.value})}
                      helperText="Azure AD Tenant ID"
                    />
                  </Grid>
                  <Grid item xs={12}>
                    <TextField
                      fullWidth
                      required
                      label="Client ID (Application ID)"
                      value={config.client_id || ''}
                      onChange={(e) => setConfig({...config, client_id: e.target.value})}
                      helperText="Service Principal Client ID / Application ID"
                    />
                  </Grid>
                  <Grid item xs={12}>
                    <TextField
                      fullWidth
                      required
                      label="Client Secret"
                      type="password"
                      value={config.client_secret || ''}
                      onChange={(e) => setConfig({...config, client_secret: e.target.value})}
                      helperText="Service Principal Client Secret"
                    />
                  </Grid>
                  <Grid item xs={12}>
                    <TextField
                      fullWidth
                      label="Folder Path (optional)"
                      value={config.folder_path || ''}
                      onChange={(e) => setConfig({...config, folder_path: e.target.value})}
                      helperText="Azure Data Lake path (e.g., abfs://container@account.dfs.core.windows.net/path)"
                    />
                  </Grid>
                  <Grid item xs={12}>
                    <FormControl fullWidth>
                      <InputLabel>Storage Type</InputLabel>
                      <Select
                        value={config.storage_type || 'datalake'}
                        onChange={(e) => setConfig({...config, storage_type: e.target.value, use_dfs_endpoint: e.target.value === 'datalake'})}
                        label="Storage Type"
                      >
                        <MenuItem value="datalake">Data Lake Gen2</MenuItem>
                        <MenuItem value="blob">Blob Storage</MenuItem>
                      </Select>
                    </FormControl>
                  </Grid>
                </>
              )}
              {connectionType === 'Standard Connection' && selectedConnector?.id === 'oracle_db' && (
                <>
                  <Grid item xs={12}>
                    <TextField
                      fullWidth
                      required
                      label="Host"
                      value={config.host || ''}
                      onChange={(e) => setConfig({...config, host: e.target.value})}
                      helperText="Oracle database hostname or IP address"
                    />
                  </Grid>
                  <Grid item xs={12} sm={6}>
                    <TextField
                      fullWidth
                      required
                      label="Port"
                      type="number"
                      value={config.port || '1521'}
                      onChange={(e) => setConfig({...config, port: e.target.value})}
                      helperText="Oracle database port (default: 1521)"
                    />
                  </Grid>
                  <Grid item xs={12} sm={6}>
                    <TextField
                      fullWidth
                      required
                      label="Service Name / SID"
                      value={config.service_name || ''}
                      onChange={(e) => setConfig({...config, service_name: e.target.value})}
                      helperText="Oracle service name or SID"
                    />
                  </Grid>
                  <Grid item xs={12}>
                    <TextField
                      fullWidth
                      required
                      label="Username"
                      value={config.username || ''}
                      onChange={(e) => setConfig({...config, username: e.target.value})}
                      helperText="Oracle database username"
                    />
                  </Grid>
                  <Grid item xs={12}>
                    <TextField
                      fullWidth
                      required
                      label="Password"
                      type="password"
                      value={config.password || ''}
                      onChange={(e) => setConfig({...config, password: e.target.value})}
                      helperText="Oracle database password"
                    />
                  </Grid>
                  <Grid item xs={12}>
                    <TextField
                      fullWidth
                      label="Schema Filter (optional)"
                      value={config.schema_filter || ''}
                      onChange={(e) => setConfig({...config, schema_filter: e.target.value})}
                      helperText="Optional: Comma-separated list of schemas to scan (leave empty for all)"
                    />
                  </Grid>
                </>
              )}
              {connectionType === 'JDBC' && selectedConnector?.id === 'oracle_db' && (
                <>
                  <Grid item xs={12}>
                    <TextField
                      fullWidth
                      required
                      label="JDBC URL"
                      value={config.jdbc_url || ''}
                      onChange={(e) => setConfig({...config, jdbc_url: e.target.value})}
                      helperText="Oracle JDBC connection URL (e.g., jdbc:oracle:thin:@//host:port/service_name or jdbc:oracle:thin:@host:port:service_name)"
                      placeholder="jdbc:oracle:thin:@//host:port/service_name"
                    />
                  </Grid>
                  <Grid item xs={12}>
                    <TextField
                      fullWidth
                      required
                      label="Username"
                      value={config.username || ''}
                      onChange={(e) => setConfig({...config, username: e.target.value})}
                      helperText="Oracle database username"
                    />
                  </Grid>
                  <Grid item xs={12}>
                    <TextField
                      fullWidth
                      required
                      label="Password"
                      type="password"
                      value={config.password || ''}
                      onChange={(e) => setConfig({...config, password: e.target.value})}
                      helperText="Oracle database password"
                    />
                  </Grid>
                  <Grid item xs={12}>
                    <TextField
                      fullWidth
                      label="Schema Filter (optional)"
                      value={config.schema_filter || ''}
                      onChange={(e) => setConfig({...config, schema_filter: e.target.value})}
                      helperText="Optional: Comma-separated list of schemas to scan (leave empty for all)"
                    />
                  </Grid>
                </>
              )}
            </Grid>
          </Box>
        );
      
      case 2:
        return (
          <Box>
            <Typography variant="h6" sx={{ mb: 2 }}>
              Test Connection
            </Typography>
            <Box sx={{ textAlign: 'center', py: 4 }}>
              {testing ? (
                <Box>
                  <CircularProgress sx={{ mb: 2 }} />
                  <Typography sx={{ mb: 3, fontWeight: 600 }}>Testing connection...</Typography>
                  

                  {discoveryProgress.length > 0 && (
                    <Box sx={{ 
                      maxHeight: '300px', 
                      overflowY: 'auto', 
                      bgcolor: '#f5f5f5', 
                      p: 2, 
                      borderRadius: 1,
                      textAlign: 'left',
                      fontFamily: 'monospace',
                      fontSize: '0.875rem'
                    }}>
                      {discoveryProgress.map((message, index) => (
                        <Box key={index} sx={{ 
                          py: 0.5,
                          color: message.includes('Discovery complete') ? 'success.main' : 
                                 message.includes('Error') ? 'error.main' : 
                                 message.includes('Discovering') ? 'primary.main' : 'text.primary',
                          display: 'flex',
                          alignItems: 'center',
                          gap: 1
                        }}>
                          {message.includes('Discovery complete') && <CheckCircle sx={{ fontSize: 16 }} />}
                          <span>{message}</span>
                        </Box>
                      ))}
                    </Box>
                  )}
                </Box>
              ) : testResult ? (
                <Box>
                  {}
                  {!testResult.success && (
                    <Alert severity="error" sx={{ mb: 2, whiteSpace: 'pre-line' }}>
                      {testResult.message}
                    </Alert>
                  )}
                  
                  {}
                  {testResult.success && testResult.discoveredAssets > 0 && (
                    <Card variant="outlined" sx={{ p: 2, mb: 3, bgcolor: 'success.light', color: 'success.dark' }}>
                      <Typography variant="h6" sx={{ fontWeight: 600, mb: 1 }}>
                        Discovery Complete
                      </Typography>
                      <Typography variant="body1">
                        {testResult.totalContainers !== undefined ? (
                          <>
                        <strong>{testResult.discoveredAssets}</strong> assets discovered across <strong>{testResult.totalContainers || 0}</strong> container(s)
                          </>
                        ) : (
                          <>
                            <strong>{testResult.discoveredAssets}</strong> assets discovered
                          </>
                        )}
                      </Typography>
                      {testResult.accountName && (
                        <Typography variant="body2" sx={{ mt: 0.5, opacity: 0.9 }}>
                          Account: {testResult.accountName}
                        </Typography>
                      )}
                      <Typography variant="body2" sx={{ mt: 1, opacity: 0.9 }}>
                        View them in the "Discovered Assets" section
                      </Typography>
                    </Card>
                  )}
                  
                  
                  {}
                  {discoveryProgress.length > 0 && (
                    <Box>
                      <Typography variant="subtitle2" sx={{ mb: 1, textAlign: 'left', fontWeight: 600 }}>
                        Discovery Progress:
                      </Typography>
                      <Box sx={{ 
                        maxHeight: '300px', 
                        overflowY: 'auto', 
                        bgcolor: '#f5f5f5', 
                        p: 2, 
                        borderRadius: 1,
                        textAlign: 'left',
                        fontFamily: 'monospace',
                        fontSize: '0.875rem',
                        border: '1px solid #e0e0e0'
                      }}>
                        {discoveryProgress.map((message, index) => {
                          const isContainer = message.includes('[CONTAINER]');
                          const isFolder = message.includes('[FOLDER]');
                          const isAsset = message.includes('[FILE]');
                          const isSchema = message.includes('[SCHEMA]');
                          const isTable = message.includes('[TABLE]');
                          const isView = message.includes('[VIEW]');
                          const isMView = message.includes('[MATERIALIZED VIEW]');
                          const isProcedure = message.includes('[PROCEDURE]');
                          const isFunction = message.includes('[FUNCTION]');
                          const isTrigger = message.includes('[TRIGGER]');
                          const isLineage = message.includes('[LINEAGE]');
                          const isSuccess = message.includes('Discovery complete') || message.includes('✅') || message.includes('successful');
                          const isError = message.includes('Error') || message.includes('error');
                          const isWarning = message.includes('Warning');
                          
                          let color = 'text.primary';
                          if (isSuccess) color = 'success.main';
                          else if (isError) color = 'error.main';
                          else if (isWarning) color = 'warning.main';
                          else if (isContainer || isSchema) color = 'primary.main';
                          else if (isFolder) color = 'info.main';
                          else if (isAsset || isTable || isView || isMView || isProcedure || isFunction || isTrigger) color = 'text.secondary';
                          else if (isLineage) color = 'info.main';
                          
                          return (
                            <Box key={index} sx={{ 
                              py: 0.5,
                              pl: (isAsset || isTable || isView || isMView || isProcedure || isFunction || isTrigger) ? 3 : (isFolder) ? 2 : (isContainer || isSchema) ? 1 : 0,
                              color: color,
                              fontWeight: (isContainer || isSchema) ? 600 : 'normal',
                              fontSize: (isAsset || isTable || isView || isMView || isProcedure || isFunction || isTrigger) ? '0.8rem' : '0.875rem'
                            }}>
                              {message}
                            </Box>
                          );
                        })}
                      </Box>
                    </Box>
                  )}
                  
                  {}
                  {testResult && (
                    <Box sx={{ mt: 3, display: 'flex', justifyContent: 'center' }}>
                      <Tooltip title="Retry testing the connection with the current configuration">
                        <span>
                          <Button
                            variant="outlined"
                            startIcon={<Replay />}
                            onClick={handleTestConnection}
                            disabled={testing}
                            sx={{ minWidth: '150px' }}
                          >
                            {testing ? 'Testing...' : 'Retry Test'}
                          </Button>
                        </span>
                      </Tooltip>
                    </Box>
                  )}
                </Box>
              ) : (
                <Tooltip title="Test the connection with the provided credentials before saving">
                  <span>
                    <Button
                      variant="contained"
                      onClick={handleTestConnection}
                      disabled={
                        !connectionType || 
                        !config.name
                      }
                    >
                      Test Connection
                    </Button>
                  </span>
                </Tooltip>
              )}
            </Box>
          </Box>
        );
      
      case 3:
        return (
          <Box>
            <Typography variant="h6" sx={{ mb: 2 }}>
              Summary
            </Typography>
            <Card variant="outlined" sx={{ p: 2 }}>
              <Typography variant="subtitle1" sx={{ mb: 1 }}>
                <strong>Connector:</strong> {selectedConnector?.name}
              </Typography>
              <Typography variant="subtitle1" sx={{ mb: 1 }}>
                <strong>Connection Type:</strong> {connectionType}
              </Typography>
              <Typography variant="subtitle1" sx={{ mb: 1 }}>
                <strong>Connection Name:</strong> {config.name}
              </Typography>
              {testResult && (
                <Alert severity={testResult.success ? 'success' : 'error'} sx={{ mt: 2 }}>
                  {testResult.message}
                </Alert>
              )}
            </Card>
          </Box>
        );
      
      default:
        return 'Unknown step';
    }
  };

  return (
    <Box sx={{ p: 3 }}>
      <Box sx={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center', mb: 4 }}>
        <Box sx={{ display: 'flex', alignItems: 'center', gap: 2 }}>
          <Typography variant="h4" component="h1" sx={{ fontWeight: 600 }}>
            Connectors
          </Typography>
        </Box>
        <Box sx={{ display: 'flex', gap: 2 }}>
          <Tooltip title="Refresh the connections list to get the latest data">
            <span>
              <Button
                variant="outlined"
                startIcon={<Refresh />}
                onClick={() => {
                  fetchMyConnections();
                }}
                disabled={loading}
              >
                Refresh
              </Button>
            </span>
          </Tooltip>
          <Tooltip title="Create a new data source connection">
            <span>
              <Button
                variant="contained"
                startIcon={<Add />}
                onClick={() => setDialogOpen(true)}
              >
                New Connector
              </Button>
            </span>
          </Tooltip>
        </Box>
      </Box>

      {}
      <Card sx={{ mb: 4 }}>
        <CardContent>
          <Box sx={{ display: 'flex', alignItems: 'center', mb: 3 }}>
            <CloudSync sx={{ mr: 1.5, color: 'primary.main', fontSize: 28 }} />
            <Typography variant="h5" sx={{ fontWeight: 600 }}>
              My Connections
            </Typography>
          </Box>
          
          {myConnections.length > 0 ? (
            <Grid container spacing={2}>
                  {myConnections.map((connection) => (
                    <Grid item xs={12} sm={6} md={4} key={connection.id}>
                      <Card variant="outlined" sx={{ p: 2, height: '100%', position: 'relative' }}>
                        <Box sx={{ display: 'flex', alignItems: 'center', mb: 2 }}>
                          <Avatar sx={{ bgcolor: 'primary.main', mr: 2 }}>
                            <CloudSync />
                          </Avatar>
                          <Box sx={{ flex: 1 }}>
                            <Typography variant="h6" sx={{ fontWeight: 600 }}>
                              {connection.name}
                            </Typography>
                            <Chip
                              label={connection.status}
                              size="small"
                              color={connection.status === 'active' ? 'success' : 'error'}
                              sx={{ mt: 0.5 }}
                            />
                          </Box>
                          <Tooltip title={`Delete connection "${connection.name}"`}>
                            <IconButton
                              size="small"
                              onClick={() => handleDeleteClick(connection)}
                              sx={{ 
                                color: 'error.main',
                                '&:hover': {
                                  backgroundColor: 'error.light',
                                  color: 'error.dark'
                                }
                              }}
                            >
                              <Delete fontSize="small" />
                            </IconButton>
                          </Tooltip>
                        </Box>
                        <Typography variant="body2" color="text.secondary" sx={{ mb: 1 }}>
                          Type: {connection.type}
                        </Typography>
                        <Typography variant="body2" color="text.secondary">
                          Last run: {connection.last_run ? new Date(connection.last_run).toLocaleString() : 'Never'}
                        </Typography>
                      </Card>
                    </Grid>
                  ))}
            </Grid>
          ) : (
            <Box sx={{ textAlign: 'center', py: 4 }}>
              <CloudSync sx={{ fontSize: 48, color: 'text.secondary', mb: 2, opacity: 0.5 }} />
              <Typography variant="h6" color="text.secondary" sx={{ mb: 1 }}>
                No active connections
              </Typography>
              <Typography variant="body2" color="text.secondary">
                Connect to data sources below to get started
              </Typography>
            </Box>
          )}
        </CardContent>
      </Card>

      {}
      <Card>
        <CardContent>
          <Box sx={{ display: 'flex', alignItems: 'center', mb: 3 }}>
            <Add sx={{ mr: 1.5, color: 'primary.main', fontSize: 28 }} />
            <Typography variant="h5" sx={{ fontWeight: 600 }}>
              Available Connectors
            </Typography>
          </Box>
          
          <Grid container spacing={3}>
            {availableConnectors.map((connector) => (
              <Grid item xs={12} sm={4} md={4} key={connector.id}>
                <Card 
                  variant="outlined" 
                  sx={{ 
                    p: 3, 
                    height: '100%',
                    cursor: 'pointer',
                    transition: 'all 0.2s ease-in-out',
                    '&:hover': {
                      boxShadow: 3,
                      transform: 'translateY(-2px)',
                    }
                  }}
                >
                  <Box sx={{ display: 'flex', alignItems: 'center', mb: 2 }}>
                    <Avatar 
                      sx={{ 
                        bgcolor: connector.color, 
                        mr: 2,
                        width: 48,
                        height: 48,
                      }}
                    >
                      {logoErrors.has(connector.id) ? (
                        <Box sx={{ color: 'white', display: 'flex', alignItems: 'center', justifyContent: 'center' }}>
                          {connector.fallbackIcon}
                        </Box>
                      ) : (
                        <img 
                          src={connector.logo} 
                          alt={connector.name}
                          style={{ 
                            width: '32px', 
                            height: '32px',
                            objectFit: 'contain'
                          }}
                          onError={() => {
                            setLogoErrors(prev => new Set([...prev, connector.id]));
                          }}
                        />
                      )}
                    </Avatar>
                    <Box>
                      <Typography variant="h6" sx={{ fontWeight: 600 }}>
                        {connector.name}
                      </Typography>
                    </Box>
                  </Box>
                  
                  <Typography variant="body2" color="text.secondary" sx={{ mb: 3 }}>
                    {connector.description}
                  </Typography>
                  
                  <Tooltip title={`Connect to ${connector.name} data source`}>
                    <span>
                      <Button
                        variant="contained"
                        fullWidth
                        onClick={() => handleConnectClick(connector)}
                        sx={{
                          bgcolor: connector.color,
                          '&:hover': {
                        bgcolor: connector.color,
                        opacity: 0.9,
                      }
                    }}
                  >
                    Connect
                  </Button>
                    </span>
                  </Tooltip>
                </Card>
              </Grid>
            ))}
          </Grid>
        </CardContent>
      </Card>

      {}
      <Dialog 
        open={dialogOpen} 
        onClose={() => setDialogOpen(false)}
        maxWidth="sm"
        fullWidth
      >
        <Box sx={{ p: 3, position: 'relative' }}>
          <IconButton 
            onClick={() => setDialogOpen(false)} 
            sx={{ 
              position: 'absolute', 
              right: 8, 
              top: 8 
            }}
          >
            <Close />
          </IconButton>
          
          <Typography variant="h5" sx={{ fontWeight: 600, mb: 3, textAlign: 'center' }}>
            Select Connector
          </Typography>
          
          <Grid container spacing={2}>
            {availableConnectors.map((connector) => (
              <Grid item xs={4} key={connector.id}>
                <Card 
                  variant="outlined" 
                  sx={{ 
                    p: 2,
                    cursor: 'pointer',
                    transition: 'all 0.2s ease-in-out',
                    display: 'flex',
                    flexDirection: 'column',
                    alignItems: 'center',
                    '&:hover': {
                      boxShadow: 3,
                      transform: 'translateY(-2px)',
                      borderColor: connector.color,
                    }
                  }}
                  onClick={() => {
                    handleConnectClick(connector);
                    setDialogOpen(false);
                  }}
                >
                  <Avatar 
                    sx={{ 
                      bgcolor: connector.color, 
                      width: 64,
                      height: 64,
                      mb: 1.5,
                    }}
                  >
                    {logoErrors.has(connector.id) ? (
                      <Box sx={{ color: 'white', display: 'flex', alignItems: 'center', justifyContent: 'center', fontSize: 40 }}>
                        {connector.fallbackIcon}
                      </Box>
                    ) : (
                      <img 
                        src={connector.logo} 
                        alt={connector.name}
                        style={{ 
                          width: '48px', 
                          height: '48px',
                          objectFit: 'contain'
                        }}
                        onError={() => {
                          setLogoErrors(prev => new Set([...prev, connector.id]));
                        }}
                      />
                    )}
                  </Avatar>
                  
                  <Typography 
                    variant="body2" 
                    sx={{ 
                      fontWeight: 600,
                      textAlign: 'center',
                      fontSize: '0.875rem'
                    }}
                  >
                    {connector.name}
                  </Typography>
                </Card>
              </Grid>
            ))}
          </Grid>
        </Box>
      </Dialog>

      {}
      <Dialog 
        open={wizardOpen} 
        onClose={handleWizardClose}
        maxWidth="lg"
        fullWidth
        disableEnforceFocus
        disableAutoFocus
        disableRestoreFocus
      >
        <Box sx={{ p: 3 }}>
          <Box sx={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center', mb: 3 }}>
            <Typography variant="h5" sx={{ fontWeight: 600 }}>
              Connect to {selectedConnector?.name}
            </Typography>
            <Tooltip title="Close the connection wizard">
              <IconButton onClick={handleWizardClose}>
                <Close />
              </IconButton>
            </Tooltip>
          </Box>
          
          <Stepper activeStep={activeStep} sx={{ mb: 4 }}>
            {wizardSteps.map((label) => (
              <Step key={label}>
                <StepLabel>{label}</StepLabel>
              </Step>
            ))}
          </Stepper>
          
          <Box sx={{ mb: 3, minHeight: 300 }}>
            {renderStepContent(activeStep)}
          </Box>
          
          <Box sx={{ display: 'flex', justifyContent: 'space-between' }}>
            <Tooltip title="Go back to the previous step">
              <span>
                <Button
                  disabled={activeStep === 0}
                  onClick={handleBack}
                  startIcon={<ArrowBack />}
                >
                  Back
                </Button>
              </span>
            </Tooltip>
            <Box>
              <Tooltip title="Cancel and close the connection wizard">
                <span>
                  <Button onClick={handleWizardClose} sx={{ mr: 1 }}>
                    Cancel
                  </Button>
                </span>
              </Tooltip>
              {activeStep === wizardSteps.length - 1 ? (
                <Tooltip title="Complete the connection setup and save">
                  <span>
                    <Button variant="contained" onClick={handleFinish}>
                      Complete
                    </Button>
                  </span>
                </Tooltip>
              ) : (
                <Tooltip title="Continue to the next step">
                  <span>
                    <Button
                      variant="contained"
                      onClick={handleNext}
                      endIcon={<ArrowForward />}
                      disabled={
                        (activeStep === 0 && !connectionType) ||
                        (activeStep === 1 && (
                          !config.name || 
                          (connectionType === 'Connection String' && selectedConnector?.id === 'azure_blob' && !config.connection_string) ||
                      (connectionType === 'Service Principal' && selectedConnector?.id === 'azure_blob' && !config.name) ||
                      (connectionType === 'Standard Connection' && selectedConnector?.id === 'oracle_db' && (!config.host || !config.service_name || !config.username || !config.password)) ||
                      (connectionType === 'JDBC' && selectedConnector?.id === 'oracle_db' && (!config.jdbc_url || !config.username || !config.password))
                    )) ||
                    (activeStep === 2 && !testResult)
                  }
                >
                  Next
                </Button>
                  </span>
                </Tooltip>
              )}
            </Box>
          </Box>
        </Box>
          </Dialog>

          {}
          <Dialog
            open={deleteDialogOpen}
            onClose={handleDeleteCancel}
            maxWidth="sm"
            fullWidth
          >
            <Box sx={{ p: 3 }}>
              <Typography variant="h6" sx={{ fontWeight: 600, mb: 2 }}>
                Delete Connection
              </Typography>
              
              <Typography variant="body1" sx={{ mb: 3 }}>
                Are you sure you want to delete the connection "{connectionToDelete?.name}"? 
                This action cannot be undone and will also remove all associated discovered assets.
              </Typography>
              
              <Box sx={{ display: 'flex', justifyContent: 'flex-end', gap: 2 }}>
                <Button
                  variant="outlined"
                  onClick={handleDeleteCancel}
                >
                  Cancel
                </Button>
                <Button
                  variant="contained"
                  color="error"
                  onClick={handleDeleteConfirm}
                >
                  Delete
                </Button>
              </Box>
            </Box>
          </Dialog>
        </Box>
      );
    };

    export default ConnectorsPage;
