import React from 'react';
import { BrowserRouter as Router, Routes, Route } from 'react-router-dom';
import { ThemeProvider, createTheme } from '@mui/material/styles';
import CssBaseline from '@mui/material/CssBaseline';

import Layout from './components/Layout';

import AssetsPage from './pages/AssetsPage';
import MarketplacePage from './pages/MarketplacePage';
import DataLineagePage from './pages/DataLineagePage';
import ConnectorsPage from './pages/ConnectorsPage';

const theme = createTheme({
  palette: {
    primary: {
      main: '#8FA0F5',
      light: '#B3C0F7',
      dark: '#6B7CF3',
    },
    secondary: {
      main: '#f3f4f6',
      light: '#f9fafb',
      dark: '#e5e7eb',
    },
    background: {
      default: '#fafafa',
      paper: '#ffffff',
    },
    text: {
      primary: '#1f2937',
      secondary: '#6b7280',
    },
  },
  typography: {
    fontFamily: '"Roboto", "Helvetica", "Arial", sans-serif',
    h4: {
      fontWeight: 700,
    },
    h6: {
      fontWeight: 600,
    },
  },
  components: {
    MuiCard: {
      styleOverrides: {
        root: {
          borderRadius: 0,
          boxShadow: '0 1px 3px 0 rgba(0, 0, 0, 0.1), 0 1px 2px 0 rgba(0, 0, 0, 0.06)',
        },
      },
    },
    MuiChip: {
      styleOverrides: {
        root: {
          borderRadius: 8,
        },
      },
    },
    MuiContainer: {
      styleOverrides: {
        root: {
          maxWidth: 'none !important',
          width: '100% !important',
        },
      },
    },
    MuiGrid: {
      styleOverrides: {
        container: {
          maxWidth: 'none !important',
          width: '100% !important',
        },
      },
    },
  },
});

function App() {
  return (
    <ThemeProvider theme={theme}>
      <CssBaseline />
      <Router>
        <Routes>
          <Route path="/" element={<Layout />}>
            <Route index element={<AssetsPage />} />
            <Route path="assets" element={<AssetsPage />} />
            <Route path="lineage" element={<DataLineagePage />} />
            <Route path="marketplace" element={<MarketplacePage />} />
            <Route path="connectors" element={<ConnectorsPage />} />
          </Route>
        </Routes>
      </Router>
    </ThemeProvider>
  );
}

export default App;
