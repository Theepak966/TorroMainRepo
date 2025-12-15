import React, { useState } from 'react';
import { Outlet } from 'react-router-dom';
import {
  Box,
} from '@mui/material';

import Header from './Header';
import Sidebar from './Sidebar';

const Layout = () => {
  const [sidebarOpen, setSidebarOpen] = useState(false);

  const handleMenuClick = () => {
    setSidebarOpen(true);
  };

  const handleSidebarClose = () => {
    setSidebarOpen(false);
  };

  return (
    <Box sx={{ 
      flexGrow: 1, 
      minHeight: '100vh', 
      backgroundColor: 'background.default',
      width: '100%',
      maxWidth: 'none'
    }}>
      <Header onMenuClick={handleMenuClick} />
      <Sidebar open={sidebarOpen} onClose={handleSidebarClose} />
      <Outlet />
    </Box>
  );
};

export default Layout;
