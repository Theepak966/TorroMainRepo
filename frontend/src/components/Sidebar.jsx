import React from 'react';
import {
  Drawer,
  List,
  ListItem,
  ListItemButton,
  ListItemIcon,
  ListItemText,
  Box,
} from '@mui/material';
import {
  Dashboard,
  Storage,
  AccountTree,
  Search,
  Timeline,
  Settings,
} from '@mui/icons-material';
import { useNavigate, useLocation } from 'react-router-dom';

const Sidebar = ({ open, onClose }) => {
  const navigate = useNavigate();
  const location = useLocation();

  const menuItems = [
    { label: 'Connectors', icon: <Settings />, path: 'connectors' },
    { label: 'Discovered Assets', icon: <AccountTree />, path: '' },
    { label: 'Data Lineage', icon: <Timeline />, path: 'lineage' },
  ];

  const handleItemClick = (path) => {
    navigate(path || '/');
    onClose();
  };

  return (
    <Drawer
      anchor="left"
      open={open}
      onClose={onClose}
      sx={{
        '& .MuiDrawer-paper': {
          width: 280,
          backgroundColor: '#ffffff',
          borderRight: '1px solid #e5e7eb',
        },
      }}
    >
      <Box sx={{ pt: 3 }}>
        <List sx={{ px: 2 }}>
          {menuItems.map((item) => {
            const itemPath = item.path === '' ? '/' : `/${item.path}`;
            
            const normalizedPath = location.pathname.replace(/^\/airflow-fe/, '') || '/';
            const isActive = normalizedPath === itemPath;
            return (
              <ListItem key={item.path} disablePadding>
                <ListItemButton
                  onClick={() => handleItemClick(item.path)}
                  sx={{
                    borderRadius: 2,
                    mb: 0.5,
                    py: 1.5,
                    backgroundColor: isActive ? 'primary.main' : 'transparent',
                    color: isActive ? 'white' : 'text.primary',
                    '&:hover': {
                      backgroundColor: isActive ? 'primary.dark' : 'action.hover',
                    },
                  }}
                >
                  <ListItemIcon sx={{ color: isActive ? 'white' : 'text.secondary', minWidth: 40 }}>
                    {item.icon}
                  </ListItemIcon>
                  <ListItemText 
                    primary={item.label}
                    sx={{
                      '& .MuiListItemText-primary': {
                        fontWeight: isActive ? 600 : 500,
                        fontSize: '0.95rem',
                      },
                    }}
                  />
                </ListItemButton>
              </ListItem>
            );
          })}
        </List>
      </Box>
    </Drawer>
  );
};

export default Sidebar;
