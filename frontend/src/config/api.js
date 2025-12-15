

const API_BASE_URL = import.meta.env.VITE_API_BASE_URL || 'http://localhost:8099';

export const API_ENDPOINTS = {
  BASE_URL: API_BASE_URL,
  HEALTH: `${API_BASE_URL}/api/health`,
  CONNECTIONS: `${API_BASE_URL}/api/connections`,
  ASSETS: `${API_BASE_URL}/api/assets`,
  ASSET: (id) => `${API_BASE_URL}/api/assets/${id}`,
  CONNECTION: (id) => `${API_BASE_URL}/api/connections/${id}`,
};

export default API_ENDPOINTS;

