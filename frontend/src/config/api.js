

const API_BASE_URL = import.meta.env.VITE_API_BASE_URL;
if (!API_BASE_URL) {
  throw new Error('VITE_API_BASE_URL environment variable is required');
}

export const API_ENDPOINTS = {
  BASE_URL: API_BASE_URL,
  HEALTH: `${API_BASE_URL}/api/health`,
  CONNECTIONS: `${API_BASE_URL}/api/connections`,
  ASSETS: `${API_BASE_URL}/api/assets`,
  ASSET: (id) => `${API_BASE_URL}/api/assets/${id}`,
  CONNECTION: (id) => `${API_BASE_URL}/api/connections/${id}`,
};

export default API_ENDPOINTS;

