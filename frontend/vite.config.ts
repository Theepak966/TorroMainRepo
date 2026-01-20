import { defineConfig, loadEnv } from 'vite'
import react from '@vitejs/plugin-react'

export default defineConfig(({ mode }) => {
  // Ensure `.env*` values win even if the shell has VITE_* variables set.
  // This prevents the frontend from accidentally pointing at an old/stale IP like 10.0.0.5.
  const env = loadEnv(mode, process.cwd(), 'VITE_')
  if (env.VITE_API_BASE_URL) {
    process.env.VITE_API_BASE_URL = env.VITE_API_BASE_URL
  }

  return {
    plugins: [react()],
    base: '/airflow-fe/',
    server: {
      host: '0.0.0.0',
      port: 5162,
      open: true,
      proxy: {
        '/airflow-be': {
          target: env.VITE_API_BASE_URL || 'http://localhost:8099',
          changeOrigin: true,
          rewrite: (path) => path.replace(/^\/airflow-be/, '')
        }
      }
    }
  }
})
