# Fix for Frontend Connection Errors

## Issue
Frontend is trying to connect to `10.0.0.5:8099` instead of `localhost:8099`

## Solution Applied
1. ✅ Created `.env` file with `VITE_API_BASE_URL=http://localhost:8099`
2. ✅ Created `.env.local` file (Vite priority file)
3. ✅ Created `.env.development` file
4. ✅ Cleared Vite cache
5. ✅ Restarted frontend server

## IMPORTANT: Browser Cache Issue

The frontend code is now correct, but your **browser has cached the old JavaScript** that was trying to connect to `10.0.0.5:8099`.

### You MUST do one of these:

**Option 1: Hard Refresh (Recommended)**
- **Chrome/Edge**: Press `Ctrl+Shift+R` (Windows) or `Cmd+Shift+R` (Mac)
- **Firefox**: Press `Ctrl+F5` (Windows) or `Cmd+Shift+R` (Mac)
- **Safari**: Press `Cmd+Option+R`

**Option 2: Clear Browser Cache**
1. Open Developer Tools (F12)
2. Right-click the refresh button
3. Select "Empty Cache and Hard Reload"

**Option 3: Open in Incognito/Private Window**
- This will bypass cache completely
- Open `http://localhost:5162/airflow-fe/` in incognito mode

## Verify It's Working

After hard refresh, open Developer Console (F12) and check:
1. Network tab - should see requests to `localhost:8099` (not `10.0.0.5:8099`)
2. Console tab - should NOT see `ERR_CONNECTION_TIMED_OUT` errors
3. Application should load connections successfully

## Current Configuration
- Frontend URL: `http://localhost:5162/airflow-fe/`
- Backend URL: `http://localhost:8099`
- Environment: Development

## If Still Not Working

1. Check backend is running:
   ```bash
   curl http://localhost:8099/health
   ```

2. Check frontend logs:
   ```bash
   tail -f logs/frontend.log
   ```

3. Verify .env file:
   ```bash
   cat frontend/.env
   ```
