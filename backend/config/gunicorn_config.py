
import multiprocessing
import os
from pathlib import Path

bind = f"0.0.0.0:{os.getenv('FLASK_PORT', '8099')}"
backlog = 4096  # UPGRADE: Increased from 2048 for high traffic

# UPGRADE: More workers for 500 users
cpu_count = multiprocessing.cpu_count()
workers = min(cpu_count * 4 + 1, 30)  # UPGRADE: Increased from 5 to up to 30
worker_class = "sync"
worker_connections = 1000
timeout = 900  # INCREASED: 15 minutes for large discovery operations (2977+ assets)
graceful_timeout = 30  # Give workers 30s to finish before killing

# NEW FEATURE: Worker recycling to prevent memory leaks
max_requests = 2000  # Restart worker after 2000 requests
max_requests_jitter = 100  # Randomize restart (1900-2000 requests)
preload_app = False  # Don't preload - each worker loads independently

keepalive = 10  # UPGRADE: Increased from 2




backend_dir = Path(__file__).parent
logs_dir = backend_dir.parent / "logs"
logs_dir.mkdir(exist_ok=True)

accesslog = str(logs_dir / "gunicorn-access.log")
errorlog = str(logs_dir / "gunicorn-error.log")
loglevel = "info"
access_log_format = '%(h)s %(l)s %(u)s %(t)s "%(r)s" %(s)s %(b)s "%(f)s" "%(a)s" %(D)s'

proc_name = "torroforairflow-api"

daemon = False
pidfile = str(logs_dir / "gunicorn.pid")
umask = 0
user = None
group = None
tmp_upload_dir = None

