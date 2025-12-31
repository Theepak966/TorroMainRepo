
import multiprocessing
import os
from pathlib import Path

bind = f"0.0.0.0:{os.getenv('FLASK_PORT', '8099')}"
backlog = 2048

workers = multiprocessing.cpu_count() * 2 + 1
worker_class = "sync"
worker_connections = 1000
timeout = 600
keepalive = 2




backend_dir = Path(__file__).parent
logs_dir = backend_dir.parent / "logs"
logs_dir.mkdir(exist_ok=True)

accesslog = str(logs_dir / "gunicorn-access.log")
errorlog = str(logs_dir / "gunicorn-error.log")
loglevel = "info"
access_log_format = '%(h)s %(l)s %(u)s %(t)s "%(r)s" %(s)s %(b)s "%(f)s" "%(a)s" %(D)s'

proc_name = "torroforexcel-api"

daemon = False
pidfile = str(logs_dir / "gunicorn.pid")
umask = 0
user = None
group = None
tmp_upload_dir = None

