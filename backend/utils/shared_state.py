"""
Shared application state for route handlers.
Production-level state management for discovery progress and lineage jobs.
"""

from threading import Lock
from datetime import datetime
import logging

logger = logging.getLogger(__name__)

# In-memory discovery progress store (used by frontend polling during long-running saves)
# Keyed by connection_id. Best-effort only (cleared on process restart).
DISCOVERY_PROGRESS = {}
DISCOVERY_PROGRESS_LOCK = Lock()
# TTL for progress entries: 24 hours (86400 seconds)
DISCOVERY_PROGRESS_TTL = 86400

# In-memory lineage extraction guard to avoid request storms creating endless threads.
# Best-effort only (cleared on process restart).
LINEAGE_JOBS_LOCK = Lock()
LINEAGE_JOBS_RUNNING = {}  # key -> {"started_at": datetime}
LINEAGE_JOBS_TTL_SECONDS = 30 * 60  # 30 minutes


def set_discovery_progress(connection_id: int, **updates):
    """Set discovery progress for a connection (alias for _set_discovery_progress)"""
    return _set_discovery_progress(connection_id, **updates)


def _set_discovery_progress(connection_id: int, **updates):
    """Set discovery progress for a connection"""
    try:
        with DISCOVERY_PROGRESS_LOCK:
            # Cleanup old entries (older than TTL) to prevent memory leak
            current_time = datetime.utcnow()
            expired_keys = []
            for key, value in DISCOVERY_PROGRESS.items():
                if isinstance(value, dict) and "updated_at" in value:
                    try:
                        updated_at = datetime.fromisoformat(value["updated_at"].replace('Z', '+00:00'))
                        if (current_time - updated_at.replace(tzinfo=None)).total_seconds() > DISCOVERY_PROGRESS_TTL:
                            expired_keys.append(key)
                    except (ValueError, AttributeError):
                        # If we can't parse the date, consider it expired
                        expired_keys.append(key)
            
            for key in expired_keys:
                DISCOVERY_PROGRESS.pop(key, None)
            
            # Update progress for current connection
            current = DISCOVERY_PROGRESS.get(connection_id, {}) if isinstance(DISCOVERY_PROGRESS.get(connection_id), dict) else {}
            current.update(updates)
            current["updated_at"] = current_time.isoformat()
            DISCOVERY_PROGRESS[connection_id] = current
    except Exception as e:
        # Progress reporting should never break discovery, but log errors for debugging
        logger.warning('FN:_set_discovery_progress connection_id:{} error:{} message:Progress update failed but continuing'.format(connection_id, str(e)))


def try_start_lineage_job(key: str) -> bool:
    """Return True if job can start; False if already running (best-effort)."""
    return _try_start_lineage_job(key)


def _try_start_lineage_job(key: str) -> bool:
    """Return True if job can start; False if already running (best-effort)."""
    now = datetime.utcnow()
    with LINEAGE_JOBS_LOCK:
        # cleanup expired
        expired = []
        for k, v in LINEAGE_JOBS_RUNNING.items():
            try:
                started_at = v.get("started_at")
                if started_at and (now - started_at).total_seconds() > LINEAGE_JOBS_TTL_SECONDS:
                    expired.append(k)
            except Exception:
                expired.append(k)
        for k in expired:
            LINEAGE_JOBS_RUNNING.pop(k, None)

        if key in LINEAGE_JOBS_RUNNING:
            return False
        LINEAGE_JOBS_RUNNING[key] = {"started_at": now}
        return True


def finish_lineage_job(key: str) -> None:
    """Mark a lineage job as finished"""
    return _finish_lineage_job(key)


def _finish_lineage_job(key: str) -> None:
    """Mark a lineage job as finished"""
    with LINEAGE_JOBS_LOCK:
        LINEAGE_JOBS_RUNNING.pop(key, None)

