#!/usr/bin/env python3
"""
Run database migration to create deduplication_jobs table.
"""

import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from database import engine
from sqlalchemy import text

def run_migration():
    migration_file = os.path.join(
        os.path.dirname(os.path.dirname(os.path.dirname(__file__))),
        'database',
        'migrations',
        'add_deduplication_jobs_table.sql'
    )
    
    if not os.path.exists(migration_file):
        print(f"Error: Migration file not found: {migration_file}")
        return False
    
    with open(migration_file, 'r') as f:
        sql = f.read()
    
    try:
        with engine.begin() as conn:
            # Execute the migration
            conn.execute(text(sql))
        print("✅ Migration successful: deduplication_jobs table created")
        return True
    except Exception as e:
        print(f"❌ Migration failed: {e}")
        import traceback
        traceback.print_exc()
        return False

if __name__ == '__main__':
    success = run_migration()
    sys.exit(0 if success else 1)
