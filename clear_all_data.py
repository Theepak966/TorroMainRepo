#!/usr/bin/env python3
"""
Script to clear all connections and discovered assets from the database.
WARNING: This will delete ALL data - use with caution!
"""

import sys
import os

# Add backend to path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), 'backend'))

from database import SessionLocal
from models import Asset, Connection, LineageRelationship, DataDiscovery, LineageHistory, SQLQuery

def clear_all_data():
    """Clear all connections, assets, and related data from the database."""
    db = SessionLocal()
    try:
        print("=" * 70)
        print("WARNING: This will delete ALL connections and assets!")
        print("=" * 70)
        
        # Count records before deletion
        asset_count = db.query(Asset).count()
        connection_count = db.query(Connection).count()
        lineage_count = db.query(LineageRelationship).count()
        discovery_count = db.query(DataDiscovery).count()
        history_count = db.query(LineageHistory).count()
        sql_query_count = db.query(SQLQuery).count()
        
        print(f"\nCurrent database state:")
        print(f"  - Assets: {asset_count}")
        print(f"  - Connections: {connection_count}")
        print(f"  - Lineage Relationships: {lineage_count}")
        print(f"  - Data Discovery Records: {discovery_count}")
        print(f"  - Lineage History: {history_count}")
        print(f"  - SQL Queries: {sql_query_count}")
        
        # Confirm deletion
        response = input("\nAre you sure you want to delete ALL data? (yes/no): ")
        if response.lower() != 'yes':
            print("Deletion cancelled.")
            return
        
        print("\nStarting deletion...")
        
        # Delete in order to respect foreign key constraints
        # 1. Delete lineage history (references lineage_relationships)
        deleted_history = db.query(LineageHistory).delete()
        print(f"  ✓ Deleted {deleted_history} lineage history records")
        
        # 2. Delete lineage relationships (references assets)
        deleted_lineage = db.query(LineageRelationship).delete()
        print(f"  ✓ Deleted {deleted_lineage} lineage relationships")
        
        # 3. Delete data discovery records (references assets)
        deleted_discovery = db.query(DataDiscovery).delete()
        print(f"  ✓ Deleted {deleted_discovery} data discovery records")
        
        # 4. Delete SQL queries (references assets)
        deleted_sql = db.query(SQLQuery).delete()
        print(f"  ✓ Deleted {deleted_sql} SQL query records")
        
        # 5. Delete all assets
        deleted_assets = db.query(Asset).delete()
        print(f"  ✓ Deleted {deleted_assets} assets")
        
        # 6. Delete all connections
        deleted_connections = db.query(Connection).delete()
        print(f"  ✓ Deleted {deleted_connections} connections")
        
        # Commit all deletions
        db.commit()
        
        print("\n" + "=" * 70)
        print("SUCCESS: All data has been cleared!")
        print("=" * 70)
        print(f"\nSummary:")
        print(f"  - Assets deleted: {deleted_assets}")
        print(f"  - Connections deleted: {deleted_connections}")
        print(f"  - Lineage relationships deleted: {deleted_lineage}")
        print(f"  - Data discovery records deleted: {deleted_discovery}")
        print(f"  - Lineage history deleted: {deleted_history}")
        print(f"  - SQL queries deleted: {deleted_sql}")
        
    except Exception as e:
        db.rollback()
        print(f"\nERROR: Failed to clear data: {str(e)}")
        import traceback
        traceback.print_exc()
        sys.exit(1)
    finally:
        db.close()

if __name__ == "__main__":
    clear_all_data()

