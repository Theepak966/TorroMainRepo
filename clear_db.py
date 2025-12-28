#!/usr/bin/env python3
"""Clear all application tables from the database"""
import pymysql
import os
from dotenv import load_dotenv

load_dotenv()

db_config = {
    'host': os.getenv('DB_HOST', 'localhost'),
    'port': int(os.getenv('DB_PORT', 3306)),
    'user': os.getenv('DB_USER', 'root'),
    'password': os.getenv('DB_PASSWORD', 'rootpassword'),
    'database': os.getenv('DB_NAME', 'torroforexcel')
}

conn = pymysql.connect(**db_config)
cursor = conn.cursor()

try:
    print("Clearing database tables...")
    cursor.execute("SET FOREIGN_KEY_CHECKS = 0")
    cursor.execute("TRUNCATE TABLE lineage_relationships")
    cursor.execute("TRUNCATE TABLE sql_queries")
    cursor.execute("TRUNCATE TABLE data_discovery")
    cursor.execute("TRUNCATE TABLE assets")
    cursor.execute("TRUNCATE TABLE connections")
    cursor.execute("SET FOREIGN_KEY_CHECKS = 1")
    conn.commit()
    
    # Verify
    cursor.execute("SELECT COUNT(*) FROM assets")
    asset_count = cursor.fetchone()[0]
    cursor.execute("SELECT COUNT(*) FROM connections")
    conn_count = cursor.fetchone()[0]
    cursor.execute("SELECT COUNT(*) FROM data_discovery")
    disc_count = cursor.fetchone()[0]
    
    print("✅ Database cleared successfully")
    print(f"✅ Verification: Assets={asset_count}, Connections={conn_count}, Discoveries={disc_count}")
except Exception as e:
    conn.rollback()
    print(f"❌ Error: {e}")
finally:
    cursor.close()
    conn.close()

