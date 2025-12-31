import smtplib
import logging
import json
import sys
import os
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from typing import List, Dict
import pymysql

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from config.azure_config import DB_CONFIG, DISCOVERY_CONFIG

logger = logging.getLogger(__name__)


def get_db_connection():
    return pymysql.connect(
        host=DB_CONFIG["host"],
        port=DB_CONFIG["port"],
        user=DB_CONFIG["user"],
        password=DB_CONFIG["password"],
        database=DB_CONFIG["database"],
        cursorclass=pymysql.cursors.DictCursor,
        charset='utf8mb4'
    )


def get_new_discoveries() -> List[Dict]:
    conn = None
    try:
        conn = get_db_connection()
        with conn.cursor() as cursor:
            sql = """
                SELECT id, file_metadata, storage_location, discovered_at, environment, data_source_type,
                       JSON_UNQUOTE(JSON_EXTRACT(storage_location, '$.path')) as storage_path
                FROM data_discovery
                WHERE discovered_at >= DATE_SUB(NOW(), INTERVAL 20 MINUTE)
                  AND notification_sent_at IS NULL
                ORDER BY discovered_at DESC
                UPDATE data_discovery
                SET notification_sent_at = NOW(),
                    notification_recipients = %s
                WHERE id IN ({placeholders})
        <html>
        <body>
            <h2>New Data Discovered</h2>
            <p>Hello Data Governors,</p>
            <p>The following {len(discoveries)} file(s) have been discovered:</p>
            <table border="1" cellpadding="5" cellspacing="0">
                <tr>
                    <th>File Name</th>
                    <th>Path</th>
                    <th>Environment</th>
                    <th>Data Source</th>
                    <th>Discovered At</th>
                    <th>Link</th>
                </tr>
                <tr>
                    <td>{file_name}</td>
                    <td>{storage_path}</td>
                    <td>{environment}</td>
                    <td>{data_source_type}</td>
                    <td>{discovered_at}</td>
                    <td><a href="{discovery_link}">View</a></td>
                </tr>
            </table>
            <p>Please review and approve/reject these discoveries.</p>
            <p>Best regards,<br>Torro Data Discovery System</p>
        </body>
        </html>