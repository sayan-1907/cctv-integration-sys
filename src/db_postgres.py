import psycopg2
import psycopg2.extras
import os
import logging

logger = logging.getLogger("layer4.db")

def get_pg_connection():
    """
    Connect to the PostGIS container defined in docker-compose.yml.
    """
    try:
        conn = psycopg2.connect(
            host=os.environ.get("POSTGRES_HOST", "localhost"),
            port=os.environ.get("POSTGRES_PORT", "5432"),
            database=os.environ.get("POSTGRES_DB", "sentinel"),
            user=os.environ.get("POSTGRES_USER", "sentinel"),
            password=os.environ.get("POSTGRES_PASSWORD", "sentinel_dev")
        )
        return conn
    except Exception as e:
        logger.error(f"Failed to connect to PostGIS: {e}")
        raise e
