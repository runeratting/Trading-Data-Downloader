"""
Test script to verify database connection
"""
import asyncio
import logging
from config import load_config
from db_handler import TickDBHandler

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)

async def test_connection():
    try:
        # Load configuration
        config = load_config('config')
        db_handler = TickDBHandler(config)
        
        logging.info(f"Attempting to connect to database at {config.db.host}...")
        
        # Test connection
        await db_handler.connect()
        
        # Try a simple query
        async with db_handler.pool.acquire() as conn:
            version = await conn.fetchval('SELECT version()')
            logging.info(f"Successfully connected to PostgreSQL server:")
            logging.info(f"Server version: {version}")
            
            # Test table access
            for instrument in config.instruments:
                table_name = db_handler.get_table_name(instrument)
                try:
                    count = await conn.fetchval(f'SELECT COUNT(*) FROM {table_name}')
                    logging.info(f"Table {table_name} contains {count:,} rows")
                except Exception as e:
                    logging.error(f"Failed to query table {table_name}: {str(e)}")
        
        logging.info("Database connection test completed successfully")
        
    except Exception as e:
        logging.error(f"Connection test failed: {str(e)}")
        raise
    finally:
        if 'db_handler' in locals():
            db_handler.close()

if __name__ == "__main__":
    asyncio.run(test_connection())
