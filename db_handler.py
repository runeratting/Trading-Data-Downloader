# db_handler.py
import psycopg2
from psycopg2.extras import execute_batch
from datetime import datetime
from typing import List, Tuple
import logging
from pathlib import Path

class TickDBHandler:
    def __init__(self, config: dict):
        """
        Initialize with complete config containing both DB_CONFIG and INSTRUMENTS
        """
        self.config = config
        self.batch_size = 10000
        
        # Setup specific logger for data issues
        self.data_logger = logging.getLogger('data_issues')
        self.data_logger.setLevel(logging.ERROR)
        Path('logs').mkdir(exist_ok=True)
        handler = logging.FileHandler('logs/data_issues.log')
        handler.setFormatter(logging.Formatter('%(asctime)s - %(message)s'))
        self.data_logger.addHandler(handler)

    def get_connection(self):
        return psycopg2.connect(**self.config['DB_CONFIG'])

    def get_table_name(self, instrument: str) -> str:
        return self.config['INSTRUMENTS'][instrument]['table_name']

    async def insert_ticks(self, instrument: str, day_ticks: List[Tuple]) -> int:
        """
        Insert tick data into database.
        If exact duplicate found - skip
        If timestamp exists but data differs - log error and skip
        """
        table_name = self.get_table_name(instrument)
        
        try:
            with self.get_connection() as conn:
                with conn.cursor() as cur:
                    execute_batch(cur,
                        f"""
                        INSERT INTO {table_name} 
                        (timestamp, bid, ask, bid_volume, ask_volume)
                        VALUES (%s, %s, %s, %s, %s)
                        ON CONFLICT (timestamp) DO UPDATE
                        SET bid = EXCLUDED.bid,
                            ask = EXCLUDED.ask,
                            bid_volume = EXCLUDED.bid_volume,
                            ask_volume = EXCLUDED.ask_volume
                        WHERE {table_name}.bid != EXCLUDED.bid 
                           OR {table_name}.ask != EXCLUDED.ask
                           OR {table_name}.bid_volume != EXCLUDED.bid_volume
                           OR {table_name}.ask_volume != EXCLUDED.ask_volume
                        RETURNING 
                            timestamp,
                            bid <> EXCLUDED.bid as bid_diff,
                            ask <> EXCLUDED.ask as ask_diff,
                            bid_volume <> EXCLUDED.bid_volume as bidvol_diff,
                            ask_volume <> EXCLUDED.ask_volume as askvol_diff
                        """,
                        day_ticks,
                        page_size=self.batch_size
                    )
                    
                    # Check if any rows were returned (indicating conflicts with different values)
                    conflicts = cur.fetchall()
                    if conflicts:
                        for conflict in conflicts:
                            timestamp, bid_diff, ask_diff, bidvol_diff, askvol_diff = conflict
                            self.data_logger.error(
                                f"Data mismatch for {instrument} at {timestamp}: "
                                f"bid_different:{bid_diff}, "
                                f"ask_different:{ask_diff}, "
                                f"bid_volume_different:{bidvol_diff}, "
                                f"ask_volume_different:{askvol_diff}"
                            )
                    
                    return len(day_ticks)

        except Exception as e:
            logging.error(f"Failed to insert ticks for {instrument}: {str(e)}")
            raise

    async def get_latest_timestamp(self, instrument: str) -> datetime:
        """Get the latest timestamp for an instrument"""
        table_name = self.get_table_name(instrument)
        
        try:
            with self.get_connection() as conn:
                with conn.cursor() as cur:
                    cur.execute(f"""
                        SELECT MAX(timestamp)
                        FROM {table_name}
                    """)
                    result = cur.fetchone()
                    return result[0] if result and result[0] else None

        except Exception as e:
            logging.error(f"Failed to get latest timestamp for {instrument}: {str(e)}")
            raise