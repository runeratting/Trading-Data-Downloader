# db_handler.py
import psycopg2
from datetime import datetime, timezone, date
from typing import List, Tuple, Dict, Optional
import logging
from pathlib import Path
from io import StringIO
import asyncpg
import asyncio

class TickDBHandler:
    def __init__(self, config: dict):
        self.config = config
        self._connection = None
        self.pool = None
        
        # Buffer setup
        self.buffers: Dict[str, List[Tuple]] = {}  # Separate buffer for each instrument
        self.buffer_size = 100000  # Flush after this many ticks
        
        # Logging setup
        self.data_logger = logging.getLogger('data_issues')
        self.data_logger.setLevel(logging.ERROR)
        Path('logs').mkdir(exist_ok=True)
        handler = logging.FileHandler('logs/data_issues.log')
        handler.setFormatter(logging.Formatter('%(asctime)s - %(message)s'))
        self.data_logger.addHandler(handler)

    @property
    def connection(self):
        """Get or create a database connection"""
        if self._connection is None or self._connection.closed:
            try:
                self._connection = psycopg2.connect(**self.config['DB_CONFIG'])
                self._connection.autocommit = False  # Ensure we're in transaction mode
            except Exception as e:
                logging.error(f"Failed to create database connection: {str(e)}")
                raise
        return self._connection

    async def connect(self):
        """Initialize database connection pool"""
        if self.pool is None:
            self.pool = await asyncpg.create_pool(
                host=self.config['DB_CONFIG']['host'],
                port=self.config['DB_CONFIG']['port'],
                user=self.config['DB_CONFIG']['user'],
                password=self.config['DB_CONFIG']['password'],
                database=self.config['DB_CONFIG']['database']
            )

    def close(self):
        """Flush buffers and close connection"""
        try:
            # Flush all buffers before closing
            for instrument in list(self.buffers.keys()):
                if self.buffers[instrument]:
                    self.flush_buffer(instrument)
        except Exception as e:
            logging.error(f"Error during close cleanup: {str(e)}")
        finally:
            if self._connection is not None and not self._connection.closed:
                try:
                    self._connection.close()
                except Exception as e:
                    logging.error(f"Error closing connection: {str(e)}")
                self._connection = None

    def get_table_name(self, instrument: str) -> str:
        return self.config['INSTRUMENTS'][instrument]['table_name']

    async def flush_buffer(self, instrument: str) -> int:
        """Flush buffer for an instrument using text COPY"""
        if not self.buffers.get(instrument):
            return 0
            
        table_name = self.get_table_name(instrument)
        ticks = self.buffers[instrument]
        
        try:
            with self.connection.cursor() as cur:
                with StringIO() as buffer:
                    # Write ticks in tab-separated format
                    for timestamp, bid, ask, bid_volume, ask_volume in ticks:
                        dt = datetime.fromtimestamp(timestamp/1000, tz=timezone.utc)
                        buffer.write(f"{dt}\t{bid:.3f}\t{ask:.3f}\t{bid_volume}\t{ask_volume}\n")
                    
                    buffer.seek(0)
                    cur.copy_from(
                        buffer,
                        table_name,
                        columns=('timestamp', 'bid', 'ask', 'bid_volume', 'ask_volume'),
                        sep='\t'
                    )
                
            self.connection.commit()
            count = len(ticks)
            self.buffers[instrument] = []
            return count
            
        except Exception as e:
            logging.error(f"Failed to flush buffer for {instrument}: {str(e)}")
            try:
                self.connection.rollback()
            except Exception as rollback_error:
                logging.error(f"Error during rollback: {str(rollback_error)}")
            self._connection = None  # Force new connection on next attempt
            raise

    async def insert_ticks(self, instrument: str, day_ticks: List[Tuple]) -> int:
        """Buffer ticks and flush when buffer is full"""
        try:
            if instrument not in self.buffers:
                self.buffers[instrument] = []
                
            self.buffers[instrument].extend(day_ticks)
            
            if len(self.buffers[instrument]) >= self.buffer_size:
                return await self.flush_buffer(instrument)
                
            return 0
        except Exception as e:
            logging.error(f"Failed to insert ticks for {instrument}: {str(e)}")
            try:
                self.connection.rollback()
            except Exception as rollback_error:
                logging.error(f"Error during rollback: {str(rollback_error)}")
            self._connection = None  # Force new connection on next attempt
            raise

    async def get_latest_timestamp(self, instrument: str) -> datetime:
        """Get the latest timestamp for an instrument"""
        table_name = self.get_table_name(instrument)
        
        try:
            with self.connection.cursor() as cur:
                cur.execute(f"""
                    SELECT MAX(timestamp)
                    FROM {table_name}
                """)
                result = cur.fetchone()
                self.connection.commit()
                return result[0] if result and result[0] else None

        except Exception as e:
            logging.error(f"Failed to get latest timestamp for {instrument}: {str(e)}")
            try:
                self.connection.rollback()
            except Exception as rollback_error:
                logging.error(f"Error during rollback: {str(rollback_error)}")
            self._connection = None  # Force new connection on next attempt
            raise

    async def get_hour_tick_counts(self, instrument: str, day: date) -> Dict[int, int]:
        """
        Returns a dictionary of {hour: tick_count} for the given instrument and day
        """
        await self.connect()  # Ensure we have a connection
        query = """
            SELECT EXTRACT(HOUR FROM timestamp) as hour, COUNT(*) as tick_count
            FROM ticks
            WHERE instrument = $1
            AND DATE(timestamp) = $2
            GROUP BY EXTRACT(HOUR FROM timestamp)
        """
        results = await self.pool.fetch(query, instrument, day)
        return {int(r['hour']): r['tick_count'] for r in results}

    async def find_missing_hours(self, instrument: str, day: date, trading_hours_func) -> List[int]:
        """Returns a list of hours that have no data in the database for given instrument and day"""
        # Get count of ticks for each hour of the day
        hour_counts = await self.get_hour_tick_counts(instrument, day)
        
        # Check which hours during trading time have no data
        missing_hours = []
        for hour in range(24):
            if not trading_hours_func(day.weekday(), hour):
                continue
            if hour not in hour_counts or hour_counts[hour] == 0:
                missing_hours.append(hour)
        
        return missing_hours