# db_handler.py
import psycopg2
from datetime import datetime, timezone, date, timedelta
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
        
        # Get the correct table name for this instrument
        table_name = self.config['INSTRUMENTS'][instrument]['table_name']
        
        query = """
            SELECT EXTRACT(HOUR FROM timestamp) as hour, COUNT(*) as tick_count
            FROM {}
            WHERE DATE(timestamp) = $1
            GROUP BY EXTRACT(HOUR FROM timestamp)
        """.format(table_name)
        
        results = await self.pool.fetch(query, day)
        return {int(r['hour']): r['tick_count'] for r in results}

    async def get_missing_hours_batch(self, instrument: str, start_date: date, end_date: date) -> Dict[date, List[int]]:
        """
        Returns a dictionary of {date: [missing_hours]} for the given date range
        Much more efficient than checking one day at a time
        """
        await self.connect()
        
        table_name = self.config['INSTRUMENTS'][instrument]['table_name']
        trading_hours_func = self.config['INSTRUMENTS'][instrument]['trading_hours']
        
        # First, get all hours that have data
        query = """
            SELECT 
                DATE(timestamp) as day,
                EXTRACT(HOUR FROM timestamp) as hour,
                COUNT(*) as tick_count
            FROM {}
            WHERE DATE(timestamp) BETWEEN $1 AND $2
            GROUP BY DATE(timestamp), EXTRACT(HOUR FROM timestamp)
        """.format(table_name)
        
        results = await self.pool.fetch(query, start_date, end_date)
        
        # Convert results to {date: {hour: count}} format
        existing_data = {}
        for r in results:
            day = r['day']
            hour = int(r['hour'])
            if day not in existing_data:
                existing_data[day] = {}
            existing_data[day][hour] = r['tick_count']
        
        # Find missing hours for each day
        missing_hours = {}
        current_date = start_date
        while current_date <= end_date:
            day_missing = []
            for hour in range(24):
                if not trading_hours_func(current_date.weekday(), hour):
                    continue
                
                # Check if hour is missing or has zero ticks
                if (current_date not in existing_data or 
                    hour not in existing_data[current_date] or 
                    existing_data[current_date][hour] == 0):
                    day_missing.append(hour)
            
            if day_missing:  # Only include days that have missing hours
                missing_hours[current_date] = day_missing
            
            current_date += timedelta(days=1)
        
        return missing_hours

    async def find_missing_hours(self, instrument: str, day: date, trading_hours_func) -> List[int]:
        """Legacy method for compatibility"""
        missing_dict = await self.get_missing_hours_batch(instrument, day, day)
        return missing_dict.get(day, [])