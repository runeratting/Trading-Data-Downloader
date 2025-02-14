import asyncio
import asyncpg
from datetime import datetime, timezone, date, timedelta
from typing import List, Tuple, Dict, Optional, Set
import logging
from pathlib import Path
from io import StringIO

from config import Config

class DatabaseError(Exception):
    """Base class for database errors"""
    pass

class ConnectionError(DatabaseError):
    """Raised when database connection fails"""
    pass

class QueryError(DatabaseError):
    """Raised when a database query fails"""
    pass

class TickDBHandler:
    def __init__(self, config: Config):
        self.config = config
        self.pool: Optional[asyncpg.Pool] = None
        
        # Buffer setup
        self.buffers: Dict[str, List[Tuple]] = {}  # Separate buffer for each instrument
        
        # Setup data issues logger
        self.data_logger = logging.getLogger('data_issues')
        self.data_logger.setLevel(logging.ERROR)
        
        # Ensure logs directory exists
        self.config.logging.log_dir.mkdir(exist_ok=True)
        
        # Setup data issues log handler
        handler = logging.FileHandler(
            self.config.logging.log_dir / 'data_issues.log'
        )
        handler.setFormatter(logging.Formatter(self.config.logging.format))
        self.data_logger.addHandler(handler)

    async def connect(self):
        """Initialize database connection pool"""
        if self.pool is None:
            try:
                self.pool = await asyncpg.create_pool(
                    host=self.config.db.host,
                    port=self.config.db.port,
                    user=self.config.db.user,
                    password=self.config.db.password,
                    database=self.config.db.database
                )
            except Exception as e:
                raise ConnectionError(f"Failed to create connection pool: {str(e)}")

    def close(self):
        """Close all connections and cleanup resources"""
        try:
            # Flush all buffers before closing
            for instrument in list(self.buffers.keys()):
                if self.buffers[instrument]:
                    asyncio.create_task(self.flush_buffer(instrument))
        except Exception as e:
            logging.error(f"Error during close cleanup: {str(e)}")
        finally:
            if self.pool is not None:
                asyncio.create_task(self.pool.close())

    def get_table_name(self, instrument: str) -> str:
        """Get table name for instrument"""
        return self.config.instruments[instrument].table_name

    async def flush_buffer(self, instrument: str) -> int:
        """
        Flush buffer for an instrument using COPY
        
        Args:
            instrument: Trading instrument symbol
            
        Returns:
            Number of ticks inserted
            
        Raises:
            QueryError: If buffer flush fails
        """
        if not self.buffers.get(instrument):
            return 0
            
        table_name = self.get_table_name(instrument)
        ticks = self.buffers[instrument]
        
        try:
            async with self.pool.acquire() as conn:
                async with conn.transaction():
                    # Prepare COPY statement
                    copy_stmt = f"""
                        COPY {table_name} (timestamp, bid, ask, bid_volume, ask_volume)
                        FROM STDIN WITH (FORMAT text, DELIMITER '\t')
                    """
                    
                    # Format data for COPY
                    data = StringIO()
                    for timestamp, bid, ask, bid_volume, ask_volume in ticks:
                        dt = datetime.fromtimestamp(timestamp/1000, tz=timezone.utc)
                        data.write(f"{dt}\t{bid:.3f}\t{ask:.3f}\t{bid_volume}\t{ask_volume}\n")
                    
                    data.seek(0)
                    await conn.copy_to_table(
                        table_name,
                        source=data,
                        columns=['timestamp', 'bid', 'ask', 'bid_volume', 'ask_volume'],
                        format='text',
                        delimiter='\t'
                    )
            
            count = len(ticks)
            self.buffers[instrument] = []
            return count
            
        except Exception as e:
            raise QueryError(f"Failed to flush buffer for {instrument}: {str(e)}")

    async def insert_ticks(self, instrument: str, day_ticks: List[Tuple]) -> int:
        """
        Buffer ticks and flush when buffer is full
        
        Args:
            instrument: Trading instrument symbol
            day_ticks: List of tick tuples to insert
            
        Returns:
            Number of ticks inserted if buffer was flushed, 0 otherwise
            
        Raises:
            QueryError: If tick insertion fails
        """
        try:
            if instrument not in self.buffers:
                self.buffers[instrument] = []
                
            self.buffers[instrument].extend(day_ticks)
            
            if len(self.buffers[instrument]) >= self.config.buffer.buffer_size:
                return await self.flush_buffer(instrument)
                
            return 0
            
        except Exception as e:
            raise QueryError(f"Failed to insert ticks for {instrument}: {str(e)}")

    async def get_latest_timestamp(self, instrument: str) -> Optional[datetime]:
        """
        Get the latest timestamp for an instrument
        
        Args:
            instrument: Trading instrument symbol
            
        Returns:
            Latest timestamp or None if no data exists
            
        Raises:
            QueryError: If query fails
        """
        await self.connect()
        table_name = self.get_table_name(instrument)
        
        try:
            async with self.pool.acquire() as conn:
                result = await conn.fetchval(f"""
                    SELECT MAX(timestamp)
                    FROM {table_name}
                """)
                return result
                
        except Exception as e:
            raise QueryError(f"Failed to get latest timestamp for {instrument}: {str(e)}")

    async def get_hour_tick_counts(self, instrument: str, day: date) -> Dict[int, int]:
        """
        Get tick counts for each hour of a day
        
        Args:
            instrument: Trading instrument symbol
            day: Date to get counts for
            
        Returns:
            Dictionary of {hour: tick_count}
            
        Raises:
            QueryError: If query fails
        """
        await self.connect()
        table_name = self.get_table_name(instrument)
        
        try:
            async with self.pool.acquire() as conn:
                rows = await conn.fetch("""
                    SELECT EXTRACT(HOUR FROM timestamp) as hour, COUNT(*) as tick_count
                    FROM {}
                    WHERE DATE(timestamp) = $1
                    GROUP BY EXTRACT(HOUR FROM timestamp)
                """.format(table_name), day)
                
                return {int(r['hour']): r['tick_count'] for r in rows}
                
        except Exception as e:
            raise QueryError(f"Failed to get hour tick counts for {instrument}: {str(e)}")

    async def get_missing_hours_batch(self, instrument: str, start_date: date, end_date: date) -> Dict[date, List[int]]:
        """
        Get missing hours for a date range
        
        Args:
            instrument: Trading instrument symbol
            start_date: Start of date range
            end_date: End of date range
            
        Returns:
            Dictionary of {date: [missing_hours]}
            
        Raises:
            QueryError: If query fails
        """
        await self.connect()
        
        table_name = self.get_table_name(instrument)
        trading_hours_func = self.config.instruments[instrument].trading_hours
        
        try:
            # Get all hours that have data
            async with self.pool.acquire() as conn:
                rows = await conn.fetch("""
                    SELECT 
                        DATE(timestamp) as day,
                        EXTRACT(HOUR FROM timestamp) as hour,
                        COUNT(*) as tick_count
                    FROM {}
                    WHERE DATE(timestamp) BETWEEN $1 AND $2
                    GROUP BY DATE(timestamp), EXTRACT(HOUR FROM timestamp)
                """.format(table_name), start_date, end_date)
            
            # Convert results to {date: {hour: count}} format
            existing_data: Dict[date, Dict[int, int]] = {}
            for r in rows:
                day = r['day']
                hour = int(r['hour'])
                if day not in existing_data:
                    existing_data[day] = {}
                existing_data[day][hour] = r['tick_count']
            
            # Find missing hours for each day
            missing_hours: Dict[date, List[int]] = {}
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
            
        except Exception as e:
            raise QueryError(f"Failed to get missing hours for {instrument}: {str(e)}")

    async def find_missing_hours(self, instrument: str, day: date, trading_hours_func) -> List[int]:
        """Legacy method for compatibility"""
        missing_dict = await self.get_missing_hours_batch(instrument, day, day)
        return missing_dict.get(day, [])
