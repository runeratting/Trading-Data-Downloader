import aiohttp
import lzma
from datetime import datetime, timezone, date
from typing import Tuple, Optional, Dict, List, Set
import logging
from pathlib import Path
import asyncio
import multiprocessing
from concurrent.futures import ProcessPoolExecutor

from config import Config
from validation import TickValidator, ValidationStats, ValidationError
from data_processing import TickProcessor, TickBatch

class DownloaderError(Exception):
    """Base class for downloader errors"""
    pass

class DownloadError(DownloaderError):
    """Raised when download fails after retries"""
    pass

class DecompressionError(DownloaderError):
    """Raised when decompression fails"""
    pass

class DukasCopyDownloader:
    URL = "https://datafeed.dukascopy.com/datafeed/{currency}/{year}/{month:02d}/{day:02d}/{hour:02d}h_ticks.bi5"
    TICK_SIZE = 20

    def __init__(self, config: Config):
        self.config = config
        self.headers = {'User-Agent': self.config.downloader.user_agent}
        self.process_pool = ProcessPoolExecutor(
            max_workers=multiprocessing.cpu_count()
        )
        self._prefetch_cache: Dict[Tuple[str, date], List[Optional[bytes]]] = {}
        self.validator = TickValidator(config)
        self.validation_stats: Dict[str, ValidationStats] = {}

    def _decode_tick_data(self, raw_data: bytes, base_timestamp: int, point_value: int) -> TickBatch:
        """
        Process tick data using optimized numpy-based processor
        
        Args:
            raw_data: Raw binary tick data
            base_timestamp: Base timestamp for the hour
            point_value: Point value for price conversion
            
        Returns:
            TickBatch containing decoded tick data
        """
        return TickProcessor.decode_ticks(
            raw_data,
            base_timestamp,
            point_value,
            chunk_size=self.config.buffer.max_batch_size
        )

    async def download_hour(self, instrument: str, day: date, hour: int, session: Optional[aiohttp.ClientSession] = None) -> Optional[bytes]:
        """
        Download and decompress a single hour of tick data with retries
        
        Args:
            instrument: Trading instrument symbol
            day: Date to download
            hour: Hour to download (0-23)
            
        Returns:
            Decompressed tick data or None if no data available
            
        Raises:
            DownloadError: If download fails after retries
            DecompressionError: If decompression fails
        """
        url = self.URL.format(
            currency=instrument,
            year=day.year,
            month=day.month - 1,  # Convert to 0-based month
            day=day.day,
            hour=hour
        )

        retry_delay = self.config.downloader.initial_retry_delay
        
        for attempt in range(self.config.downloader.max_retries):
            try:
                # Use provided session or create new one
                if session is None:
                    async with aiohttp.ClientSession(headers=self.headers) as session:
                        return await self._download_with_session(session, url)
                else:
                    return await self._download_with_session(session, url)
            except (DownloadError, DecompressionError) as e:
                raise  # Re-raise these specific errors
            except Exception as e:
                if attempt < self.config.downloader.max_retries - 1:
                    logging.warning(
                        f"Attempt {attempt + 1}/{self.config.downloader.max_retries} failed for {instrument} on "
                        f"{day} hour {hour:02d}: {str(e)}. Retrying in {retry_delay:.1f} seconds..."
                    )
                    await asyncio.sleep(retry_delay)
                    retry_delay *= 1.5  # Exponential backoff
                else:
                    raise DownloadError(
                        f"Failed to download {instrument} for {day} hour {hour:02d} "
                        f"after {self.config.downloader.max_retries} attempts: {str(e)}"
                    )

    async def _download_with_session(self, session: aiohttp.ClientSession, url: str) -> Optional[bytes]:
        """Helper method to handle download with a session"""
        async with session.get(url) as response:
            if response.status == 200:
                compressed_data = await response.read()
                try:
                    return lzma.decompress(compressed_data)
                except lzma.LZMAError as e:
                    raise DecompressionError(f"Failed to decompress data: {str(e)}")
            elif response.status == 404:
                return None
            else:
                raise DownloadError(f"HTTP {response.status}")

    async def prefetch_day(self, instrument: str, day: date) -> List[Optional[bytes]]:
        """
        Prefetch all hours for a day
        
        Args:
            instrument: Trading instrument symbol
            day: Date to prefetch
            
        Returns:
            List of decompressed tick data or None for each hour
        """
        cache_key = (instrument, day)
        if cache_key not in self._prefetch_cache:
            tasks = []
            for hour in range(24):
                tasks.append(self.download_hour(instrument, day, hour))
            self._prefetch_cache[cache_key] = await asyncio.gather(*tasks, return_exceptions=True)
            
            # Clean up old cache entries if cache gets too large
            if len(self._prefetch_cache) > self.config.downloader.cache_days:
                oldest_key = next(iter(self._prefetch_cache))
                del self._prefetch_cache[oldest_key]
                
        return self._prefetch_cache[cache_key]

    def decode_ticks(self, raw_data: bytes, day: date, hour: int, instrument: str) -> List[Tuple]:
        """
        Decode and validate binary tick data using parallel processing
        
        Args:
            raw_data: Raw binary tick data
            day: Date of the data
            hour: Hour of the data
            instrument: Trading instrument symbol
            
        Returns:
            List of valid decoded ticks as tuples (timestamp, bid, ask, bid_volume, ask_volume)
            
        Raises:
            ValidationError: If validation is enabled and all ticks are invalid
        """
        if not raw_data:
            return []
            
        base_timestamp = int(datetime(
            day.year, day.month, day.day, hour, 0, 0, tzinfo=timezone.utc
        ).timestamp() * 1000)
        
        point_value = self.config.instruments[instrument].point_value
        
        # Use optimized processor for decoding
        tick_batch = self._decode_tick_data(raw_data, base_timestamp, point_value)
        
        # Calculate statistics
        stats = TickProcessor.calculate_statistics(tick_batch)
        logging.info(
            f"Processed {stats['count']} ticks for {instrument} on {day} hour {hour:02d}",
            extra={
                'instrument': instrument,
                'day': day,
                'hour': hour,
                'stats': stats
            }
        )
        
        # Find anomalies
        anomalies = TickProcessor.find_anomalies(tick_batch, {
            'max_spread_pct': self.config.validation.max_price_spread_percent / 100,
            'max_volume': self.config.validation.max_volume,
            'max_tick_interval': self.config.validation.max_tick_interval * 1000  # Convert to ms
        })
        
        if anomalies['total_anomalies'] > 0:
            logging.warning(
                f"Found {anomalies['total_anomalies']} anomalies in {instrument} data",
                extra={
                    'instrument': instrument,
                    'day': day,
                    'hour': hour,
                    'anomalies': anomalies
                }
            )
        
        decoded_ticks = tick_batch.to_tuples()
        
        # Validate ticks
        validation_stats = self.validator.validate_ticks(decoded_ticks, instrument)
        self.validation_stats[instrument] = validation_stats
        
        if validation_stats.invalid_ticks > 0:
            logging.warning(
                f"Found {validation_stats.invalid_ticks} invalid ticks out of {validation_stats.total_ticks} "
                f"for {instrument} on {day} hour {hour:02d}",
                extra={
                    'instrument': instrument,
                    'day': day,
                    'hour': hour,
                    'error_counts': validation_stats.error_counts
                }
            )
            
            if (self.config.validation.reject_invalid_ticks and 
                validation_stats.valid_ticks == 0):
                raise ValidationError(
                    f"All ticks ({validation_stats.total_ticks}) were invalid "
                    f"for {instrument} on {day} hour {hour:02d}"
                )
        
        # Return only valid ticks if rejection is enabled
        if self.config.validation.reject_invalid_ticks:
            return [
                tick for tick in decoded_ticks
                if self.validator.validate_tick(tick, instrument).is_valid
            ]
            
        return decoded_ticks

    def get_prefetched_hour(self, instrument: str, day: date, hour: int) -> Optional[bytes]:
        """
        Get prefetched data for a specific hour
        
        Args:
            instrument: Trading instrument symbol
            day: Date to get data for
            hour: Hour to get data for
            
        Returns:
            Decompressed tick data or None if not available
        """
        cache_key = (instrument, day)
        if cache_key in self._prefetch_cache:
            try:
                return self._prefetch_cache[cache_key][hour]
            except (IndexError, TypeError):
                return None
        return None

    def cleanup(self):
        """Cleanup resources"""
        self.process_pool.shutdown()
        self._prefetch_cache.clear()
        self.validation_stats.clear()

    def clear_cache(self, instrument: str, day: date):
        """
        Clear specific cache entry
        
        Args:
            instrument: Trading instrument symbol
            day: Date to clear from cache
        """
        self._prefetch_cache.pop((instrument, day), None)
