# downloader.py
import aiohttp
import lzma
from datetime import datetime, timezone, date
from typing import Tuple, Optional, Dict, List
import struct
import logging
from pathlib import Path
import asyncio
import multiprocessing
from concurrent.futures import ProcessPoolExecutor

class DukasCopyDownloader:
    URL = "https://datafeed.dukascopy.com/datafeed/{currency}/{year}/{month:02d}/{day:02d}/{hour:02d}h_ticks.bi5"
    TICK_SIZE = 20
    MAX_RETRIES = 3
    INITIAL_RETRY_DELAY = 2

    def __init__(self, config):
        self.config = config
        self.headers = {'User-Agent': 'Python'}
        self.process_pool = ProcessPoolExecutor(max_workers=multiprocessing.cpu_count())
        self._prefetch_cache: Dict[Tuple[str, date], List[Optional[bytes]]] = {}

    @staticmethod
    def _decode_tick_data(raw_data: bytes, base_timestamp: int, point_value: int) -> list[Tuple]:
        """Static method for parallel processing of tick data"""
        ticks = []
        for i in range(0, len(raw_data), 20):
            tick_data = raw_data[i:i + 20]
            if len(tick_data) == 20:
                ms_offset, bid_raw, ask_raw, vol_bid, vol_ask = struct.unpack('!IIIff', tick_data)
                timestamp = base_timestamp + ms_offset
                bid = float(bid_raw) / point_value
                ask = float(ask_raw) / point_value
                bid_volume = round(float(vol_bid) * 1_000_000)
                ask_volume = round(float(vol_ask) * 1_000_000)
                ticks.append((timestamp, bid, ask, bid_volume, ask_volume))
        return ticks

    async def download_hour(self, instrument: str, day: date, hour: int) -> Optional[bytes]:
        """Download and decompress a single hour of tick data with retries"""
        url = self.URL.format(
            currency=instrument,
            year=day.year,
            month=day.month - 1,  # Convert to 0-based month
            day=day.day,
            hour=hour
        )

        retry_delay = self.INITIAL_RETRY_DELAY
        
        for attempt in range(self.MAX_RETRIES):
            try:
                async with aiohttp.ClientSession(headers=self.headers) as session:
                    async with session.get(url) as response:
                        if response.status == 200:
                            compressed_data = await response.read()
                            return lzma.decompress(compressed_data)
                        elif response.status == 404:
                            logging.info(f"No data available for {instrument} on {day} hour {hour:02d} (404)")
                            return None
                        else:
                            raise Exception(f"HTTP {response.status}")
            except Exception as e:
                if attempt < self.MAX_RETRIES - 1:
                    logging.warning(
                        f"Attempt {attempt + 1}/{self.MAX_RETRIES} failed for {instrument} on "
                        f"{day} hour {hour:02d}: {str(e)}. Retrying in {retry_delay:.1f} seconds..."
                    )
                    await asyncio.sleep(retry_delay)
                    retry_delay *= 1.5  # Exponential backoff
                else:
                    logging.error(
                        f"Failed to download {instrument} for {day} hour {hour:02d} "
                        f"after {self.MAX_RETRIES} attempts: {str(e)}"
                    )
                    raise

    async def prefetch_day(self, instrument: str, day: date) -> List[Optional[bytes]]:
        """Prefetch all hours for a day"""
        cache_key = (instrument, day)
        if cache_key not in self._prefetch_cache:
            tasks = []
            for hour in range(24):
                tasks.append(self.download_hour(instrument, day, hour))
            self._prefetch_cache[cache_key] = await asyncio.gather(*tasks, return_exceptions=True)
            
            # Clean up old cache entries if cache gets too large
            if len(self._prefetch_cache) > 10:  # Keep only last 10 days
                oldest_key = next(iter(self._prefetch_cache))
                del self._prefetch_cache[oldest_key]
                
        return self._prefetch_cache[cache_key]

    def decode_ticks(self, raw_data: bytes, day: date, hour: int, instrument: str) -> list[Tuple]:
        """Decode binary tick data using parallel processing"""
        if not raw_data:
            return []
            
        base_timestamp = int(datetime(
            day.year, day.month, day.day, hour, 0, 0, tzinfo=timezone.utc
        ).timestamp() * 1000)
        
        point_value = self.config['INSTRUMENTS'][instrument]['point_value']
        
        # Use process pool for parallel processing
        future = self.process_pool.submit(
            self._decode_tick_data,
            raw_data,
            base_timestamp,
            point_value
        )
        return future.result()

    def get_prefetched_hour(self, instrument: str, day: date, hour: int) -> Optional[bytes]:
        """Get prefetched data for a specific hour"""
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

    def clear_cache(self, instrument: str, day: date):
        """Clear specific cache entry"""
        self._prefetch_cache.pop((instrument, day), None)