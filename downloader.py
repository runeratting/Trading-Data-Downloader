import aiohttp
import lzma
from datetime import date, timezone
from typing import Tuple, Optional
import struct
import logging
from pathlib import Path
import asyncio

class DukasCopyDownloader:
    URL = "https://datafeed.dukascopy.com/datafeed/{currency}/{year}/{month:02d}/{day:02d}/{hour:02d}h_ticks.bi5"
    TICK_SIZE = 20
    MAX_RETRIES = 5
    INITIAL_RETRY_DELAY = 2  # seconds

    def __init__(self, config):
        self.config = config
        self.headers = {'User-Agent': 'Python'}
        
        # Setup specific logger for missing data
        self.missing_logger = logging.getLogger('missing_data')
        self.missing_logger.setLevel(logging.INFO)
        
        # Create logs directory if it doesn't exist
        Path('logs').mkdir(exist_ok=True)
        
        # Add file handler for missing data
        missing_handler = logging.FileHandler('logs/missing_data.log')
        missing_handler.setFormatter(
            logging.Formatter('%(asctime)s - %(message)s')
        )
        self.missing_logger.addHandler(missing_handler)

    async def download_hour(self, instrument: str, day: date, hour: int) -> Optional[bytes]:
        """Download and decompress a single hour of tick data"""
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
                            # Expected for holidays/non-trading times
                            self.missing_logger.info(
                                f"No data available for {instrument} on {day} hour {hour:02d} (404)"
                            )
                            return None
                        else:
                            raise Exception(f"HTTP {response.status}")
            except Exception as e:
                if attempt < self.MAX_RETRIES - 1:
                    logging.warning(
                        f"Attempt {attempt + 1}/{self.MAX_RETRIES} failed for {instrument} "
                        f"on {day} hour {hour:02d}: {str(e)}. Retrying in {retry_delay} seconds..."
                    )
                    await asyncio.sleep(retry_delay)
                    retry_delay *= 1.5  # Gentler exponential backoff
                else:
                    # Log unexpected failures to missing_data.log
                    self.missing_logger.error(
                        f"Failed to download {instrument} for {day} hour {hour:02d} "
                        f"after {self.MAX_RETRIES} attempts: {str(e)}"
                    )
                    raise

    def decode_ticks(self, raw_data: bytes, day: date, hour: int, instrument: str) -> list[Tuple]:
        """
        Decode binary tick data into list of tuples
        Returns: List[Tuple(timestamp, bid, ask, bid_volume, ask_volume)]
        """
        ticks = []
        point_value = self.config['INSTRUMENTS'][instrument]['point_value']
        
        for i in range(0, len(raw_data), self.TICK_SIZE):
            tick_data = raw_data[i:i + self.TICK_SIZE]
            if len(tick_data) == self.TICK_SIZE:
                # Unpack binary data
                ms_offset, bid_raw, ask_raw, vol_bid, vol_ask = struct.unpack('!IIIff', tick_data)
                
                # Convert prices
                bid = float(bid_raw) / point_value
                ask = float(ask_raw) / point_value
                
                # Convert volumes (multiply by 1M)
                bid_volume = float(vol_bid) * 1_000_000
                ask_volume = float(vol_ask) * 1_000_000
                
                ticks.append((
                    ms_offset,  # Keep as offset, let caller handle final timestamp
                    bid,
                    ask,
                    bid_volume,
                    ask_volume
                ))
        
        return ticks