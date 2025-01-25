# main.py
import asyncio
import logging
from datetime import datetime, date, timedelta, timezone
from pathlib import Path
from typing import List, Optional, Dict, Tuple
from tqdm import tqdm
from downloader import DukasCopyDownloader
from db_handler import TickDBHandler
from config import DB_CONFIG, INSTRUMENTS

class DataOrchestrator:
    def __init__(self):
        self.config = {'DB_CONFIG': DB_CONFIG, 'INSTRUMENTS': INSTRUMENTS}
        self.downloader = DukasCopyDownloader(self.config)
        self.db = TickDBHandler(self.config)
        self.stats: Dict[str, Dict] = {}

    async def get_dates_to_download(self, instrument: str) -> List[date]:
        """Calculate which dates need to be downloaded"""
        latest_timestamp = await self.db.get_latest_timestamp(instrument)
        instrument_config = self.config['INSTRUMENTS'][instrument]
        
        logging.info(f"Start date from config: {instrument_config['start_date']}")
        if latest_timestamp:
            logging.info(f"Latest timestamp from DB: {latest_timestamp}")
            start_date = latest_timestamp.date() + timedelta(days=1)
        else:
            start_date = instrument_config['start_date'].date()
            
        end_date = datetime.now(timezone.utc).date() - timedelta(days=1)
        
        logging.info(f"Will download from {start_date} to {end_date}")
        
        return [
            current_date for current_date in (
                start_date + timedelta(days=x) 
                for x in range((end_date - start_date).days + 1)
            )
        ]

    def should_download_hour(self, instrument: str, day: date, hour: int) -> bool:
        """Check if this hour should be downloaded based on trading hours"""
        trading_hours_func = self.config['INSTRUMENTS'][instrument]['trading_hours']
        return trading_hours_func(day.weekday(), hour)

    async def process_hour(self, instrument: str, day: date, hour: int) -> Optional[int]:
        """Process a single hour of data"""
        if not self.should_download_hour(instrument, day, hour):
            return None
            
        try:
            raw_data = await self.downloader.download_hour(instrument, day, hour)
            if raw_data is None:
                return None
                
            ticks = self.downloader.decode_ticks(raw_data, day, hour, instrument)
            if ticks:
                return await self.db.insert_ticks(instrument, ticks)
            
            return 0
            
        except Exception as e:
            logging.error(f"Failed to process {instrument} for {day} hour {hour}: {str(e)}")
            raise

    async def process_day(self, instrument: str, day: date) -> Tuple[int, float]:
        """Process a full day of data with controlled concurrency"""
        start_time = datetime.now()
        total_ticks = 0
        
        # Create batches of trading hours
        trading_hours = [
            hour for hour in range(24)
            if self.should_download_hour(instrument, day, hour)
        ]
        
        # Process hours in batches of 16
        batch_size = 16
        for i in range(0, len(trading_hours), batch_size):
            batch = trading_hours[i:i + batch_size]
            tasks = [self.process_hour(instrument, day, hour) for hour in batch]
            results = await asyncio.gather(*tasks, return_exceptions=True)
            
            for result in results:
                if isinstance(result, int):
                    total_ticks += result
        
        elapsed_ms = (datetime.now() - start_time).total_seconds() * 1000
        logging.info(f"Processed {total_ticks} ticks for {instrument} on {day} in {elapsed_ms:.0f}ms")
        return total_ticks, elapsed_ms

    async def run(self):
        """Main execution method"""
        for instrument in self.config['INSTRUMENTS']:
            self.stats[instrument] = {
                'total_ticks': 0,
                'days_processed': 0,
                'hours_processed': 0,
                'total_time_ms': 0
            }
            
            logging.info(f"\nProcessing instrument: {instrument}")
            
            dates = await self.get_dates_to_download(instrument)
            if not dates:
                logging.info(f"No dates to process for {instrument}")
                continue
                
            logging.info(f"Found {len(dates)} days to process for {instrument}")
            
            # Setup progress bar
            pbar = tqdm(total=len(dates), desc=f"Processing {instrument}")
            
            # Process each day
            for day in dates:
                ticks, elapsed_ms = await self.process_day(instrument, day)
                self.stats[instrument]['total_ticks'] += ticks
                self.stats[instrument]['days_processed'] += 1
                self.stats[instrument]['total_time_ms'] += elapsed_ms
                pbar.update(1)
                pbar.set_description(
                    f"Processing {instrument} - Day {self.stats[instrument]['days_processed']}/{len(dates)}"
                )
            
            pbar.close()
            
            # Ensure all data is written
            await self.db.flush_buffer(instrument)
            
            # Print statistics
            self.print_instrument_stats(instrument)

    def print_instrument_stats(self, instrument: str):
        """Print statistics for an instrument"""
        stats = self.stats[instrument]
        print("\n" + "="*50)
        print(f"Statistics for {instrument}:")
        print(f"Total days processed: {stats['days_processed']}")
        print(f"Total ticks inserted: {stats['total_ticks']:,}")
        if stats['days_processed'] > 0:
            print(f"Average ticks per day: {stats['total_ticks']/stats['days_processed']:,.0f}")
            print(f"Average time per day: {stats['total_time_ms']/stats['days_processed']:,.0f}ms")
            print(f"Average ticks per second: {(stats['total_ticks']/(stats['total_time_ms']/1000)):,.0f}")
        print("="*50 + "\n")

async def main():
    # Create logs directory if it doesn't exist
    Path('logs').mkdir(exist_ok=True)
    
    # Setup logging
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(levelname)s - %(message)s',
        handlers=[
            logging.FileHandler('logs/downloader.log'),
            logging.StreamHandler()
        ]
    )
    
    try:
        print("\nStarting data download...")
        orchestrator = DataOrchestrator()
        await orchestrator.run()
        print("\nDownload completed!")
    except Exception as e:
        logging.error(f"Main execution failed: {str(e)}")
        raise
    finally:
        orchestrator.db.close()

if __name__ == "__main__":
    asyncio.run(main())