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
import argparse

# Normal mode (default)
# python main.py

# Retry mode with date range
# python main.py --mode retry --start 2024-01-01 --end 2024-01-31

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

    async def process_day(self, instrument: str, day: date, mode: str = 'normal') -> Tuple[int, float]:
        """Process a full day of data with controlled concurrency"""
        start_time = datetime.now()
        total_ticks = 0
        
        # Get hours to process based on mode
        if mode == 'retry':
            trading_hours = await self.db.find_missing_hours(
                instrument, 
                day, 
                self.config['INSTRUMENTS'][instrument]['trading_hours']
            )
        else:
            trading_hours = [
                hour for hour in range(24)
                if self.should_download_hour(instrument, day, hour)
            ]
        
        if not trading_hours:
            return 0, 0
        
        # Create semaphore to limit concurrent downloads
        semaphore = asyncio.Semaphore(4)  # Allow 4 concurrent batches
        batch_size = 4  # 4 downloads per batch
        
        async def process_batch(batch_hours):
            """Process a batch of hours with controlled concurrency"""
            async with semaphore:
                tasks = [self.process_hour(instrument, day, hour) for hour in batch_hours]
                results = await asyncio.gather(*tasks, return_exceptions=True)
                
                # Handle results and flush after each batch
                batch_ticks = 0
                for result in results:
                    if isinstance(result, Exception):
                        logging.error(f"Error processing hour for {instrument} on {day}: {str(result)}")
                        continue
                    if isinstance(result, int):
                        batch_ticks += result
                
                # Flush the buffer after each batch
                await self.db.flush_buffer(instrument)
                return batch_ticks
        
        # Create and run all batches
        batch_tasks = []
        for i in range(0, len(trading_hours), batch_size):
            batch = trading_hours[i:i + batch_size]
            batch_tasks.append(process_batch(batch))
        
        # Wait for all batches to complete and sum the results
        batch_results = await asyncio.gather(*batch_tasks, return_exceptions=True)
        for result in batch_results:
            if isinstance(result, Exception):
                logging.error(f"Error processing batch for {instrument} on {day}: {str(result)}")
                continue
            if isinstance(result, int):
                total_ticks += result
        
        elapsed_ms = (datetime.now() - start_time).total_seconds() * 1000
        logging.info(f"Processed {total_ticks} ticks for {instrument} on {day} in {elapsed_ms:.0f}ms")
        return total_ticks, elapsed_ms

    async def run(self, mode: str = 'normal', start_date: Optional[date] = None, end_date: Optional[date] = None):
        """Main execution method"""
        for instrument in self.config['INSTRUMENTS']:
            self.stats[instrument] = {
                'total_ticks': 0,
                'days_processed': 0,
                'hours_processed': 0,
                'total_time_ms': 0
            }
            
            logging.info(f"\nProcessing instrument: {instrument}")
            
            if mode == 'normal':
                dates = await self.get_dates_to_download(instrument)
            else:
                # For retry mode, use specified date range
                if start_date is None or end_date is None:
                    raise ValueError("start_date and end_date are required for retry mode")
                dates = [
                    start_date + timedelta(days=x)
                    for x in range((end_date - start_date).days + 1)
                ]
                
            if not dates:
                logging.info(f"No dates to process for {instrument}")
                continue
                
            logging.info(f"Found {len(dates)} days to process for {instrument}")
            
            # Setup progress bar
            pbar = tqdm(total=len(dates), desc=f"Processing {instrument}")
            
            # Process each day
            for day in dates:
                ticks, elapsed_ms = await self.process_day(instrument, day, mode)
                self.stats[instrument]['total_ticks'] += ticks
                self.stats[instrument]['days_processed'] += 1
                self.stats[instrument]['total_time_ms'] += elapsed_ms
                pbar.update(1)
                pbar.set_description(
                    f"Processing {instrument} - Day {self.stats[instrument]['days_processed']}/{len(dates)}"
                )
            
            pbar.close()
            
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
    parser = argparse.ArgumentParser(description='DukasCopy Data Downloader')
    parser.add_argument('--mode', choices=['normal', 'retry'], default='normal',
                      help='Operation mode: normal (default) or retry')
    parser.add_argument('--start', type=parse_date,
                      help='Start date (YYYY-MM-DD). Required for retry mode')
    parser.add_argument('--end', type=parse_date,
                      help='End date (YYYY-MM-DD). Required for retry mode')
    
    args = parser.parse_args()
    
    # Validate arguments
    if args.mode == 'retry' and (args.start is None or args.end is None):
        parser.error("--start and --end dates are required for retry mode")
    
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
        await orchestrator.run(
            mode=args.mode,
            start_date=args.start,
            end_date=args.end
        )
        print("\nDownload completed!")
    except Exception as e:
        logging.error(f"Main execution failed: {str(e)}")
        raise
    finally:
        orchestrator.db.close()

if __name__ == "__main__":
    asyncio.run(main())