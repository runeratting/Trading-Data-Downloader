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

# Both mode with date range
# python main.py --mode both --start 2024-01-01 --end 2024-01-31

# Use config dates but only retry
# python main.py --mode retry

def parse_date(date_str: str) -> date:
    """Convert string date in YYYY-MM-DD format to date object"""
    try:
        return datetime.strptime(date_str, '%Y-%m-%d').date()
    except ValueError as e:
        raise argparse.ArgumentTypeError(f"Invalid date format. Please use YYYY-MM-DD. Error: {str(e)}")

class DataOrchestrator:
    def __init__(self):
        self.config = {'DB_CONFIG': DB_CONFIG, 'INSTRUMENTS': INSTRUMENTS}
        self.downloader = DukasCopyDownloader(self.config)
        self.db = TickDBHandler(self.config)
        self.stats: Dict[str, Dict] = {}

    async def get_dates_to_download(self, instrument: str) -> List[date]:
        """Get list of dates to download based on latest data and config"""
        # Get latest timestamp from DB
        latest_timestamp = await self.db.get_latest_timestamp(instrument)
        
        if latest_timestamp:
            start_date = latest_timestamp.date() + timedelta(days=1)
            logging.info(f"{instrument}: Found existing data, continuing from {start_date} (day after latest data)")
        else:
            start_date = self.config['INSTRUMENTS'][instrument]['start_date']
            logging.info(f"{instrument}: No existing data found, starting from configured date: {start_date}")
        
        # Get end date as yesterday
        end_date = datetime.now().date() - timedelta(days=1)
        logging.info(f"{instrument}: Setting end date to yesterday: {end_date}")
        
        date_range = [
            start_date + timedelta(days=x)
            for x in range((end_date - start_date).days + 1)
        ]
        
        if date_range:
            logging.info(f"{instrument}: Will download {len(date_range)} days from {date_range[0]} to {date_range[-1]}")
        else:
            logging.info(f"{instrument}: No dates to download (start date {start_date} is after end date {end_date})")
        
        return date_range

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

    async def process_day(self, instrument: str, day: date, mode: str = 'normal', trading_hours: Optional[List[int]] = None) -> Tuple[int, float]:
        """Process a full day of data with controlled concurrency"""
        start_time = datetime.now()
        total_ticks = 0
        
        # Use provided trading hours or determine them based on mode
        if trading_hours is None:
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
        if total_ticks > 0:
            logging.info(f"Processed {total_ticks} ticks for {instrument} on {day} in {elapsed_ms:.0f}ms")
        return total_ticks, elapsed_ms

    async def run(self, mode: str = 'normal', start_date: Optional[date] = None, end_date: Optional[date] = None):
        """Main execution method"""
        # Create semaphore to limit total concurrent downloads
        semaphore = asyncio.Semaphore(4)  # Allow 4 concurrent batches
        batch_size = 4  # 4 downloads per batch
        
        async def process_hour_batch(instrument: str, day: date, hours: List[int]):
            """Process a batch of hours with controlled concurrency"""
            async with semaphore:
                tasks = [self.process_hour(instrument, day, hour) for hour in hours]
                results = await asyncio.gather(*tasks, return_exceptions=True)
                
                # Handle results
                batch_ticks = 0
                for result in results:
                    if isinstance(result, Exception):
                        logging.error(f"Error processing hour for {instrument} on {day}: {str(result)}")
                        continue
                    if isinstance(result, int):
                        batch_ticks += result
                
                # Flush the buffer after each batch
                await self.db.flush_buffer(instrument)
                return batch_ticks, day
        
        for instrument in self.config['INSTRUMENTS']:
            self.stats[instrument] = {
                'total_ticks': 0,
                'days_processed': 0,
                'hours_processed': 0,
                'total_time_ms': 0,
                'missing_hours': 0
            }
            
            logging.info(f"\nProcessing instrument: {instrument}")
            
            if mode == 'normal':
                dates = await self.get_dates_to_download(instrument)
            else:
                if start_date is None or end_date is None:
                    raise ValueError("start_date and end_date are required for retry mode")
                
                missing_hours = await self.db.get_missing_hours_batch(instrument, start_date, end_date)
                dates = list(missing_hours.keys())
                
                total_missing = sum(len(hours) for hours in missing_hours.values())
                if total_missing == 0:
                    logging.info(f"No missing data found for {instrument}")
                    continue
                
                logging.info(f"Found {len(dates)} days with {total_missing} missing hours for {instrument}")
                self.stats[instrument]['missing_hours'] = total_missing
            
            if not dates:
                logging.info(f"No dates to process for {instrument}")
                continue
            
            # Setup progress bar
            pbar = tqdm(total=total_missing if mode == 'retry' else len(dates))
            pbar.set_description(
                f"Processing {instrument} - Found {self.stats[instrument].get('missing_hours', 0)} missing hours"
            )
            
            # Create all hour batches across all days
            all_batches = []
            for day in dates:
                if mode == 'retry':
                    hours = missing_hours[day]
                else:
                    hours = [h for h in range(24) if self.should_download_hour(instrument, day, h)]
                
                # Split hours into batches
                for i in range(0, len(hours), batch_size):
                    batch_hours = hours[i:i + batch_size]
                    all_batches.append((day, batch_hours))
            
            # Process batches with controlled concurrency
            batch_tasks = []
            for i in range(0, len(all_batches), 4):  # Process 4 batches at a time
                current_batches = all_batches[i:i + 4]
                tasks = [
                    process_hour_batch(instrument, day, hours)
                    for day, hours in current_batches
                ]
                results = await asyncio.gather(*tasks, return_exceptions=True)
                
                # Handle results
                for result in results:
                    if isinstance(result, Exception):
                        logging.error(f"Error processing batch: {str(result)}")
                        continue
                    if isinstance(result, tuple):
                        ticks, day = result
                        if ticks > 0:
                            self.stats[instrument]['total_ticks'] += ticks
                            if day not in self.stats[instrument].get('processed_days', set()):
                                self.stats[instrument]['days_processed'] += 1
                                self.stats[instrument].setdefault('processed_days', set()).add(day)
                        
                        pbar.update(batch_size)
                        if mode == 'retry':
                            pbar.set_description(
                                f"Processing {instrument} - "
                                f"Downloaded {self.stats[instrument]['total_ticks']} ticks"
                            )
                        else:
                            pbar.set_description(
                                f"Processing {instrument} - "
                                f"Day {self.stats[instrument]['days_processed']}/{len(dates)}"
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
    parser.add_argument('--mode', choices=['normal', 'retry', 'both'], default='normal',
                      help='Operation mode: normal (default), retry, or both')
    parser.add_argument('--start', type=parse_date,
                      help='Start date (YYYY-MM-DD). Defaults to config value')
    parser.add_argument('--end', type=parse_date,
                      help='End date (YYYY-MM-DD). Defaults to config value')
    
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