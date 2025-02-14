import asyncio
import logging
from datetime import datetime, date, timedelta, timezone
from pathlib import Path
from typing import List, Optional, Dict, Tuple, Set
from tqdm import tqdm
import argparse

from downloader import DukasCopyDownloader
from db_handler import TickDBHandler
from config import load_config, Config, ConfigurationError
from checkpoint import CheckpointManager

class DataOrchestrator:
    def __init__(self, config: Config):
        self.config = config
        self.downloader = DukasCopyDownloader(config)
        self.db = TickDBHandler(config)
        self.stats: Dict[str, InstrumentStats] = {}
        if self.config.checkpoint.enabled:
            self.checkpoint_manager = CheckpointManager(self.config.checkpoint.directory)
        else:
            self.checkpoint_manager = None

    class InstrumentStats:
        def __init__(self):
            self.total_ticks: int = 0
            self.days_processed: int = 0
            self.hours_processed: int = 0
            self.total_time_ms: float = 0
            self.missing_hours: int = 0
            self.processed_days: Set[date] = set()

    async def get_dates_to_download(self, instrument: str) -> List[date]:
        """Get list of dates to download based on latest data and config"""
        # Get latest timestamp from DB
        latest_timestamp = await self.db.get_latest_timestamp(instrument)
        
        if latest_timestamp:
            start_date = latest_timestamp.date() + timedelta(days=1)
            logging.info(f"{instrument}: Found existing data, continuing from {start_date} (day after latest data)")
        else:
            start_date = self.config.instruments[instrument].start_date.date()
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
        return self.config.instruments[instrument].trading_hours(day.weekday(), hour)

    async def process_hour(self, instrument: str, day: date, hour: int) -> Optional[int]:
        """Process a single hour of data"""
        if not self.should_download_hour(instrument, day, hour):
            return None
            
        if self.checkpoint_manager:
            async with self.checkpoint_manager.track_progress(instrument, day, hour):
                try:
                    raw_data = await self.downloader.download_hour(instrument, day, hour)
                    if raw_data is None:
                        return None
                        
                    ticks = self.downloader.decode_ticks(raw_data, day, hour, instrument)
                    if ticks:
                        tick_count = await self.db.insert_ticks(instrument, ticks)
                        await self.checkpoint_manager.mark_hour_complete(
                            instrument, day, hour, tick_count
                        )
                        return tick_count
                    
                    return 0
                    
                except Exception as e:
                    logging.error(f"Failed to process {instrument} for {day} hour {hour}: {str(e)}")
                    raise
        else:
            # Process without checkpointing
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
                    self.config.instruments[instrument].trading_hours
                )
            else:
                trading_hours = [
                    hour for hour in range(24)
                    if self.should_download_hour(instrument, day, hour)
                ]
        
        if not trading_hours:
            return 0, 0
        
        # Create semaphore to limit concurrent downloads
        semaphore = asyncio.Semaphore(self.config.downloader.max_concurrent_downloads)
        batch_size = self.config.downloader.batch_size
        
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

    async def run(
        self,
        mode: str = 'normal',
        start_date: Optional[date] = None,
        end_date: Optional[date] = None,
        resume: bool = True
    ):
        """Main execution method"""
        # Create semaphore to limit total concurrent downloads
        semaphore = asyncio.Semaphore(self.config.downloader.max_concurrent_downloads)
        batch_size = self.config.downloader.batch_size
        
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
        
        for instrument in self.config.instruments:
            self.stats[instrument] = self.InstrumentStats()
            
            logging.info(f"\nProcessing instrument: {instrument}")
            
            # Load checkpoint if enabled and resuming
            if self.checkpoint_manager and resume and self.config.checkpoint.auto_resume:
                for day in (start_date, end_date) if mode != 'normal' else []:
                    state = await self.checkpoint_manager.load_state(instrument, day)
                    if state:
                        logging.info(
                            f"Resuming download for {instrument} on {day} "
                            f"({len(state.completed_hours)} hours completed, "
                            f"{len(state.failed_hours)} hours failed)"
                        )

            if mode == 'normal':
                dates = await self.get_dates_to_download(instrument)
            else:
                if start_date is None or end_date is None:
                    raise ValueError("start_date and end_date are required for retry mode")
                
                missing_hours = await self.db.get_missing_hours_batch(instrument, start_date, end_date)
                
                # If resuming, exclude completed hours from checkpoint
                if self.checkpoint_manager and resume:
                    for day in list(missing_hours.keys()):
                        state = await self.checkpoint_manager.load_state(instrument, day)
                        if state:
                            missing_hours[day] = [
                                h for h in missing_hours[day]
                                if h not in state.completed_hours
                            ]
                            # Remove day if all hours are completed
                            if not missing_hours[day]:
                                del missing_hours[day]
                
                dates = list(missing_hours.keys())
                
                total_missing = sum(len(hours) for hours in missing_hours.values())
                if total_missing == 0:
                    logging.info(f"No missing data found for {instrument}")
                    continue
                
                logging.info(f"Found {len(dates)} days with {total_missing} missing hours for {instrument}")
                self.stats[instrument].missing_hours = total_missing
            
            if not dates:
                logging.info(f"No dates to process for {instrument}")
                continue
            
            # Setup progress bar
            pbar = tqdm(
                total=self.stats[instrument].missing_hours if mode == 'retry' else len(dates)
            )
            pbar.set_description(
                f"Processing {instrument} - Found {self.stats[instrument].missing_hours} missing hours"
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
            for i in range(0, len(all_batches), self.config.downloader.max_concurrent_downloads):
                current_batches = all_batches[i:i + self.config.downloader.max_concurrent_downloads]
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
                            self.stats[instrument].total_ticks += ticks
                            if day not in self.stats[instrument].processed_days:
                                self.stats[instrument].days_processed += 1
                                self.stats[instrument].processed_days.add(day)
                        
                        pbar.update(batch_size)
                        if mode == 'retry':
                            pbar.set_description(
                                f"Processing {instrument} - "
                                f"Downloaded {self.stats[instrument].total_ticks} ticks"
                            )
                        else:
                            pbar.set_description(
                                f"Processing {instrument} - "
                                f"Day {self.stats[instrument].days_processed}/{len(dates)}"
                            )
            
            pbar.close()
            
            # Print statistics and cleanup old checkpoints
            self.print_instrument_stats(instrument)
            if self.checkpoint_manager:
                await self.checkpoint_manager.cleanup_old_checkpoints(
                    self.config.checkpoint.cleanup_days
                )

    def print_instrument_stats(self, instrument: str):
        """Print statistics for an instrument"""
        stats = self.stats[instrument]
        
        # Basic statistics
        print("\n" + "="*50)
        print(f"Statistics for {instrument}:")
        print(f"Total days processed: {stats.days_processed}")
        print(f"Total ticks inserted: {stats.total_ticks:,}")
        if stats.days_processed > 0:
            print(f"Average ticks per day: {stats.total_ticks/stats.days_processed:,.0f}")
            print(f"Average time per day: {stats.total_time_ms/stats.days_processed:,.0f}ms")
            print(f"Average ticks per second: {(stats.total_ticks/(stats.total_time_ms/1000)):,.0f}")
        
        # Data quality metrics
        if hasattr(self, 'metrics_manager'):
            quality_metrics = self.metrics_manager.collector.quality.get(instrument, {})
            if quality_metrics:
                print("\nData Quality:")
                total_ticks = sum(m.total_ticks for m in quality_metrics.values())
                valid_ticks = sum(m.valid_ticks for m in quality_metrics.values())
                invalid_ticks = sum(m.invalid_ticks for m in quality_metrics.values())
                if total_ticks > 0:
                    print(f"Valid ticks: {valid_ticks:,} ({valid_ticks/total_ticks*100:.1f}%)")
                    print(f"Invalid ticks: {invalid_ticks:,} ({invalid_ticks/total_ticks*100:.1f}%)")
                
                # Show validation errors by type
                error_counts = {}
                for metrics in quality_metrics.values():
                    for error_type, count in metrics.validation_errors.items():
                        error_counts[error_type] = error_counts.get(error_type, 0) + count
                if error_counts:
                    print("\nValidation Errors:")
                    for error_type, count in sorted(error_counts.items()):
                        print(f"  {error_type}: {count:,}")
        
        # System metrics
        if hasattr(self, 'metrics_manager'):
            system_metrics = self.metrics_manager.collector.system
            if system_metrics:
                print("\nSystem Health:")
                latest = system_metrics[-1]
                print(f"Memory usage: {latest.process_memory_mb:.1f}MB ({latest.system_memory_percent:.1f}% system)")
                print(f"CPU usage: {latest.cpu_percent:.1f}%")
                print(f"Disk usage: {latest.disk_usage_percent:.1f}%")
                print(f"Open files: {latest.open_files}")
                print(f"Thread count: {latest.thread_count}")
        
        print("="*50 + "\n")

def parse_date(date_str: str) -> date:
    """Convert string date in YYYY-MM-DD format to date object"""
    try:
        return datetime.strptime(date_str, '%Y-%m-%d').date()
    except ValueError as e:
        raise argparse.ArgumentTypeError(f"Invalid date format. Please use YYYY-MM-DD. Error: {str(e)}")

async def main():
    parser = argparse.ArgumentParser(description='DukasCopy Data Downloader')
    parser.add_argument('--mode', choices=['normal', 'retry', 'both'], default='normal',
                      help='Operation mode: normal (default), retry, or both')
    parser.add_argument('--start', type=parse_date,
                      help='Start date (YYYY-MM-DD). Defaults to config value')
    parser.add_argument('--end', type=parse_date,
                      help='End date (YYYY-MM-DD). Defaults to config value')
    parser.add_argument('--config-dir', default='config',
                      help='Configuration directory (default: config)')
    parser.add_argument('--no-resume', action='store_true',
                      help='Disable automatic resume from checkpoints')
    parser.add_argument('--no-monitoring', action='store_true',
                      help='Disable metrics collection')
    
    args = parser.parse_args()
    
    # Validate arguments
    if args.mode == 'retry' and (args.start is None or args.end is None):
        parser.error("--start and --end dates are required for retry mode")
    
    try:
        # Load and validate configuration
        config = load_config(args.config_dir)
        
        # Setup logging
        config.setup_logging()
        
        print("\nStarting data download...")
        orchestrator = DataOrchestrator(config)
        
        # Setup monitoring if enabled
        if config.monitoring.enabled and not args.no_monitoring:
            from monitoring import MetricsManager
            orchestrator.metrics_manager = MetricsManager(Path(args.config_dir))
            await orchestrator.metrics_manager.start_monitoring()
            print("Monitoring enabled - metrics will be collected")
        
        try:
            await orchestrator.run(
                mode=args.mode,
                start_date=args.start,
                end_date=args.end,
                resume=not args.no_resume
            )
            print("\nDownload completed!")
            
            # Show final metrics summary
            if hasattr(orchestrator, 'metrics_manager'):
                print("\nFinal Metrics Summary:")
                summary = orchestrator.metrics_manager.get_metrics_summary()
                print(f"Total uptime: {summary['system']['uptime_hours']:.1f} hours")
                print(f"Peak memory usage: {summary['system']['current_memory_mb']:.1f}MB")
                print(f"Final CPU usage: {summary['system']['current_cpu_percent']:.1f}%")
                print(f"Total open files: {summary['system']['total_open_files']}")
                print(f"Active threads: {summary['system']['thread_count']}")
        finally:
            # Stop monitoring and save metrics
            if hasattr(orchestrator, 'metrics_manager'):
                await orchestrator.metrics_manager.stop_monitoring()
        
    except ConfigurationError as e:
        print(f"Configuration error: {str(e)}")
        return 1
    except Exception as e:
        logging.error(f"Main execution failed: {str(e)}")
        raise
    finally:
        if 'orchestrator' in locals():
            orchestrator.db.close()

if __name__ == "__main__":
    asyncio.run(main())
