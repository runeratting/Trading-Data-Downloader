# test_download.py
import asyncio
import ssl
import aiohttp
from datetime import date, datetime, timezone
from downloader import DukasCopyDownloader
import logging
from config import load_config

async def test_single_hour():
    try:
        # Load configuration
        config = load_config('config')
        downloader = DukasCopyDownloader(config)
        test_date = date(2024, 1, 16)
        test_hour = 10
        instrument = 'XAUUSD'
        
        logging.info(f"Downloading {instrument} data for {test_date} hour {test_hour}")
        
        # Create SSL context that doesn't verify certificates
        ssl_context = ssl.create_default_context()
        ssl_context.check_hostname = False
        ssl_context.verify_mode = ssl.CERT_NONE
        
        # Create aiohttp connector with SSL context
        connector = aiohttp.TCPConnector(ssl=ssl_context)
        
        # Create session with connector and pass to downloader
        async with aiohttp.ClientSession(connector=connector) as session:
            # Download and process data
            raw_data = await downloader.download_hour(instrument, test_date, test_hour, session=session)
        
        if raw_data is None:
            logging.info("No data available")
            return
            
        # Decode ticks with validation
        ticks = downloader.decode_ticks(raw_data, test_date, test_hour, instrument)
        
        # Get validation stats if available
        if hasattr(downloader, 'validation_stats') and instrument in downloader.validation_stats:
            stats = downloader.validation_stats[instrument]
            logging.info(f"\nValidation Stats:")
            logging.info(f"Total ticks: {stats.total_ticks}")
            logging.info(f"Valid ticks: {stats.valid_ticks}")
            logging.info(f"Invalid ticks: {stats.invalid_ticks}")
            if stats.error_counts:
                logging.info("\nValidation Errors:")
                for error_type, count in stats.error_counts.items():
                    logging.info(f"{error_type}: {count}")
        
        if not ticks:
            logging.info("No ticks found in data")
            return
            
        # Get first tick
        first_tick = ticks[0]
        ms_offset, bid, ask, bid_vol, ask_vol = first_tick
        
        # Convert timestamp
        dt = datetime.fromtimestamp(ms_offset/1000, tz=timezone.utc)
        
        logging.info("First tick values:")
        logging.info(f"Time: {dt.strftime('%Y-%m-%d %H:%M:%S.%f')[:-3]}")
        logging.info(f"Bid: {bid:.3f}")
        logging.info(f"Ask: {ask:.3f}")
        logging.info(f"Bid Volume: {bid_vol:.2f}")
        logging.info(f"Ask Volume: {ask_vol:.2f}")
            
    except Exception as e:
        logging.error(f"Test failed: {str(e)}")
        raise

if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(levelname)s - %(message)s'
    )
    
    try:
        asyncio.run(test_single_hour())
    except KeyboardInterrupt:
        logging.info("Test interrupted by user")
    except Exception as e:
        logging.error(f"Test failed: {str(e)}")
