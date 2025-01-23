# test_download.py
import asyncio
from datetime import date, datetime, timezone
from downloader import DukasCopyDownloader
import logging
from config import INSTRUMENTS

async def test_single_hour():
    try:
        downloader = DukasCopyDownloader({'INSTRUMENTS': INSTRUMENTS})
        test_date = date(2024, 1, 16)
        test_hour = 10
        instrument = 'XAUUSD'
        
        logging.info(f"Downloading {instrument} data for {test_date} hour {test_hour}")
        
        raw_data = await downloader.download_hour(instrument, test_date, test_hour)
        
        if raw_data is None:
            logging.info("No data available")
            return
            
        ticks = downloader.decode_ticks(raw_data, test_date, test_hour, instrument)
        
        if not ticks:
            logging.info("No ticks found in data")
            return
            
        # Get first tick
        first_tick = ticks[0]
        ms_offset, bid, ask, bid_vol, ask_vol = first_tick
        
        # Calculate timestamp
        base_timestamp = int(datetime(
            test_date.year, test_date.month, test_date.day, 
            test_hour, 0, 0, tzinfo=timezone.utc
        ).timestamp() * 1000)
        
        dt = datetime.fromtimestamp((base_timestamp + ms_offset)/1000, tz=timezone.utc)
        
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
    
    asyncio.run(test_single_hour())