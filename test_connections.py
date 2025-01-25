# test_connections.py
import asyncio
import aiohttp
import logging
from datetime import datetime, timezone
from typing import List, Dict

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)

class ConnectionTester:
    URL_TEMPLATE = "https://datafeed.dukascopy.com/datafeed/XAUUSD/2024/00/16/{hour:02d}h_ticks.bi5"
    
    def __init__(self):
        self.headers = {'User-Agent': 'Python'}
        self.results: Dict[int, Dict] = {}

    async def single_download(self, hour: int, session: aiohttp.ClientSession) -> bool:
        """Test a single download"""
        url = self.URL_TEMPLATE.format(hour=hour)
        try:
            async with session.get(url) as response:
                if response.status == 200:
                    return True
                elif response.status == 503:
                    logging.info(f"Hour {hour:02d}: Got 503 (Service Unavailable)")
                    return False
                else:
                    logging.info(f"Hour {hour:02d}: Got status {response.status}")
                    return False
        except Exception as e:
            logging.error(f"Hour {hour:02d}: Error: {str(e)}")
            return False

    async def test_concurrent_downloads(self, concurrent_count: int):
        """Test specific number of concurrent downloads"""
        async with aiohttp.ClientSession(headers=self.headers) as session:
            tasks = []
            for hour in range(concurrent_count):
                tasks.append(self.single_download(hour, session))
            
            results = await asyncio.gather(*tasks)
            success_count = sum(1 for r in results if r)
            
            self.results[concurrent_count] = {
                'total': concurrent_count,
                'success': success_count,
                'fail': concurrent_count - success_count
            }
            
            logging.info(f"Concurrent connections {concurrent_count}: "
                        f"Success {success_count}, "
                        f"Failed {concurrent_count - success_count}")

    async def run_tests(self):
        """Run tests with increasing concurrency"""
        # Test from 1 to 20 concurrent connections
        for concurrent in range(1, 21):
            logging.info(f"\nTesting {concurrent} concurrent connections...")
            await self.test_concurrent_downloads(concurrent)
            # Add delay between tests to not overwhelm the server
            await asyncio.sleep(5)
        
        # Print summary
        print("\nTest Results:")
        print("="*50)
        for count, result in sorted(self.results.items()):
            print(f"Concurrent: {count:2d} | "
                  f"Success: {result['success']:2d} | "
                  f"Failed: {result['fail']:2d} | "
                  f"Success Rate: {(result['success']/result['total']*100):3.0f}%")
        print("="*50)

async def main():
    tester = ConnectionTester()
    await tester.run_tests()

if __name__ == "__main__":
    asyncio.run(main())