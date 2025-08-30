import aiohttp
import asyncio
import time
import multiprocessing

async def get_depth_snapshot(session, symbol: str, data_queue: multiprocessing.Queue, limit: int = 1000):
    """Fetches depth snapshot from Binance REST API and puts it into a queue."""
    url = f"https://fapi.binance.com/fapi/v1/depth?symbol={symbol}&limit={limit}"
    try:
        async with session.get(url) as response:
            response.raise_for_status()
            data = await response.json()
            
            # Add local timestamp and symbol
            data['localtime'] = time.time()
            data['symbol'] = symbol
            
            data_queue.put(('depth_snapshot', data))
            print(f"Depth snapshot for {symbol} fetched and sent to writer.")
            return data
    except aiohttp.ClientError as e:
        print(f"An error occurred while fetching depth snapshot: {e}")
        return None

async def main():
    pass

if __name__ == "__main__":
    pass

