import asyncio
import websockets
import json
import time
import multiprocessing

async def binance_websocket_client(symbol: str, data_queue: multiprocessing.Queue, streams: list = None):
    """Connects to Binance WebSocket streams and puts incoming data into a queue."""
    if streams is None:
        streams = ['aggTrade', 'depth@0ms', 'kline_1m']
    
    # 构建流名称列表
    stream_names = []
    for stream in streams:
        if stream == 'aggTrade':
            stream_names.append(f"{symbol.lower()}@aggTrade")
        elif stream == 'depth@0ms':
            stream_names.append(f"{symbol.lower()}@depth@0ms")
        elif stream == 'kline_1m':
            stream_names.append(f"{symbol.lower()}@kline_1m")
        else:
            stream_names.append(f"{symbol.lower()}@{stream}")
    
    url = f"wss://fstream.binance.com/stream?streams={'/'.join(stream_names)}"

    while True:
        try:
            async with websockets.connect(url) as websocket:
                print(f"Connected to Binance WebSocket for {symbol}")
                while True:
                    message = await websocket.recv()
                    data = json.loads(message)
                    
                    # Add local timestamp
                    data['localtime'] = time.time()
                    
                    stream = data['stream']
                    stream_type = None
                    if 'aggTrade' in stream:
                        stream_type = 'aggtrade'
                    elif 'depth' in stream:
                        stream_type = 'depth'
                    elif 'kline' in stream:
                        stream_type = 'kline'
                    
                    if stream_type:
                        data_queue.put((stream_type, data))

        except websockets.exceptions.ConnectionClosed as e:
            print(f"Connection closed: {e}. Reconnecting in 5 seconds...")
            await asyncio.sleep(5)
        except Exception as e:
            print(f"An error occurred: {e}. Reconnecting in 5 seconds...")
            await asyncio.sleep(5)
