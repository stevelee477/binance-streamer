"""
支持订单簿管理的WebSocket客户端
在原有数据收集基础上，增加本地订单簿维护功能
"""
import asyncio
import websockets
import json
import time
import multiprocessing
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from .orderbook_manager import OrderBookManager

async def binance_websocket_client_with_orderbook(
    symbol: str, 
    data_queue: multiprocessing.Queue, 
    streams: list = None,
    orderbook_manager: 'OrderBookManager' = None
):
    """
    连接到币安WebSocket流并维护本地订单簿
    同时保持原有的数据收集功能
    """
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
                print(f"Connected to Binance WebSocket for {symbol} with OrderBook management")
                
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
                        
                        # 处理订单簿更新
                        if orderbook_manager:
                            try:
                                orderbook_manager.handle_depth_event(symbol, data['data'])
                                
                                # 检查是否需要重新同步
                                orderbook = orderbook_manager.order_books.get(symbol)
                                if orderbook and not orderbook.is_synchronized:
                                    print(f"[{symbol}] 订单簿失去同步，需要重新初始化")
                                    # 注意：这里应该在合适的时候重新同步，但不能在这个函数中阻塞
                                    
                            except Exception as e:
                                print(f"[{symbol}] 处理订单簿更新时出错: {e}")
                        
                    elif 'kline' in stream:
                        stream_type = 'kline'
                    
                    # 发送到数据队列（保持原有功能）
                    if stream_type:
                        data_queue.put((stream_type, data))

        except websockets.exceptions.ConnectionClosed as e:
            print(f"Connection closed: {e}. Reconnecting in 5 seconds...")
            await asyncio.sleep(5)
        except Exception as e:
            print(f"An error occurred: {e}. Reconnecting in 5 seconds...")
            await asyncio.sleep(5)


async def binance_websocket_client_simple(symbol: str, data_queue: multiprocessing.Queue, streams: list = None):
    """
    原始的WebSocket客户端（不带订单簿管理）
    用于向后兼容
    """
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