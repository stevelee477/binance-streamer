import asyncio
import websockets
import json

async def debug_depth_stream():
    """Debug WebSocket depth stream to see actual data format"""
    url = "wss://fstream.binance.com/stream?streams=btcusdt@depth@0ms"
    
    try:
        async with websockets.connect(url) as websocket:
            print("Connected to Binance WebSocket for debugging...")
            
            # Get first few messages to see the format
            for i in range(3):
                message = await websocket.recv()
                data = json.loads(message)
                
                print(f"\n=== Message {i+1} ===")
                print(f"完整原始数据:")
                print(json.dumps(data, indent=2))
                print("=" * 80)
                
    except Exception as e:
        print(f"Error: {e}")

if __name__ == "__main__":
    asyncio.run(debug_depth_stream())