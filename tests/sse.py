import asyncio
import aiohttp
import json
from datetime import datetime
import random
from collections import defaultdict

async def connect_sse(session, url, conn_id, event_counts):
    headers = {'Accept': 'text/event-stream'}
    
    try:
        async with session.get(url, headers=headers) as response:
            async for line in response.content:
                if line:
                    decoded_line = line.decode('utf-8').strip()
                    if decoded_line.startswith('data:'):
                        data = decoded_line[5:].strip()
                        try:
                            event_data = json.loads(data)
                            timestamp = datetime.now().strftime('%H:%M:%S.%f')[:-3]
                            event_counts[conn_id] += 1
                            print(f"[{timestamp}] Connection {conn_id} received data: {len(event_data)} fields")
                        except json.JSONDecodeError:
                            print(f"Connection {conn_id} received non-JSON data")
    except asyncio.CancelledError:
        print(f"Connection {conn_id} cancelled")
        raise
    except Exception as e:
        print(f"Connection {conn_id} error: {str(e)}")

async def main():
    url = ""
    num_connections = 200
    event_counts = defaultdict(int)
    
    async with aiohttp.ClientSession() as session:
        tasks = []
        for i in range(num_connections):
            task = asyncio.create_task(connect_sse(session, url, i + 1, event_counts))
            tasks.append(task)
            print(f"Started connection {i + 1}")
        
        print("\nRunning for 1 minute...")
        try:
            await asyncio.sleep(60)
        except asyncio.CancelledError:
            pass
        
        for task in tasks:
            if not task.done():
                task.cancel()
        
        try:
            await asyncio.gather(*tasks, return_exceptions=True)
        except asyncio.CancelledError:
            pass
        
        print("\nResults after 1 minute:")
        print("-" * 40)
        total_events = 0
        active_connections = 0
        for conn_id in sorted(event_counts.keys()):
            events = event_counts[conn_id]
            if events > 0:
                active_connections += 1
            total_events += events
            print(f"Connection {conn_id}: {events} events")
        print("-" * 40)
        print(f"Total events across all connections: {total_events}")
        print(f"Active connections: {active_connections}/{num_connections}")
        if active_connections > 0:
            print(f"Average events per active connection: {total_events/active_connections:.2f}")

if __name__ == "__main__":
    print(f"Starting {100} SSE connections...")
    asyncio.run(main())