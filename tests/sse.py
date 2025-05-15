import requests
import json

def connect_sse(url):
    headers = {'Accept': 'text/event-stream'}
    
    with requests.get(url, headers=headers, stream=True) as response:
        for line in response.iter_lines():
            if line:
                decoded_line = line.decode('utf-8')
                print(decoded_line)

                # if decoded_line.startswith('data:'):
                #     data = decoded_line[5:].strip()
                #     try:
                #         event_data = json.loads(data)
                #         print(event_data)
                #     except json.JSONDecodeError:
                #         print("Non-JSON data:", data)

if __name__ == "__main__":
    url = "http://localhost:3000/sse/home/stream"
    connect_sse(url)