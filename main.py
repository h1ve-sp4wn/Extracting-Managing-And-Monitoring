import asyncio
import aiohttp
import json
import time
import random
import subprocess
import redis
from celery import Celery
from clickhouse_driver import Client
from prometheus_client import start_http_server, Gauge

GITHUB_API_URL = "https://api.github.com/events"
GITHUB_TOKEN = "your_github_token_here"
REDIS_HOST = 'localhost'
REDIS_PORT = 6379
CLICKHOUSE_HOST = 'localhost'
CLICKHOUSE_DB = 'github_data'
CELERY_BROKER = 'redis://localhost:6379/0'

redis_client = redis.Redis(host=REDIS_HOST, port=REDIS_PORT, db=0)

app = Celery('github_jobs', broker=CELERY_BROKER)

client = Client(CLICKHOUSE_HOST)

queue_gauge = Gauge('github_event_queue_length', 'Length of the GitHub event queue')


def retry_on_failure(func, max_retries=5, delay=2):
    retries = 0
    while retries < max_retries:
        try:
            return func()
        except Exception as e:
            retries += 1
            print(f"Error occurred: {e}. Retrying {retries}/{max_retries}...")
            time.sleep(delay * random.uniform(1, 2))
            if retries == max_retries:
                print("Max retries reached. Giving up.")
                raise e

async def fetch_github_events(session: aiohttp.ClientSession):
    headers = {"Authorization": f"token {GITHUB_TOKEN}"}
    async with session.get(GITHUB_API_URL, headers=headers) as response:
        if response.status == 200:
            events = await response.json()
            process_events(events)
        else:
            print(f"Error fetching events: {response.status}")
            await handle_rate_limit(response)

async def handle_rate_limit(response):
    if response.status == 403:
        retry_after = int(response.headers.get("Retry-After", 60))
        print(f"Rate limit exceeded, retrying after {retry_after} seconds...")
        await asyncio.sleep(retry_after)
        await fetch_github_events()

def process_events(events):
    for event in events:
        print(f"Processing event: {event}")
        push_to_redis_stream(event)
        analyze_event.apply_async(args=[event])

def push_to_redis_stream(event_data):
    stream_name = "github_events_stream"
    redis_client.xadd(stream_name, {"event": json.dumps(event_data)})
    print(f"Event added to stream: {event_data}")

def consume_from_redis_stream():
    while True:
        entries = redis_client.xread({"github_events_stream": "0"}, block=0, count=10)
        for entry in entries:
            event_data = json.loads(entry[1][0][1]["event"])
            print(f"Processing event: {event_data}")
            run_analysis_job(event_data)

@app.task
def analyze_event(event_data):
    print(f"Analyzing event: {event_data}")
    run_analysis_job(event_data)

def run_analysis_job(event_data):
    repo = event_data['repo']['name']
    command = f"docker run --rm iseorg/analyzer:latest {repo}"
    subprocess.run(command, shell=True)
    batch_insert_to_clickhouse([event_data])

def batch_insert_to_clickhouse(events):
    values = []
    for event in events:
        values.append((
            event['type'],
            event['repo']['name'],
            time.strftime('%Y-%m-%d %H:%M:%S', time.gmtime())
        ))
    
    client.execute('''
        INSERT INTO github_events (event_type, repo_name, created_at)
        VALUES
    ''', values)
    print(f"Inserted {len(events)} events into ClickHouse")

def monitor_queue():
    while True:
        queue_length = redis_client.llen("github_events_stream")
        queue_gauge.set(queue_length)
        time.sleep(30)

async def main():
    async with aiohttp.ClientSession() as session:
        await fetch_github_events(session)

if __name__ == "__main__":

    start_http_server(8000)
    
    monitor_queue()

    asyncio.run(main())

    consume_from_redis_stream()