import os
from dotenv import load_dotenv, find_dotenv
from fastapi import FastAPI, Request
from fastapi.templating import Jinja2Templates
from pydantic import BaseModel, ValidationError
from contextlib import asynccontextmanager
import redis
import json
import threading
import time
import asyncio
import httpx

templates = Jinja2Templates(directory="Templates")

path = find_dotenv()
load_dotenv(path)


# ------------------- Worker -------------------
def worker():
    redis_url = os.getenv("redis_host_url")
    redis_port = int(os.getenv("redis_port"))
    redis_username = os.getenv("redis_username")
    redis_password = os.getenv("redis_password")

    redis_client = redis.Redis(
        host=redis_url,
        port=redis_port,
        decode_responses=True,
        username=redis_username,
        password=redis_password,
         
    )

    supabase_url = os.getenv("supabase_host_restapi_url")  # REST API URL
    supabase_anon_key = os.getenv("supabase_anon_key")

    while True:
        data_list = get_data_from_redis(redis_client)
        if data_list:
            validated_data_list = validate_data(data_list)
            # Run the async function in the thread
            asyncio.run(put_data_into_supabase(supabase_url, supabase_anon_key, validated_data_list))
        else:
            time.sleep(2)
        time.sleep(5)


@asynccontextmanager
async def lifespan(app: FastAPI):
    print("Worker Woke up")
    my_thread = threading.Thread(target=worker, daemon=True)
    my_thread.start()
    print("Worker done working")
    yield
    print("Worker going to sleep")


app = FastAPI(lifespan=lifespan)

# ------------------- Models -------------------
class GenralQueue:
    def __init__(self):
        self.Queue = []
        self.size = 0

    def enqueue(self, data):
        self.Queue.append(data)
        self.size += 1
        print("data inserted")

    def dequeue(self):
        if self.size > 0:
            firstElement = self.Queue.pop(0)
            self.size -= 1
            print("Data removed")
            return firstElement
        else:
            print("Queue is empty no data")


class Coords(BaseModel):
    lat: float
    lng: float


class LocationData(BaseModel):
    name: str
    address: str
    coordinates: Coords


class SubBinData(BaseModel):
    id: int
    type: str
    capacity: int
    lastEmpty: str
    currentLevel: float


class ValidateDataStructure(BaseModel):
    id: str
    status: str
    subBins: list[SubBinData]
    location: LocationData
    lastUpdated: str


# ------------------- Helpers -------------------
def validate_data(data_list):
    parsed_data_list = []
    for data in data_list:
        data = json.loads(data)
        try:
            validate_data = ValidateDataStructure(**data)
            parsed_data_list.append(validate_data.model_dump())
        except ValidationError:
            continue
    return parsed_data_list


async def put_data_into_supabase(url, key, validated_data_list):
    headers = {
        "apikey": key,
        "Authorization": f"Bearer {key}",
        "Content-Type": "application/json",
        "Prefer": "resolution=merge-duplicates"
    }

    async with httpx.AsyncClient(timeout=10) as client:
        try:
            for count, data in enumerate(validated_data_list):
                payload = {
                    "key": f"bin_data{count}",
                    "value": data
                }
                response = await client.post(url, json=payload, headers=headers)
                print("Status:", response.status_code)
                if response.status_code not in [200, 201]:
                    print("Error:", response.text)

            return True

        except Exception as e:
            print("Supabase error:", str(e))
            return False


def get_data_from_redis(redis_client) -> list:
    data_keys = redis_client.keys()
    data_queue = GenralQueue()

    for key in data_keys:
        data = redis_client.get(key)
        data_queue.enqueue(data)
        redis_client.delete(key)

    return data_queue.Queue


# ------------------- Routes -------------------
@app.api_route("/", methods=["GET", "HEAD"])
def read_root(request: Request):
    return templates.TemplateResponse("index.html", {"request": request})
