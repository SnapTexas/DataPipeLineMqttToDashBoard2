#Note: Implemented the data validation and cheking data from redis and reading it its working needs some cleaning
# not implemented the features that will help it work with load balancer
#will need to pull keys from all the buffer then get all the data from it 
#then expire it
# 
# Sign for debugging statement  #@D
#
# 
#  
# Sign for helper function   #@Hfun() 


import os
from dotenv import load_dotenv,find_dotenv
from fastapi import FastAPI,Request
from fastapi.templating import Jinja2Templates
from pydantic import BaseModel,ValidationError
from contextlib import asynccontextmanager
from typing import List,Dict
import redis
import json
from supabase import create_client
import threading , time 

templates=Jinja2Templates(directory="Templates")

path=find_dotenv()
load_dotenv(path)


def worker():
    redis_url=os.getenv("redis_host_url")
    my_port=os.getenv("redis_port")
    redis_username=os.getenv("redis_username")
    redis_password=os.getenv("redis_password")
    redis_client=redis.Redis(
        host=redis_url,
        port=my_port,
        decode_responses=True,
        username=redis_username,
        password=redis_password,
    )


    supabase_url=os.getenv("supabase_host_url")
    supabase_anon_key=os.getenv("supabase_anon_key")
    supabase_client=create_client(supabase_url,supabase_anon_key)
    while True:
        data_list=get_data_from_redis(redis_client)
        if data_list:

            validated_data_list=validate_data(data_list)
            put_data_into_supabase(supabase_client,validated_data_list)
        else:
            time.sleep(2)    
            #put in supabase
        time.sleep(5) 
    

        

        
        
        #@D print(data)

@asynccontextmanager
async def lifespan(app:FastAPI):
    print("Worker Woke up")
    print("Worker doing stuff")
    my_thread= threading.Thread(target=worker,daemon=True)
    my_thread.start()
    print("Worker done working")
    yield
    print("Worker going to sleep")




app=FastAPI(lifespan=lifespan)
class GenralQueue:
    def __init__(self):
        self.Queue=[]
        self.size=0
    def enqueue(self,data):
        self.Queue.append(data)
        self.size+=1
        print("data inserted")
    def dequeue(self):
        if self.size > 0:
            firstElement=self.Queue.pop(0)
            self.size-=1
            print("Data removed")
            return firstElement
        else:
            print("Queue is empty no data")

class Coords(BaseModel):
    lat : float
    lng : float

class LocationData(BaseModel):
    name : str
    address : str
    coordinates : Coords

class SubBinData(BaseModel):
    id: int
    type: str
    capacity: int
    lastEmpty: str
    currentLevel: float

class ValidateDataStructure(BaseModel):
    id : str
    status : str
    subBins : list[SubBinData]
    location :LocationData
    lastUpdated : str

#@Hfun() 

def validate_data(data_list):
    parsed_data_list=[]
    for data in data_list:
        data=json.loads(data)
        print(type(data))
        try:
            validate_data=ValidateDataStructure(**data)
            parsed_data_list.append(validate_data.model_dump())
        except ValidationError as e:
            continue
    return parsed_data_list
    

#@Hfun() 
#input: supabase_client or supabase_client_list(Pending)
#role: read and store data in supabase
#output: True or False based on data submitted or not when not return error
#improvement: needed
#Tested: True Not Complete
def put_data_into_supabase(supabase_client, validated_data_list):
    try:
        for count, data in enumerate(validated_data_list):
            response = supabase_client.table("kv_store_008d6529").upsert({
                "key": f"bin_data{count}",
                "value": data
            }).execute()

            # Safe error check
            if hasattr(response, "error") and response.error:
                return False, response.error.message

        return True  # all inserts succeeded

    except Exception as e:
        return False, str(e)
    


#@Hfun() 
#input: redis_client or redis_client list(pending)
#role: read and store data 
#output: data stored in redis in form of a Queue
#improvement: needed
#Tested: True


def get_data_from_redis(redis_client)->list:
    #note needs improvements if the program crashes in between the time we get data and before we delete
    #data the data may remain and maybe copied again with next update 
    
    #@D print("Looking for data in redis")
    #@D print("Keys found in redis")

    #gets the keys from redis of the data
    data_keys=redis_client.keys()
    #create a Queue to store the data 
    data_queue=GenralQueue()
  
    #@D print("Data Keys ",data_keys)
    for key in data_keys:
        data=redis_client.get(key)
        #@D print("Enqueued data : ",data)
        data_queue.enqueue(data)
        redis_client.delete(key)
        #@D print("Data Queue ",data_queue.Queue)
    return data_queue.Queue

@app.api_route("/",methods=["GET","HEAD"])
def read_root(request:Request):
    return templates.TemplateResponse("index.html",{"request":request})




    
    
        



