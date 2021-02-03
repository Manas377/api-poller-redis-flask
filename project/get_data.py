import json
import asyncio
import time
import requests
import redis
from rq import Queue, Worker
from rq.job import Job
import pickle
from rq.registry import StartedJobRegistry
import time

r = redis.Redis()
convert_to_json = Queue(connection=r)
registry = StartedJobRegistry('default', connection=r)

# FOR MONGO-DB CONNECTION
from pymongo import MongoClient
MONGO_URI = "mongodb://manaskundu:techstax@cluster0-shard-00-00.e9ozd.mongodb.net:27017,cluster0-shard-00-01.e9ozd.mongodb.net:27017,cluster0-shard-00-02.e9ozd.mongodb.net:27017/<dbname>?ssl=true&replicaSet=atlas-9yple9-shard-0&authSource=admin&retryWrites=true&w=majority"
cluster = MongoClient(MONGO_URI, connect=False)
db = cluster["techstax"]
collection = db["techstax"]

loop = asyncio.get_event_loop()  # LOOP EVENT CREATED
urllist = ["https://www.thecocktaildb.com/api/json/v1/1/random.php", "https://randomuser.me/api/"]

async def get_cocktail_api():
    url = urllist[0]
    await asyncio.sleep (0.01)
    response = requests.get(url).json()
    await asyncio.sleep(0.01)
    json_response = response
    return json_response


async def get_user_api():
    url = urllist[1]
    await asyncio.sleep (0.01)
    response = requests.get(url).json()
    await asyncio.sleep(0.01)
    json_response = response
    return json_response

# JSONDecodeError("Expecting value", s, err.value) from None

async def assign_cocktail():

    user_loop_object = loop.create_task(get_user_api())
    await asyncio.sleep(0.01)
    cocktail_loop_object = loop.create_task(get_cocktail_api())
    await asyncio.sleep(0.01)
    await asyncio.wait([user_loop_object, cocktail_loop_object])  # AWAITS FOR EXECUTION OF GET_USER_API

                                                                     # AND GET_COCKTAIL_API
    user_object = user_loop_object.result()
    cocktail_object = cocktail_loop_object.result ()
    final_object = user_object["results"] + cocktail_object["drinks"]
    return final_object   # USER AT final_object[0], COCKTAIL AT final_object[1]


def runner():
    for i in range(5):  # ATTEMPTS AFTER FAILING
        # while True:  # ATTEMPT UNTIL RESULT IS OBTAINED
        try:
            response = loop.run_until_complete(assign_cocktail())
            print(response)
            return response
        except ValueError as e:
            time.sleep(2)
            continue



















