from project.get_data import runner
from flask import Flask, request, render_template, url_for, Blueprint, jsonify
import redis
from rq import Queue, Worker, Connection
from rq.connections import NoRedisConnectionException
import asyncio
from rq.job import Job
from project.get_data import runner
from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.schedulers.background import BlockingScheduler
import logging
from datetime import timedelta
from pymongo import MongoClient
import sys
import time
import json


# --------------------FOR REDIS--------------------------#
r = redis.Redis(host='localhost', port=6379, db=0)  # REDIS SERVER
queue = Queue('queue', connection=r, default_timeout=120)  # QUEUE NAME "queue" ON REDIS SERVER
database = Queue('database', connection=r, default_timeout=120)  # QUEUE NAME "queue" ON REDIS SERVER
app = Flask(__name__)

# --------------------FOR MONGO-DB--------------------------#
#---------------(DBAAS-PASSWORD-INSLUDED)-------------------#
MONGO_URI = "mongodb://manaskundu:techstax@cluster0-shard-00-00.e9ozd.mongodb.net:27017,cluster0-shard-00-01.e9ozd.mongodb.net:27017,cluster0-shard-00-02.e9ozd.mongodb.net:27017/<dbname>?ssl=true&replicaSet=atlas-9yple9-shard-0&authSource=admin&retryWrites=true&w=majority"
client = MongoClient(MONGO_URI, connect=True)
db = client.get_database('techstax')
collection = db["techstax"]


def insert(post):  # INSERT FUNCTION
    collection.insert_one(post)
    print("values inserted")
    return None


def data_fetcher_and_queuer():
    print("Queuing started", file=sys.stderr)  # PRINTED IN FLASK CONSOLE
    for i in range(5):
        try:
            job = queue.enqueue(runner)  # IMPORTING RUNNER IN MAIN
            time.sleep(2)
        except NoRedisConnectionException:
            logging.exception("Redis connection error, could not resolve a redis connection ")
            if job.result:  # Checking in case job was received (as happened in trial)
                print(job.result, file=sys.stderr)  # Output in flask console
                break
            else:
                continue
    time.sleep(10)
    print("Queuing ended", file=sys.stderr)
    job1 = queue.fetch_job(job_id=job.id)  # FETCH RESULT OBJECT

    job2 = queue.enqueue(json.dumps, job.result, depends_on=job1)  # CONVERTS RESULT TO JSON
    database.enqueue(collection.insert_one, job2.result, depends_on=job2)  # QUEUES FOR DATABASE INSERTION


scheduler = BackgroundScheduler()
job = scheduler.add_job(data_fetcher_and_queuer, 'interval', seconds = 5, max_instances=50)
scheduler.start()


if __name__ == '__main__':
    queue.empty()
    database.empty()
    app.run (debug=False)





