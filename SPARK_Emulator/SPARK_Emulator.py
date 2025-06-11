import sys
import os
from pydantic import BaseModel
from fastapi import FastAPI
import requests
import time
import threading
from collections import deque

# Add parent directory to Python path
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from SPARK_sim_cfg import IP, PORT, DUMMY_LINK, SPARK_DELAY

# Define a Pydantic model for the request body
class SparkData(BaseModel):
    scid: int
    start_time: str
    metric: str
    source : str
    job_id: int
    endpoint: str

# Create an instance of the FastAPI class
app = FastAPI()

#Start the SPARK jobs queue 
spark_jobs_queue = deque()

#Process SPARK_data
def process_spark_jobs():
    while True:
        if len(spark_jobs_queue) > 0:
            print(f'Number of SPARK Jobs in Queue: {len(spark_jobs_queue)}')
            spark_job = spark_jobs_queue.popleft()
            print(f'Processing SPARK Job {spark_job}')    
            time.sleep(SPARK_DELAY)
            response = requests.post(spark_job.endpoint, json={'job_id': spark_job.job_id, 'url': DUMMY_LINK})
        else:
            print('No Spark Jobs in Queue')

# Define a POST endpoint
@app.post("/")
def get_spark_jobs(spark_data: SparkData):
    spark_jobs_queue.append(spark_data)
    return {"message": spark_data}


if __name__ == "__main__":
    # Start the queue processing thread
    thread = threading.Thread(target=process_spark_jobs ,daemon=True)
    thread.start()
    import uvicorn
    uvicorn.run(app, host=IP, port=PORT)