import sys
import os
from pydantic import BaseModel
from fastapi import FastAPI
import requests
import time
import threading
from collections import deque
from sqlalchemy import create_engine, Column, Integer, String
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker

# Add parent directory to Python path
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from SPARK_sim_cfg import IP, PORT, DUMMY_LINK, SPARK_DELAY, DB_FILE

#Define base class for declaritive models
Base = declarative_base()

#Define DB Model for SPARK jobs
class spark_jobs(Base):
    __tablename__ = 'spark_jobs'
    spark_id = Column(Integer, primary_key=True, autoincrement=True)
    url = Column(String)

# Define a Pydantic model for the request body
class SparkData(BaseModel):
    scid: int
    start_time: str
    metric: str
    source : str
    job_id: int
    endpoint: str

#Get database url
db_url = os.path.join(os.path.dirname(os.path.abspath(__file__)),DB_FILE)

#Check if database exist
if not os.path.exists(db_url):
    print(f'Database {db_url} does not exist, it will be created')

#Create Engine
engine = create_engine(f'sqlite:///{db_url}')

#Create Table in in the database
Base.metadata.create_all(engine)



# Create an instance of the FastAPI class
app = FastAPI()

#Start the SPARK jobs queue 
spark_jobs_queue = deque()

def create_session():
        #Create Session from engine 
        Session = sessionmaker(bind=engine)
        return Session()

def store_data(url):
     #Create Session
     session = create_session()

     #Populate DB Model for SPARK data storage   
     spark_insert = spark_jobs(url=url)

     # Add the new instance to the session
     session.add(spark_insert)

     # Commit the session to save the data to the database
     session.commit()

     # Close the session
     session.close()
    

def get_latest_id():
        #Get the latest spark id 

        #Create Session
        session = create_session()
        Metrics = spark_jobs

        last_id = session.query(Metrics).order_by(Metrics.spark_id.desc()).first()

        if last_id is None:
            return 0

        #Close Session
        session.close()

        return last_id.spark_id 

#Process SPARK_data
def process_spark_jobs():
    while True:

        #Check for SPARK Jobs in the Queue
        if len(spark_jobs_queue) > 0:
            print(f'Number of SPARK Jobs in Queue: {len(spark_jobs_queue)}')
            spark_job = spark_jobs_queue.popleft()
            print(f'Processing SPARK Job {spark_job}')
            url = f'{DUMMY_LINK}/{spark_job.job_id}_{spark_job.scid}_{spark_job.metric}_{spark_job.source}'
            store_data(url)
            spark_id = get_latest_id()

            time.sleep(SPARK_DELAY)

            response = requests.post(spark_job.endpoint, json={'job_id': spark_job.job_id, 'url': url, 'spark_id': spark_id})
        else:
            print('No Spark Jobs in Queue')
            time.sleep(3)

# Define a POST endpoint for the SPARK API
@app.post("/api/analysis")
def get_spark_jobs(spark_data: SparkData):
    spark_jobs_queue.append(spark_data)
    return {"message": spark_data}


if __name__ == "__main__":
    # Start the queue processing thread
    thread = threading.Thread(target=process_spark_jobs ,daemon=True)
    thread.start()
    import uvicorn
    uvicorn.run(app, host=IP, port=PORT)