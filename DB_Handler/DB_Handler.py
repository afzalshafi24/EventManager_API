from sqlalchemy import create_engine, Column, String, Float, DateTime, Integer, distinct
from sqlalchemy.orm import sessionmaker
import os
from sqlalchemy.ext.declarative import declarative_base
from datetime import datetime
import pandas as pd 

# Create a base class for declarative models

Base  = declarative_base()

TIME_STR_FORMAT = "%Y-%m-%d %H:%M:%S"

# Define the Metrics model
class Metrics(Base):
    __tablename__ = 'metrics'
    
    id = Column(Integer, primary_key=True, autoincrement=True)
    datetime = Column(DateTime, nullable=False)
    scid = Column(Integer, nullable=False)
    metric_name = Column(String, nullable=False)
    value = Column(Float, nullable=True)
    persistence = Column(String, nullable=False)
    persistence_samples = Column(Integer, nullable=False)
    on_change = Column(String, nullable=True)
    source = Column(String, nullable=False)
    url = Column(String, nullable=True)


class DB_Handler():
    def __init__(self, db_file) :
        #Store db file
        self.db_file = db_file

    def check_database(self):
        #Check if database exists , if not create and initialze
        self.engine = create_engine(f'sqlite:///{self.db_file}')

        if not os.path.exists(self.db_file):
            print(f"Database '{self.db_file}' does not exist. Initializing...")
            self.initialize_database()
           
         
    def initialize_database(self):
        #Initializes DB
        Base.metadata.create_all(self.engine)
        print(f"Database '{self.db_file}' initialized with table 'metrics'.")

    def store_data(self, metric_data):
        #Stores a row of data into database
        # Create a session

        print(metric_data.t)
        
        #Create Session
        session = self.create_session()

        # Convert the time string to a datetime object
        dt_object = datetime.strptime(metric_data.t, TIME_STR_FORMAT)

        # Create a new instance of the Metrics model
        new_metric = Metrics(datetime=dt_object,
                             scid=int(metric_data.scid), 
                             metric_name=metric_data.metric_name, 
                             value=metric_data.value,
                             persistence = metric_data.persistence,
                             persistence_samples = metric_data.persistence_samples,
                             on_change = metric_data.on_change,
                             source = metric_data.source,
                             url=None)

        # Add the new instance to the session
        session.add(new_metric)

        # Commit the session to save the data to the database
        session.commit()

        # Close the session
        session.close()
    
    def create_session(self):
        #Create Session from engine 
        Session = sessionmaker(bind=self.engine)
        return Session()
        
    def get_data(self, scid: int, metric: str):
        #Get Data for a given scid

        #Create Session
        session = self.create_session()

        #Query Data based off scid
        data = session.query(Metrics).filter(Metrics.scid == scid, Metrics.metric_name == metric).all()

        #Close session
        session.close()

        return data
    
    def get_data_by_time(self, scid:int, metric_name:str, start_time:datetime, end_time:datetime):
        #Query data by time window

        # Convert to datetime for querying
        start_datetime_str = datetime.combine(start_time, datetime.min.time())
        end_datetime_str = datetime.combine(end_time, datetime.max.time())

        #Create Session
        session = self.create_session()

        #Query Data based off scid, metric, and time window
        data = session.query(Metrics).filter(
        Metrics.datetime.between(start_datetime_str, end_datetime_str),
        Metrics.metric_name == metric_name,
        Metrics.scid == scid).all()

        #Close Session
        session.close()

        return data
    
    def get_latest_id(self):
        #Get the latest job_id 

        #Create Session
        session = self.create_session()

        last_id = session.query(Metrics).order_by(Metrics.id.desc()).first()

        if last_id is None:
            return 0

        #Close Session
        session.close()

        return last_id.id 
    
    def get_unique_elements(self, col_name):
        #Get Unique values in a database
        
        #Create Session
        session = self.create_session()

        # Query for unique elements dynamically
        unique_values = session.query(distinct(getattr(Metrics, col_name))).all()

        return [item[0] for item in unique_values]
    

    def update_url_by_id(self, id, new_value):
        #Updates the url_link with new value

        #Create Session
        session = self.create_session()

         # Query for the entry with the specified ID
        entry = session.query(Metrics).filter(Metrics.id == id).first()

        if entry:
            # Update the specific column
            entry.url = new_value
            session.commit()  # Commit the changes to the database
            print(f"Updated entry ID {id} with new value: {new_value}")
        else:
            print(f"No entry found with ID {id}")

        #Close Session
        session.close()

    def get_table_as_dataframe(self):
        #Get Whole Table as dataframe
        df = pd.read_sql('SELECT * FROM metrics', con=self.engine)
        return df 





