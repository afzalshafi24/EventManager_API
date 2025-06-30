from sqlalchemy import create_engine, distinct, inspect
from sqlalchemy.orm import sessionmaker
import os
from datetime import datetime
import pandas as pd 
import sys

# Add parent directory to Python path
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from DB_Handler.db_config import Base,TIME_STR_FORMAT, db_model_config



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
        print(f"Database '{self.db_file}' initialized with given tables")

    def store_data(self, metric_data):
        #Stores a row of data into database of a given database
    
        #Create Session
        session = self.create_session()

        # Convert the time string to a datetime object
        dt_object = datetime.strptime(metric_data.t, TIME_STR_FORMAT)

        # Create a new instance of the Metrics model by source
        new_metric = db_model_config[metric_data.source]()

        #Convert metric data to dictionary
        mdata = metric_data.__dict__

        #set URL to None
        mdata['url'] = None

        #Populate db model with the metric data
        for key, value in mdata.items():
            if key == 't':
                value = dt_object 
            setattr(new_metric, key, value)

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
        
    def get_data(self, scid: int, metric: str, table_name:str):
        #Get Data for a given scid

        #Create Session
        session = self.create_session()

        Metrics = db_model_config[table_name]

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
    
    def get_latest_id(self, source):
        #Get the latest job_id 

        #Create Session
        session = self.create_session()
        Metrics = db_model_config[source]

        last_id = session.query(Metrics).order_by(Metrics.id.desc()).first()

        if last_id is None:
            return 0

        #Close Session
        session.close()

        return last_id.id 
    
    def get_unique_elements(self, col_name, table_name):
        #Get Unique values in a database
        
        #Create Session
        session = self.create_session()

        Metrics = db_model_config[table_name]

        # Query for unique elements dynamically
        unique_values = session.query(distinct(getattr(Metrics, col_name))).all()

        return [item[0] for item in unique_values]
    

    def update_url_by_id(self, id, new_value, tbl_name):
        #Updates the url_link with new value

        #Create Session
        session = self.create_session()

        Metrics = db_model_config[tbl_name]

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

    def get_table_as_dataframe(self, table_name):
        #Get Whole Table as dataframe
        df = pd.read_sql(f'SELECT * FROM {table_name}', con=self.engine)
        return df 
    
    def get_table_names(self):
        #Get a list of table names in given database
        
        #Create an inspector
        inspector = inspect(self.engine)

        #Get list of table names and return it
        table_names = inspector.get_table_names()

        return table_names






