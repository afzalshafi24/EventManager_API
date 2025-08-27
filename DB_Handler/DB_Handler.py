from sqlalchemy import create_engine, distinct, inspect, MetaData, select
from sqlalchemy.orm import sessionmaker
import os
from datetime import datetime
import sys
import logging
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy import Column, String, Float, DateTime, Integer, Boolean

# Add parent directory to Python path
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))


#Time String Format
TIME_STR_FORMAT = "%Y-%m-%d %H:%M:%S"
TIME_STR_FORMAT_A = "%d-%b-%Y %H:%M:%S"

Base  = declarative_base()

# Define the Event Data DB model
class Event_Data(Base):
    __tablename__ = 'event_alerts'
    unique_index = Column(Integer, primary_key=True, autoincrement=True)
    scid = Column(Integer, nullable=False)
    event_time = Column(DateTime, nullable=False)
    event_rule_id = Column(Integer, nullable=True)
    event_name = Column(String, nullable=False)
    event_rule = Column(String, nullable=False)
    event_src = Column(String, nullable=False)
    spark_script = Column(String, nullable=True)
    spark_report_id = Column(Integer, nullable=True)
    gem_full_path = Column(String, nullable=True)


class DB_Handler():
    def __init__(self, db_url) :
        #Store db file
        self.db_url = db_url
        self.logger = logging.getLogger(__name__)
        self.logger.info(f'Using DB address : {self.db_url}')
      
         
    def initialize_database(self):
        #Initializes DB
        self.engine = create_engine(self.db_url)
        Base.metadata.create_all(self.engine)
        self.logger.info(f"Database '{self.db_url}' initialized with given tables")

    def store_data(self, metric_data):
        #Stores a row of data into database of a given database
    
        #Create Session
        session = self.create_session()

        #Get Datetime Object
        dt_object = self.get_dt_object(metric_data.event_time)

        # Create a new instance of the Metrics model by source
        event_data_insert = Event_Data(scid= metric_data.scid,
                                       event_time=dt_object,
                                       event_rule_id= metric_data.event_rule_id,
                                       event_name=metric_data.event_name,
                                       event_rule=metric_data.event_rule,
                                       event_src=metric_data.event_src,
                                       spark_script=metric_data.spark_script,
                                       gem_full_path= metric_data.gem_full_path)

        
        # Add the new instance to the session
        session.add(event_data_insert)

        # Commit the session to save the data to the database
        session.commit()

        # Close the session
        session.close()
    
    def store_heartbeat(self, metric_data):
        #Store Heartbeat Messages into Database

        #Create Session
        session = self.create_session()

        #Get Datetime Object
        dt_object = self.get_dt_object(metric_data.start_time)

        #Get DB Model for Source Specific Heartbeat Messages
        db_model = db_model_config["ECQL_Heartbeat"]

        # Check if the record exists
        record = session.query(db_model).filter(db_model.scid == metric_data.scid).first()
        
     
        if record:
            # Update the existing record
            record.start_time = dt_object
            self.logger.info(f"Updated {metric_data.source} Heartbeat for Scid {metric_data.scid} with new Time {metric_data.start_time}.")
        else:
            # Create a new record
            new_record = db_model(scid=metric_data.scid, start_time=dt_object)
            session.add(new_record)
            self.logger.info(f"Added new {metric_data.source} Heartbeat for Scid {metric_data.scid} with new Time {metric_data.start_time}.")
        
        # Commit the session
        session.commit()
        
    def get_dt_object(self, start_time):
         # Convert the time string to a datetime object
        try:
            dt_object = datetime.strptime(start_time, TIME_STR_FORMAT)
            self.logger.info(f"Using Time Format {TIME_STR_FORMAT}")
            
        except:
            dt_object = datetime.strptime(start_time, TIME_STR_FORMAT_A)
            self.logger.info(f"Using Time Format {TIME_STR_FORMAT_A}")
             
        return dt_object

    def create_session(self):
        #Create Session from engine 
        Session = sessionmaker(bind=self.engine)
        return Session()
        
    def get_all_data(self):
        #Queries all the data from the database
        
        #Create Session
        session = self.create_session()

        #Reflect the database schema
        metadata = MetaData()
        metadata.reflect(bind=self.engine)

        data = {}

        #Loop through table names and store in structure
        for table_name, table in metadata.tables.items():
            
            data[table_name] = {}

            #Get all column names
            columns = [column.name for column in table.columns] 
            for col_name in columns:

                db_model = getattr(db_model_config[table_name], col_name)

                data[table_name][col_name] = [item[0] for item in session.query(db_model).all()]
            
        
        #Close session
        session.close()

        #Return data
        return data

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
        Metrics =Event_Data

        last_id = session.query(Metrics).order_by(Metrics.unique_index.desc()).first()

        if last_id is None:
            return 0

        #Close Session
        session.close()

        return last_id.unique_index 
    
    def get_unique_elements(self, col_name, table_name):
        #Get Unique values in a database
        
        #Create Session
        session = self.create_session()

        Metrics = db_model_config[table_name]

        # Query for unique elements dynamically
        unique_values = session.query(distinct(getattr(Metrics, col_name))).all()

        return [item[0] for item in unique_values]
    

    def update_column_by_id(self, id, col_name, new_value):
        #Updates the url_link with new value

        #Create Session
        session = self.create_session()

        Metrics = Event_Data

         # Query for the entry with the specified ID
        entry = session.query(Metrics).filter(Metrics.unique_index == id).first()

        if entry:
            # Update the specific column
            setattr(entry, col_name, new_value)
            session.commit()  # Commit the changes to the database
            self.logger.info(f"Updated entry ID {id} {col_name} column with new value: {new_value}")
        else:
            self.logger.warning(f"No entry found with ID {id}")

        #Close Session
        session.close()

    def get_table(self):
        #Get All database values for a given table

        #Get DB model
        db_model = Event_Data

        #Create Session 
        session = self.create_session()

        table_data = session.query(db_model).all()

        return table_data

    
    def find_new_events(self, src_name):
        #Find new events for given src that do not have a spark_ID associated with it
        
        #Create session
        session = self.create_session()

        #Get data by source
        data = session.query(Event_Data).filter(Event_Data.event_src == src_name, Event_Data.spark_report_id == None).all()

        session.close()

        return data

    def get_table_names(self):
        #Get a list of table names in given database
        
        #Create an inspector
        inspector = inspect(self.engine)

        #Get list of table names and return it
        table_names = inspector.get_table_names()

        return table_names






