# Create a base class for declarative models
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy import Column, String, Float, DateTime, Integer


#Time String Format
TIME_STR_FORMAT = "%Y-%m-%d %H:%M:%S"

Base  = declarative_base()

# Define the ECQL_DB model
class ECQL_Data(Base):
    __tablename__ = 'ECQL'
    id = Column(Integer, primary_key=True, autoincrement=True)
    t = Column(DateTime, nullable=False)
    scid = Column(Integer, nullable=False)
    metric_name = Column(String, nullable=False)
    source = Column(String, nullable=False)
    url = Column(String, nullable=True)

# Define the ASTRA_DB model
class ASTRA_Data(Base):
    __tablename__ = 'ASTRA'
    id = Column(Integer, primary_key=True, autoincrement=True)
    scid = Column(Integer, nullable=False)
    start_time = Column(String, nullable=False)
    duration = Column(Integer, nullable=False)
    t = Column(DateTime, nullable=False)
    metric_name = Column(String, nullable=False)
    value = Column(Float, nullable=True)
    persistence_flag = Column(String, nullable=False)
    num_persistence_samples = Column(Integer, nullable=False)
    on_change_flag = Column(String, nullable=True)
    threshold_value = Column(Float, nullable=True)
    plotLink = Column(String, nullable=True)
    description = Column(String, nullable=True)
    source = Column(String, nullable=False)
    url = Column(String, nullable=True)

#Allocate Source to corresponding DB Model
db_model_config = {'ECQL' : ECQL_Data,
                   'ASTRA' : ASTRA_Data}