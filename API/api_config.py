
from pydantic import BaseModel

LOG_DIR = r'logs'
LOG_FILENAME = 'api.log'
DB_FILE = "metric_triggers.db"
IP = "127.0.0.1"
PORT = 8000
SPARK_URI = r'http://localhost:8001/api/analysis'
SPARK_ENDPOINT = r'http://localhost:8000/spark_endpoint'


# Define a Pydantic model for the request body for ECQL Alerts
class ECQL_Request(BaseModel):
    scid: int
    t: str
    metric_name: str
    source : str

class ASTRA_Request(BaseModel):
    scid: int
    start_time: str
    duration: int
    t: str
    metric_name: str
    value : float
    persistence_flag: str
    num_persistence_samples: int
    on_change_flag: str
    threshold_value : float
    plotLink : str
    description: str
    source : str

class SparkRequest(BaseModel):
    job_id: int
    url: str