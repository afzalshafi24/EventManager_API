
from pydantic import BaseModel

LOG_DIR = r'logs'
LOG_FILENAME = 'api.log'
DB_FILE = "metric_triggers.db"
IP = "127.0.0.1"
PORT = 8000
SPARK_URI = 'http://localhost:8001'
SPARK_ENDPOINT = 'http://localhost:8000/spark_endpoint'


# Define a Pydantic model for the request body
class MetricData(BaseModel):
    scid: int
    t: str
    metric_name: str
    value : float
    persistence: str
    persistence_samples: int
    on_change: str
    source : str

class SparkRequest(BaseModel):
    job_id: int
    url: str