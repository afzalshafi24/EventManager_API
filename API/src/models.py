
from pydantic import BaseModel
from typing import Optional

# Define a Pydantic model for the request body for Event Alerts
class eventRequest(BaseModel):
    scid: int
    event_time: str
    event_rule_id: int
    event_name: str
    event_rule: str
    event_src: str
    spark_script: Optional[str]
    gem_full_path: Optional[str]

# Define a Pydantic model for the request body for Spark Requests
class sparkRequest(BaseModel):
    job_id: int
    url: str
    spark_id: int