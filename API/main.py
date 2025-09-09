
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
import threading
import os
import logging
from logging.handlers import RotatingFileHandler
import logging 
from pathlib import Path

#Initialize Logging
#Check to see if logging path exist , if not make one
if not os.path.exists('logs'):
    os.makedirs('logs')

log_name = Path(os.path.join('logs', 'event_mgr_api.log'))

# Configure logging
log_handler = RotatingFileHandler(log_name.as_posix(), maxBytes=10*1024*1024, backupCount=5)
log_handler.setFormatter(logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(funcName)s - %(message)s'))

logging.basicConfig(level=logging.INFO, handlers=[log_handler, logging.StreamHandler()])

#Get Uvicorn Logger 
uvicorn_logger = logging.getLogger("uvicorn")
uvicorn_logger.setLevel(logging.INFO)
uvicorn_logger.addHandler(log_handler)
logger = logging.getLogger(__name__)


#EventManager_API Depenency Imports
from src import (ORGINS, IP, PORT, cfg_file)
from src.routes import event_mgr_router
from src.utils import data_ingestion_processing, data_polling_processing


def main():
    # Create an instance of the FastAPI class
    app = FastAPI()

    #Include the EventManager Routers to FastpAPI app
    app.include_router(event_mgr_router)

    # Add CORS middleware
    app.add_middleware(
        CORSMiddleware,
        allow_origins=ORGINS,  # Allows specified origins
        allow_credentials=True,
        allow_methods=["*"],  # Allows all methods (GET, POST, etc.)
        allow_headers=["*"],  # Allows all headers
    )
    
    logger.info(f'Using config file {cfg_file}')

    #Report config to log
    logger.info(f'Starting Application on http://{IP}:{PORT}')


    # Start the queue processing thread
    data_ingestion_thread = threading.Thread(target=data_ingestion_processing, daemon=True)
    data_ingestion_thread.start()

    #Start Data Polling Processing
    data_polling_thread = threading.Thread(target=data_polling_processing, daemon=True)
    data_polling_thread.start()

    #Start Uvicorn Application
    import uvicorn
    uvicorn.run(app, host=IP, port=PORT, log_level="info")

if __name__ == "__main__":
    main()    