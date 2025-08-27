
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from time import sleep
import threading
import os
import logging 
from pathlib import Path
from logging.handlers import RotatingFileHandler
import sys

# Add current directory to Python path
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

from src import (ORGINS, LOG_DIR, LOG_FILENAME, IP, PORT, cfg_file, db_handler)
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


    #Check to see if logging path exist , if not make one
    if not os.path.exists(LOG_DIR):
        os.makedirs(LOG_DIR)

    log_name = Path(os.path.join(LOG_DIR, LOG_FILENAME))

    # Configure logging
    log_handler = RotatingFileHandler(log_name.as_posix(), maxBytes=10*1024*1024, backupCount=5)
    log_handler.setFormatter(logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(funcName)s - %(message)s'))

    logging.basicConfig(level=logging.INFO, handlers=[log_handler, logging.StreamHandler()])


    #Get Uvicorn Logger 
    uvicorn_logger = logging.getLogger("uvicorn")
    uvicorn_logger.setLevel(logging.INFO)
    uvicorn_logger.addHandler(log_handler)
    logger = logging.getLogger(__name__)

    logger.info(f'Using config file {cfg_file}')

    #Report config to log
    logger.info(f'Starting Application on http://{IP}:{PORT}')

    #Initialize Database
    db_handler.initialize_database()

    # Run the app with: uvicorn app:app --reload

    # Start the queue processing thread
    data_ingestion_thread = threading.Thread(target=data_ingestion_processing, daemon=True)
    data_ingestion_thread.start()

    data_polling_thread = threading.Thread(target=data_polling_processing, daemon=True)
    data_polling_thread.start()

    import uvicorn
    uvicorn.run(app, host=IP, port=PORT, log_level="info")

if __name__ == "__main__":
    main()    