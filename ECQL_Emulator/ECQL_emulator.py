#ECQL emulator to process batch scripts 
import multiprocessing
import subprocess
from time import sleep
from datetime import datetime
import sys
import random 
from ecql_emulator_cfg import uri, metric_cfg

def ECQL_emulator(X):
    #Extract Inputs 
    uri = X[0]
    scid = X[1]
    

    while True:
        #Generate random sleep time
        sleep_time = random.randint(10,30)
        
        # Get the current time
        current_time = datetime.now()

        #Pick a random metric 
        batch_file = metric_cfg[random.choice(list(metric_cfg.keys()))]

        # Format the current time as a string
        t = current_time.strftime("%Y_%m_%d_%H%M%S")
        subprocess.run(f'{batch_file} {uri} {scid} {t}' )
        sleep(sleep_time)



if __name__ == "__main__":
    #get inputs
    config_data = sys.argv[1]
    

    #Initialzie multi processing pool
    pool = multiprocessing.Pool()

 
    
    #Start multi processing jobs
    pool.map(ECQL_emulator, [(uri,10),
                             (uri,20),
                             (uri,30)])
