#ECQL emulator to process batch scripts 
import pandas as pd
import subprocess
from time import sleep
from datetime import datetime
import sys
import random 

def ECQL_emulator(ecql_cfg):
    #Extract data from batch file

    cfg_data = pd.read_csv(ecql_cfg, delimiter=';')
    num_of_batch_scripts = len(cfg_data['#AppName'])

    while True:
        #Generate random sleep time
        sleep_time = random.randint(10,30)
        
        #Get Random Batch Script to run
        batch_script_idx = random.randint(0, num_of_batch_scripts-1)
        batch_file = cfg_data['#AppName'][batch_script_idx]

        print(f'Executing batch file: {batch_file}')
        subprocess.run(f'{batch_file}')
        sleep(sleep_time)



if __name__ == "__main__":
    
    #get inputs
    ecql_cfg = sys.argv[1]

    #Start ECQL Emulator
    ECQL_emulator(ecql_cfg)




