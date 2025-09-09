

# EventManager_API Directory Structure

- **EventManager_API**
  - **main.py**: Main Application for EventManager_API, source that gets compiled with Pyinstaller
  - **api_config.json**: Input Config for main.py
  - **src**: Directory of support source code for EventManager_API
    - **DB_Handler**: Class for interacting with database
    - **models.py**: FastAPI model definitions
    - **routes.py**: Endpoint definitions
    - **Spark_Manager.py**: Module to handle sending jobs to Spark
    - **utils.py**: Support utility functions used by EventManager_API

- **Dev_Support_Tools**: Tools used for EventManager_API and Testing
  - **Data_Ingestion_Sim**: Support tools to send Post Requests events to EventManager_API
  - **ECQL_Emulator**: Support tools to simulate ECQL and to test/dev Data Polling
  - **SPARK_Emulator**: Support tools to simulate SPARK input and output
  - **test_scripts**: Misc scripts for testing and dev
  - **Visualization**: Support tools to visualize events processed by EventManager_API

- **requirement.txt**: A list of Python dependencies for EventManager_API
- **internal_libraries**: Internal libraries needed

# EventManager_API Run Procedure
### Dependencies:
  1. Oracle or PostGre Database server running if using non-SQLite databases
  2. Eureka Server if EventManager_API is configured to register to Eureka

## EventManager_API Run Procedure (Non-Compiled)


Navigate to API directory and run the following command in Powershell or Windows CMD

`python .\main.py`

## EventManager_API Run Procedure (Compiled)

Follow the deployment proceduce documented in the link below to compile EventManager_API

[EventManager_API Deployment Procedure](#EventManager_API-Run-Deployment-Procedure)

Once compiled, navigate to API/pyinstaller/dist/main folder and run the following command in Powershell or Windows CMD

`.\main.exe`

# EventManager_API Run Deployment Procedure

## Step 1: Clone source code from Bitbucket Repo
Clone the EventManager_API repo using the following command from Windows, Powershell or a GIT Bash Terminal

`git clone https://github.com/afzalshafi24/EventManager_API.git`

## Step 2: Create a virtual Python Environment for the Build
Open up Windows Cmd window and navigate to your eventmanager_api clone

Create a base python virtual environment with the following command

`python -m venv venv`

## Step 3: Activate the python virtual environment
Active the python virtual environment with the following command, from the root directory of the cloned repo

`.\venv\Scripts\activate`

## Step 4: Install Dependencies for EventManager_API
In the activated python virtual environment, identify the requirement.txt file in the root directory of your and run the following command in a Windows or Powershell terminal 

`pip install -r .\requirement.txt`

In addition, you will need to install the Eureka Client Library to register the EventManager_API service, run the following command from root directory to install in your active python environment

`pip install .\internal_libaries\eureka_client-0.1.5-py3-none-any.whl`

## Step 5: Install Pyinstaller in the virtual python environment
In the python virtual environment install pyinstaller with the following command:

`pip install pyinstaller`

## Step 6: Build the EventManager_API with Pyinstaller
In the Windows or Powershell terminal, navigate to the API folder from the root repo folder

Create a folder called "pyinstaller" and navigate to it

Run pyinstaller on the main.py script located in API

All the commands for Step 6 are shown below:

```
cd API
mkdir pyinstaller
cd .\pyinstaller\
pyinstaller --add-data "..\api_config.json;." ..\main.py
```

## Step 7: Run EventManager_API executable build
From the pyinstaller directory, make a copy of the api_config,json file located in the API directory in API/pyinstaller/dist/main

Navigate to the pyinstaller/dist/main folder

Run the main.exe file
All the commands for Step 7 are shown below:
```
cd .\dist\main\
.\main.exe
```

## Step 8: Verify EventManager_API executable build
Once the executable has booted, verify the program does not crash. You should constanly see "No Data in Queue" constantly being printed to the console the main.exe in running on
