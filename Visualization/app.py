import streamlit as st
import pandas as pd
import numpy as np
import time
import requests
from app_config import URI, MIN_THRESH, MAX_THRESH
from datetime import datetime
import pandas as pd
import altair as alt

def check_thresholds(val):
    
    if not isinstance(val, int):
        return None
    else:
        if val > MAX_THRESH:
            return 'background-color: red'
        elif val > MIN_THRESH:
            return 'background-color: yellow'
        else:
            return 'background-color: green'

def extract_response_status(response):
    #Extract Response Status
    if response.status_code == 200:
        return response.json()
    else:
         print(f"Error accessing items endpoint: {response.status_code}, Response: {response.text}")
         return None

def fetch_dataframe(source):
    #Fetch DataFrame from API
    response = requests.get(URI + '/get_dataframe', params={"table_name":source})
    data = extract_response_status(response)
    if data is not None:
        return pd.DataFrame.from_dict(data['data'], orient='columns')
    else:
        return data

def fetch_unique_scids_metric(col_name, source):
    #Get Unique Elements in a database for a given table
    response = requests.get(URI + '/get_unique_db_vals', params={"col_name": col_name, 
                                                                 "table_name": source})
    data = extract_response_status(response) 
    
    if extract_response_status(response) is not None:
        return data['data']
    else:
        return data

def fetch_data(scid, metric, source):
    #Fetch Data from API
    response = requests.get(URI + '/get_data', params={"scid": scid, "metric": metric, "table_name":source})
    data = extract_response_status(response)
    if data is not None:
        return len(data['data'])
    else:
        return None 

def get_source_names():
    #Fetch Source Name from API
    response = requests.get(URI + '/get_table_names')
    data = extract_response_status(response)
    if data is not None:
        return data['data']
    else:
        return None 

def get_scid_metric_data(scids, metric, source):
    #Create a list of metric data for listed scids
    smd = []
    for s in scids:
        try:
            results = fetch_data(s, metric, source)
        except:
            results = st.session_state[source][metric][f'SCID{s}'][-1]
        smd.append(results)
    return smd

def init_session_state_data():
    #Initialize session state data
    session_state_data = {}
   
    for sc in SOURCE_NAMES:
        print(sc)
        session_state_data[sc] = {}
        scids =  fetch_unique_scids_metric('scid', sc)
        metrics = fetch_unique_scids_metric('metric_name', sc)
        col_names = [f'SCID{s}' for s in scids]
        
        if metrics is None:
            continue
        
        for m in metrics:
            session_state_data[sc][m] = {}
            session_state_data[sc][m]['time'] = [datetime.now()]
            for c in col_names:
                session_state_data[sc][m][c] = [0]
    return session_state_data

def update_session_state_data(smd, metric, source):
    if metric in st.session_state.data[source]:
        st.session_state.data[source][metric]['time'].append(datetime.now())

        for idx,s in enumerate(COLUMN_NAMES):
            if s in st.session_state.data[source][metric]: 
                st.session_state.data[source][metric][s].append(smd[idx])
            else:
                st.session_state.data[source][metric][s] = [smd[idx]]   
    else:
        st.session_state.data[source][metric] = {}
        st.session_state.data[source][metric]['time'] =  [datetime.now()]
        for idx,s in enumerate(COLUMN_NAMES):
            st.session_state.data[source][metric][s] = [smd[idx]]   
# Streamlit app
# Set the page config to use dark mode
st.set_page_config(page_title="Metric-Based Event Triggers Dashboard", layout="centered", initial_sidebar_state="expanded")
st.title(f"Metric-Based Event Triggers Dashboard")



SOURCE_NAMES  = get_source_names()

print(SOURCE_NAMES)

#Create Sidebar 
st.sidebar.header("Data Filters:")

#Create a Source_Selection
selected_source = st.sidebar.selectbox("Select Source:", options=SOURCE_NAMES)

SCIDS = fetch_unique_scids_metric('scid', selected_source)
print(SCIDS)
METRICS = fetch_unique_scids_metric('metric_name', selected_source)
COLUMN_NAMES = [f'SCID{s}' for s in SCIDS]
#Create a SCID filter
selected_scids = st.sidebar.multiselect("Select SCIDs to Filter:", options=SCIDS, default=SCIDS)

#Create a Metric_Selection
selected_metric = st.sidebar.selectbox("Select Metric to Plot:", options=METRICS)

df = pd.DataFrame(columns=['METRIC_NAME'] + COLUMN_NAMES, index=[0])

if selected_scids:
    plot_column_names = [f'SCID{s}' for s in selected_scids]
    
# Initialize session state for storing data
if 'data' not in st.session_state:
    st.session_state.data = init_session_state_data()
    

# Create a placeholder for the Metric plot LABEL
metric_plot_label = st.empty()


#Create Threshold Line for Metric
# Create threshold lines with legends
threshold_line1 = alt.Chart(pd.DataFrame({'y': [MAX_THRESH]})).mark_rule(
    color='red',
    strokeDash=[5, 5]  # Dotted line
).encode(
    y='y:Q',
    tooltip=alt.Tooltip(['y:Q'], title='Max Threshold')
).properties(
    title='Max Threshold'
)

threshold_line2 = alt.Chart(pd.DataFrame({'y': [MIN_THRESH]})).mark_rule(
    color='yellow',
    strokeDash=[5, 5]  # Dotted line
).encode(
    y='y:Q',
    tooltip=alt.Tooltip(['y:Q'], title='Min Threshold')
).properties(
    title='Min Threshold'
)

main_tab, spark_tab = st.tabs(['Event Triggers', 'SPARK Jobs'])
print(f'Look at {main_tab}')

with main_tab:
    print(main_tab)
    # Create a placeholder for the Metric table DataFrame
    metric_tigger_table = st.empty()

    # Create a placeholder for the Metric plot 
    metric_plot = st.empty()
    # Main loop to update the DataFrame
    
    # Update the DataFrame with all the metric data
    
    for idx , metric in enumerate(METRICS):
        smd = get_scid_metric_data(SCIDS, metric, selected_source)
        #Update Session State data
        update_session_state_data(smd, metric, selected_source)
        df.loc[idx] = [metric] +  smd

    # Display the DataFrame with dynamic background colors
    try:
        styled_df = df.style \
            .applymap(check_thresholds, subset=COLUMN_NAMES) \
            .hide(axis="index")
    except:
        pass

    # Update the placeholder with the styled DataFrame
    metric_tigger_table.dataframe(styled_df, use_container_width=True)


    
    #df_plot.set_index('time', inplace=True)
    # Display title and labels
    # Create the Altair chart
    try:
        df_plot = pd.DataFrame(st.session_state.data[selected_source][selected_metric])
        chart = alt.Chart(df_plot).transform_fold(
            plot_column_names,
            as_=['Plot', 'Value']
        ).mark_line().encode(
            x=alt.X('time:T', title='Time'),
            y=alt.Y('Value:Q', title='Number of Alerts'),
            color='Plot:N'
        ).properties(
            title=selected_metric,
            width=700,
            height=400
        )
    except:
        # Create an empty Altair chart
        chart = alt.Chart(pd.DataFrame(columns=['x', 'y'])).mark_line().encode(
            x=alt.X('time:T', title='Time'),
            y=alt.Y('Value:Q', title='Number of Alerts'),
        ).properties(
            title='NO SCIDS Selected',
            width=700,
            height=400
)

    # Display the chart
    metric_plot.altair_chart(chart + threshold_line1 + threshold_line2)

# Function to create clickable links
def make_clickable(url):
    return f'<a href="{url}" target="_blank">{url}</a>'

 

with spark_tab:
    #Create a SCID and metric filter for SPARK tab
    #selected_scids_spark = st.multiselect("Select SCIDs to Filter:", options=SCIDS, default=SCIDS)

    selected_metric_spark = st.multiselect("Select Metric(s):", options=METRICS, default=METRICS)
    big_df = fetch_dataframe(selected_source)
    
    # Filter the DataFrame
    filtered_big_df = big_df[
        big_df['scid'].isin(selected_scids) & big_df['metric_name'].isin(selected_metric_spark)
    ]
    # Apply the function to the URL column
    filtered_big_df['url'] = filtered_big_df['url'].apply(make_clickable)

    # Display the DataFrame as HTML
    st.markdown(filtered_big_df.to_html(escape=False), unsafe_allow_html=True)   
        



