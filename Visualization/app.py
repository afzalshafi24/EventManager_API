import streamlit as st
import pandas as pd
import numpy as np
import time
import requests
from app_config import URI, DF_URI,  METRIC_CFG, SCIDS, MIN_THRESH, MAX_THRESH
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

def fetch_dataframe():
    #Fetch DataFrame from API
    response = requests.get(DF_URI)
    if response.status_code == 200:
        data = response.json()
        return pd.DataFrame.from_dict(data['data'], orient='columns')
    
    else:
         print(f"Error accessing items endpoint: {response.status_code}, Response: {response.text}")
         return None


def fetch_data(scid, metric):
    #Fetch Data from API
    response = requests.get(URI, params={"scid": scid, "metric": metric})
    if response.status_code == 200:
        data = response.json()
        return len(data['data'])
    
    else:
         print(f"Error accessing items endpoint: {response.status_code}, Response: {response.text}")
         return None

def get_scid_metric_data(scids, metric):
    #Create a list of metric data for listed scids
    smd = []
    for s in scids:
        try:
            results = fetch_data(s, metric)
        except:
            results = st.session_state[metric][f'SCID{s}'][-1]
        smd.append(results)
    return smd

def init_session_state_data():
    #Initialize session state data
    session_state_data = {}
    for m in METRIC_CFG.keys():
        session_state_data[m] = {}
        session_state_data[m]['time'] = [datetime.now()]
        for s in COLUMN_NAMES:
            session_state_data[m][s] = [0]
    return session_state_data

def update_session_state_data(smd, metric):
    st.session_state.data[metric]['time'].append(datetime.now())

    for idx,s in enumerate(COLUMN_NAMES):
        st.session_state.data[metric][s].append(smd[idx])

# Streamlit app
# Set the page config to use dark mode
st.set_page_config(page_title="ECQL Triggers", layout="centered", initial_sidebar_state="expanded")
st.title(f"ECQL Metric Triggers")

COLUMN_NAMES = [f'SCID{s}' for s in SCIDS]

#Create Sidebar 
st.sidebar.header("Data Filters:")

#Create a SCID filter
selected_scids = st.sidebar.multiselect("Select SCIDs to Filter:", options=SCIDS, default=SCIDS)

selected_metric = st.sidebar.selectbox("Select Metric to Plot:", options=METRIC_CFG.keys())

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

main_tab, spark_tab = st.tabs(['ECQL Triggers', 'SPARK'])
print(f'Look at {main_tab}')

with main_tab:
    print(main_tab)
    # Create a placeholder for the Metric table DataFrame
    metric_tigger_table = st.empty()

    # Create a placeholder for the Metric plot 
    metric_plot = st.empty()
    # Main loop to update the DataFrame
    
    # Update the DataFrame with all the metric data
    
    for idx , metric in enumerate(METRIC_CFG.keys()):
        smd = get_scid_metric_data(SCIDS, metric)
        #Update Session State data
        update_session_state_data(smd, metric)
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


    df_plot = pd.DataFrame(st.session_state.data[selected_metric])
    #df_plot.set_index('time', inplace=True)
    # Display title and labels
    # Create the Altair chart
    try:
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

    selected_metric_spark = st.multiselect("Select Metric to Plot:", options=METRIC_CFG.keys())
    big_df = fetch_dataframe()
    
    # Filter the DataFrame
    filtered_big_df = big_df[
        big_df['scid'].isin(selected_scids) & big_df['metric_name'].isin(selected_metric_spark)
    ]
    # Apply the function to the URL column
    filtered_big_df['url'] = filtered_big_df['url'].apply(make_clickable)

    # Display the DataFrame as HTML
    st.markdown(filtered_big_df.to_html(escape=False), unsafe_allow_html=True)   
        



