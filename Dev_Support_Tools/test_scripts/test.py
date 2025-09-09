import streamlit as st
import pandas as pd
import numpy as np
import time

# Function to generate random data
def generate_data():
    return pd.DataFrame({
        'Time': pd.date_range(start='1/1/2025', periods=10, freq='T'),
        'Value': np.random.randn(10).cumsum()
    })

# Streamlit app
st.title("Real-time Plots in Different Tabs")

# Create tabs
tab1, tab2 = st.tabs(["Plot 1", "Plot 2"])

# Initialize session state for controlling updates
if 'run_plot1' not in st.session_state:
    st.session_state.run_plot1 = False
if 'run_plot2' not in st.session_state:
    st.session_state.run_plot2 = False

# Plot 1
with tab1:
    if st.button("Start Plot 1"):
        st.session_state.run_plot1 = True
    if st.button("Stop Plot 1"):
        st.session_state.run_plot1 = False

    chart_placeholder1 = st.empty()
    
    while st.session_state.run_plot1:
        # Generate new data for Plot 1
        data1 = generate_data()
        
        # Update the chart in Plot 1
        chart_placeholder1.line_chart(data1.set_index('Time'))

        # Wait for a few seconds before updating again
        time.sleep(1)

# Plot 2
with tab2:
    if st.button("Start Plot 2"):
        st.session_state.run_plot2 = True
    if st.button("Stop Plot 2"):
        st.session_state.run_plot2 = False

    chart_placeholder2 = st.empty()
    
    while st.session_state.run_plot2:
        # Generate new data for Plot 2
        data2 = generate_data()
        
        # Update the chart in Plot 2
        chart_placeholder2.line_chart(data2.set_index('Time'))

        # Wait for a few seconds before updating again
        time.sleep(1)