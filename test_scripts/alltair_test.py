import streamlit as st
import altair as alt
import pandas as pd
import numpy as np
import time

# Set the title of the app
st.title("Live Plotting with Altair")

# Initialize an empty DataFrame to store the data
data = pd.DataFrame(columns=["Time", "Plot1", "Plot2", "Plot3"])

# Function to generate new data
def generate_data():
    current_time = time.time()
    plot1_value = np.sin(current_time)  # Example data for Plot 1
    plot2_value = np.cos(current_time)  # Example data for Plot 2
    plot3_value = np.sin(current_time) + np.cos(current_time)  # Example data for Plot 3
    return current_time, plot1_value, plot2_value, plot3_value

# Create a placeholder for the chart
chart_placeholder = st.empty()

# Live plotting loop
while True:
    # Generate new data
    current_time, plot1_value, plot2_value, plot3_value = generate_data()
    
    # Append new data to the DataFrame
    new_data = pd.DataFrame({
        "Time": [current_time],
        "Plot1": [plot1_value],
        "Plot2": [plot2_value],
        "Plot3": [plot3_value]
    })
    data = pd.concat([data, new_data], ignore_index=True)
    print(data)
    # Create the Altair chart
    chart = alt.Chart(data).transform_fold(
        ['Plot1', 'Plot2', 'Plot3'],
        as_=['Plot', 'Value']
    ).mark_line().encode(
        x='Time:T',
        y='Value:Q',
        color='Plot:N'
    ).properties(
        width=700,
        height=400
    )

    # Display the chart
    chart_placeholder.altair_chart(chart)

    # Sleep for a short duration to simulate real-time data updates
    time.sleep(1)