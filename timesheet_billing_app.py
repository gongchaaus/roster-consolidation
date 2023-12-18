import pandas as pd
import time
import streamlit as st

#Date & Time
from datetime import datetime, timedelta

#for making API calls
import http.client
import json



import io

st.title('Timesheet & Billing')

uploaded_files = st.file_uploader("Choose Files", accept_multiple_files = True)

# Create an empty container
output = st.empty()

# if uploaded_files is not None:
if len(uploaded_files) > 0:
    ts, bl, ot, an, bo = calc_timesheets_n_billings(uploaded_files)

    buffer = io.BytesIO()

    with pd.ExcelWriter(buffer, engine='xlsxwriter') as writer:
    # Write each dataframe to a different worksheet.
        ts.to_excel(writer, sheet_name='Timesheet', index = False)
        bl.to_excel(writer, sheet_name='Billing', index = False)
        ot.to_excel(writer, sheet_name='Over Threshold', index = False)
        an.to_excel(writer, sheet_name='Analysis',index = False)
        bo.to_excel(writer, sheet_name='Bonus',index = False)

    # Close the Pandas Excel writer and output the Excel file to the buffer
    writer.close()

    st.download_button(
        label="Download",
        data=buffer,
        file_name="Timesheet & Billing.xlsx",
        mime="application/vnd.ms-excel"
    )
