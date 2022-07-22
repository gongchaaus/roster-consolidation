import pandas as pd
import streamlit as st


def calc_timesheets_n_billings(files):
    timesheets = pd.DataFrame()
    billings = pd.DataFrame()

    for file in files:
        timesheet = pd.read_excel(file, sheet_name = 'Timesheet')
        billing = pd.read_excel(file, sheet_name = 'Billing')
        timesheets = timesheets.append(timesheet,ignore_index = True)
        billings = billings.append(billing,ignore_index = True)

    #Remove irrelevant rows
    timesheets.dropna(subset = ['Employee ID'], inplace=True)
    #Keep the needed columns
    timesheets_cols = [1,2,6,7,8,9,10,11,12,13,14,15,16,17,18,19]
    timesheets = timesheets[timesheets.columns[timesheets_cols]]
    timesheets['Update Wage'] = timesheets['Update Wage'].astype(bool)
    #Column Aggregations
    timesheets_agg_cols = {'Preferred Name':'first','Update Wage':'first','Hour Threshold':'first','Company':'first','Ord':'sum','Sat':'sum','Sun':'sum','Pub':'sum','Eve 1':'sum','Eve 2':'sum','No. of Shifts':'sum','Personal Leave':'sum','Annual Leave':'sum','Unpaid Leave':'sum','Total':'sum'}
    timesheets = timesheets.groupby('Employee ID', as_index = False).agg(timesheets_agg_cols)
    #Calculate Over Threshold
    timesheets['Over Threshold'] = timesheets['Total'] - timesheets['Hour Threshold']
    timesheets.loc[timesheets["Over Threshold"] <=0, "Over Threshold"] = 0
    #Keep Over Thresholds to a new df
    over_threshold = timesheets[timesheets['Over Threshold']>0]
    #Reduce Ord & Total with the excess
    timesheets['Ord'] = timesheets['Ord'] - timesheets['Over Threshold']
    timesheets['Total'] = timesheets['Total'] - timesheets['Over Threshold']
    #Convert 100 hours to 76 hours
    hours_col = ['Ord', 'Sat','Sun','Eve 1','Eve 2', 'Personal Leave', 'Annual Leave', 'Unpaid Leave', 'Total']
    timesheets.loc[timesheets["Hour Threshold"] == 100, hours_col] = timesheets[hours_col]/100*76
    #drop Hour Threshold & Over Threshold
    timesheets = timesheets.drop(['Hour Threshold','Over Threshold'],axis = 1)

    #Remove irrelevant rows
    billings.dropna(subset=['Store'],inplace = True)
    billings = billings[billings['Total'] > 0]

    #Keep the needed columns
    billings_cols = [0,1,2,3,4,5,6,7,8,9,10,11]
    billings = billings[billings.columns[billings_cols]]
    #Column Aggregations
    billings_agg_cols = {'Ord':'sum','Sat':'sum','Sun':'sum','Pub':'sum','Eve 1':'sum','Eve 2':'sum','No. of Shifts':'sum','Personal Leave':'sum','Annual Leave':'sum','Unpaid Leave':'sum','Total':'sum'}
    billings = billings.groupby('Store', as_index = False).agg(billings_agg_cols)

    return timesheets, billings, over_threshold



import io

st.title('Timesheet & Billing')

uploaded_files = st.file_uploader("Choose Files", accept_multiple_files = True)
if uploaded_files is not None:
    ts, bl, ot = calc_timesheets_n_billings(uploaded_files)

    buffer = io.BytesIO()

    with pd.ExcelWriter(buffer, engine='xlsxwriter') as writer:
    # Write each dataframe to a different worksheet.
        ts.to_excel(writer, sheet_name='Timesheet')
        bl.to_excel(writer, sheet_name='Billing')
        ot.to_excel(writer, sheet_name='Over Threshold')

    # Close the Pandas Excel writer and output the Excel file to the buffer
    writer.save()

    st.download_button(
        label="Download",
        data=buffer,
        file_name="Timesheet & Billing.xlsx",
        mime="application/vnd.ms-excel"
    )
