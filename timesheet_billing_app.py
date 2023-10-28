import pandas as pd
import time
import streamlit as st

#Date & Time
from datetime import datetime, timedelta

#for making API calls
import http.client
import json

def extract_week(file, sheet_name):
  week = pd.read_excel(file, sheet_name = sheet_name)
  week = week.iloc[:, :24]
  cols = [0,1,2]
  for col in range(3, 24, 3):
      week[week.columns[col]] = week[week.columns[col+2]]
      cols.append(col)
  week = week.iloc[:,cols]
  week = week.dropna(subset=week.columns[:3], how='all')
  week = week.melt(id_vars=['Employee ID', 'Store', 'Preferred Name'], value_vars=week.columns[3:], var_name='Date', value_name='Hours')
  week = week[week['Hours'] != 0]
  return week


def get_access_token():
    conn = http.client.HTTPSConnection("pos.aupos.com.au")
    payload = json.dumps({
    "username": "gc-admin",
    "password": "ofbiz"
    })
    headers = {
    'userTenantId': 'gc',
    'User-Agent': 'Apifox/1.0.0 (https://apifox.com)',
    'Content-Type': 'application/json'
    }
    conn.request("POST", "/api/auth/token", payload, headers)
    res = conn.getresponse()
    data = res.read()

    json_data = json.loads(data.decode("utf-8"))

    status = json_data["status"]
    access_token = json_data["data"]["access_token"]
    return status, access_token


def get_batch_sales_df(start, end, shop_id_list):

    status, access_token = get_access_token()

    conn = http.client.HTTPSConnection("pos.aupos.com.au")
    payload = {
    "inputFields": {
        "dateDateValue_fld0_op": "greaterThanEqualTo",
        "dateDateValue_fld0_grp": "g1",
        "dateDateValue_fld0_value": start,
        "dateDateValue_fld1_op": "lessThan",
        "dateDateValue_fld1_grp": "g1",
        "dateDateValue_fld1_value": end,
        "storeProductStoreId_fld0_op": "in",
        "storeProductStoreId_fld0_grp": "g1",
        "storeProductStoreId_fld0_value": shop_id_list
    },
    "orderBy": "",
    "page": 1,
    "size": 1000
    }

    payload_json = json.dumps(payload)

    headers = {
    'Content-Type': 'application/json',
    'userTenantId': 'gc',
    'Authorization': f'Bearer {access_token}',
    'Cookie': 'JSESSIONID=0A7608CA4C2FB965C0EFE3CEB7E149F8.jvm1; OFBiz.Visitor=826825'
    }
    conn.request("POST", "/api/services/sales-summary", payload_json, headers)
    res = conn.getresponse()
    data = res.read()

    json_data = json.loads(data.decode("utf-8"))
    status = json_data["status"]
    sales = json_data["data"]["content"]
    sales_df = pd.DataFrame(sales)

    return status, payload_json, data, sales_df


def calc_timesheets_n_billings(files):
  timesheets = pd.DataFrame()
  billings = pd.DataFrame()
  weeks = pd.DataFrame()

  for file in files:
    time.sleep(1)
    timesheet = pd.read_excel(file, sheet_name = 'Timesheet')
    billing = pd.read_excel(file, sheet_name = 'Billing')
    week1 = extract_week(file, 'Week 1 Roster')
    week2 = extract_week(file, 'Week 2 Roster')
    # # append will be deprecated # #
    # timesheets = timesheets.append(timesheet,ignore_index = True)
    # billings = billings.append(billing,ignore_index = True)
    timesheets = pd.concat([timesheets, timesheet], ignore_index=True)
    billings = pd.concat([billings, billing], ignore_index=True)
    weeks = pd.concat([weeks, week1], ignore_index=True)
    weeks = pd.concat([weeks, week2], ignore_index=True)
    employees = pd.read_excel(file, sheet_name = 'Employees')
    # employees['Employee ID'] = employees['Employee ID'].astype(int)
    # stores = pd.read_excel(file, sheet_name = 'Stores')

  #Remove irrelevant rows
  timesheets.dropna(subset = ['Employee ID'], inplace=True)
  #Keep the needed columns
  timesheets_cols = [1,2,3,7,8,9,10,11,12,13,14,15,16,17,18,19,20]
  timesheets = timesheets[timesheets.columns[timesheets_cols]]
  timesheets['Update Wage'] = timesheets['Update Wage'].astype(bool)
  #Column Aggregations
  timesheets_agg_cols = {'First Name':'first','Last Name':'first','Update Wage':'first','Hour Threshold':'first','Company':'first','Ord':'sum','Sat':'sum','Sun':'sum','Pub':'sum','Eve 1':'sum','Eve 2':'sum','No. of Shifts':'sum','Personal Leave':'sum','Annual Leave':'sum','Unpaid Leave':'sum','Total':'sum'}
  timesheets = timesheets.groupby('Employee ID', as_index = False).agg(timesheets_agg_cols)
  #Calculate Over Threshold
  timesheets['Over Threshold'] = timesheets['Total'] - timesheets['Hour Threshold']
  timesheets.loc[timesheets["Over Threshold"] <=0, "Over Threshold"] = 0
  #Keep Over Thresholds to a new df
  over_threshold = timesheets[timesheets['Over Threshold']>0]
  #Reduce Ord & Total with the excess
  timesheets['Ord'] = timesheets['Ord'] - timesheets['Over Threshold']
  timesheets['Total'] = timesheets['Total'] - timesheets['Over Threshold']
  #Convert 80 & 100 hours to 76 hours
  hours_col = ['Ord', 'Sat','Sun','Eve 1','Eve 2','Pub','Personal Leave', 'Annual Leave', 'Unpaid Leave', 'Total']
  if(100 in timesheets["Hour Threshold"].values):
    timesheets.loc[timesheets["Hour Threshold"] == 100, hours_col] = timesheets[hours_col]/100*76
  if(80 in timesheets["Hour Threshold"].values):
    timesheets.loc[timesheets["Hour Threshold"] == 80, hours_col] = timesheets[hours_col]/80*76
  if any(timesheets["Hour Threshold"] > 1000):
    # Find rows where "Hour Threshold" is greater than 1000
    rows_to_update = timesheets.loc[timesheets["Hour Threshold"] > 1000]
    # Perform actions on the rows
    for index, row in rows_to_update.iterrows():
      threshold = int(row["Hour Threshold"])
      base = int(str(threshold)[:2])
      conversion = int(str(threshold)[-2:])
      # Update multiple columns using .loc
      timesheets.loc[index, hours_col] = timesheets.loc[index, hours_col] / conversion * base

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

  analysis = weeks.copy()
  analysis = analysis.dropna(subset=['Employee ID'])
  analysis['Employee ID'] = analysis['Employee ID'].str[:6]
  analysis['Employee ID'] = analysis['Employee ID'].astype(int)
  analysis = pd.merge(analysis, employees[['Employee ID', 'First Name', 'Last Name', 'Company']], how='left', on=['Employee ID'])
  analysis_col = [
    'Employee ID',
    'First Name',
    'Last Name',
    'Preferred Name',
    'Company',
    'Store',
    'Date',
    'Hours'
    ]
  analysis = analysis[analysis_col]
  analysis['Date'] = pd.to_datetime(analysis['Date'])

  bonus = analysis.copy()

  # Stitch Store ID and drop rows which Store ID are not found
  sheet_id = '1rqOeBjA9drmTnjlENvr57RqL5-oxSqe_KGdbdL2MKhM'
  sheet_name = 'StoreReference'
  url = f'https://docs.google.com/spreadsheets/d/{sheet_id}/gviz/tq?tqx=out:csv&sheet={sheet_name}'
  store_ref = pd.read_csv(url)
  if bonus['Store'].dtypes == float:
    bonus['Store'] = bonus['Store'].astype(int).astype(str)
  bonus = pd.merge(bonus, store_ref, on=['Store'], how = 'left')
  bonus.dropna(subset=['Store ID'], inplace = True)

  # Stitch shop_id
  sheet_id = '1ezyBlKquUhYnFwmIKTR4fghI59ZvGaKL35mKbcdeRy4'
  sheet_name = 'Stores'
  url = f'https://docs.google.com/spreadsheets/d/{sheet_id}/gviz/tq?tqx=out:csv&sheet={sheet_name}'
  store_df_gs = pd.read_csv(url)

  bonus = pd.merge(bonus, store_df_gs[['Store ID', 'shop_id']], on=['Store ID'], how = 'left')
  bonus['shop_id'] = bonus['shop_id'].astype(int)

  # Stich sales based on shop_id & dates
  start = bonus['Date'].min()
  end = bonus['Date'].max()

  date_range = pd.date_range(start=start, end=end, freq='D')
  shop_id_list = bonus['shop_id'].unique().tolist()
  shop_id_list_str = [str(x) for x in shop_id_list]
  sales_df = pd.DataFrame()

  for d in date_range:
    tmr = d + timedelta(days=1)
    start_str = d.strftime("%Y-%m-%d")
    end_str = tmr.strftime("%Y-%m-%d")
    _, _, _, df = get_batch_sales_df(start_str, end_str, shop_id_list_str)
    df['Date'] = d
    sales_df = pd.concat([sales_df, df], ignore_index=True)

  sales_df.rename(columns={'storeProductStoreId': 'shop_id', 'grandTotal':'sales'}, inplace=True)
  sales_df['shop_id'] = sales_df['shop_id'].astype(int)

  bonus = pd.merge(bonus, sales_df[['shop_id', 'Date', 'sales']], on=['shop_id', 'Date'], how = 'left')

  # Stitch Target Sales & Bonus Rates
  sheet_id = '1rqOeBjA9drmTnjlENvr57RqL5-oxSqe_KGdbdL2MKhM'
  sheet_name = 'Targets'
  url = f'https://docs.google.com/spreadsheets/d/{sheet_id}/gviz/tq?tqx=out:csv&sheet={sheet_name}'
  targets = pd.read_csv(url)
  targets['Date'] = pd.to_datetime(targets['Date'])

  # Change Bonus Rates to 0, if Target Sales is not met
  bonus = pd.merge(bonus, targets[['Store ID', 'Date', 'Target Sales', 'Bonus Rate']], on=['Store ID', 'Date'], how = 'left')
  bonus['Bonus Rate'] = bonus['Bonus Rate'].where(bonus['sales'] >= bonus['Target Sales'], 0)
  bonus['Bonus'] = bonus['Bonus Rate']  * bonus['Hours']


  return timesheets, billings, over_threshold, analysis, bonus

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
