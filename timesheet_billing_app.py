import pandas as pd
from database_utils import *

# # # START OF FUNCTIONS
def read_csv_from_config(gs_config):
    url = f"https://docs.google.com/spreadsheets/d/{gs_config['sheet_id']}/gviz/tq?tqx=out:csv&sheet={gs_config['sheet_name']}"
    # Create the SQLAlchemy engine
    df  = pd.read_csv(url)
    return df

def extract_additional_hr(file, sheet_name):
  df = pd.read_excel(file, sheet_name = sheet_name)
  df = df.dropna(subset=['Employee ID'])

  if df['Employee ID'].dtypes == 'object':
    df['Employee ID'] = df['Employee ID'].str[:6]
  df['Employee ID'] = df['Employee ID'].astype(int)

  if df['Store'].dtypes == float:
    df['Store'] = df['Store'].astype(int).astype(str)

  df = df.fillna({'Add': 0, 'Add.1': 0,'Add.2': 0,'Add.3': 0,'Add.4': 0,'Add.5': 0,'Personal Leave': 0,'Annual Leave': 0,})
  df['2012-11-01'] = df['Add'] + df['Add.1'] + df['Add.2'] + df['Add.3'] + df['Add.4'] + df['Add.5']
  df['2013-01-01'] = df['Personal Leave'] + df['Annual Leave']
  cols =[0,1,2, df.columns.tolist().index('2012-11-01'),df.columns.tolist().index('2013-01-01')]

  df = df.iloc[:, cols]
  df = df.dropna(subset=df.columns[:3], how='all')
  df = df.melt(id_vars=['Employee ID', 'Store', 'Preferred Name'], value_vars=df.columns[3:], var_name='Date', value_name='Hours')
  df = df[df['Hours'] != 0]
  return df

def extract_rostered_hr(file, sheet_name):
  df = pd.read_excel(file, sheet_name = sheet_name)

  df = df.dropna(subset=['Employee ID'])

  if df['Employee ID'].dtypes == 'object':
    df['Employee ID'] = df['Employee ID'].str[:6]
  df['Employee ID'] = df['Employee ID'].astype(int)

  if df['Store'].dtypes == float:
    df['Store'] = df['Store'].astype(int).astype(str)

  df = df.iloc[:, :24]
  cols = [0,1,2]
  for col in range(3, 24, 3):
      df[df.columns[col]] = df[df.columns[col+2]]
      cols.append(col)
  df = df.iloc[:,cols]
  df = df.dropna(subset=df.columns[:3], how='all')
  df = df.melt(id_vars=['Employee ID', 'Store', 'Preferred Name'], value_vars=df.columns[3:], var_name='Date', value_name='Hours')
  df = df[df['Hours'] != 0]
  return df

# Function to count digits before the decimal point in a float number
def count_digits_before_decimal(number):
  # Convert to string, split at the decimal point, and count digits in the integer part
  integer_part = str(number).split('.')[0]
  return len(integer_part.replace('-', '').replace('nan', ''))

def calc_timesheets_n_billings(files):
  print('calc')
  timesheets = pd.DataFrame()
  billings = pd.DataFrame()
  rostered_hr = pd.DataFrame()
  additional_hr = pd.DataFrame()

  for file in files:
    timesheet = pd.read_excel(file, sheet_name = 'Timesheet')
    billing = pd.read_excel(file, sheet_name = 'Billing')
    rostered_hr_w1 = extract_rostered_hr(file, 'Week 1 Roster')
    rostered_hr_w2 = extract_rostered_hr(file, 'Week 2 Roster')
    additional_hr_w1 = extract_additional_hr(file, 'Week 1 Roster')
    additional_hr_w2 = extract_additional_hr(file, 'Week 2 Roster')
    employees = pd.read_excel(file, sheet_name = 'Employees')

    timesheets = pd.concat([timesheets, timesheet], ignore_index=True)
    billings = pd.concat([billings, billing], ignore_index=True)
    rostered_hr = pd.concat([rostered_hr, rostered_hr_w1], ignore_index=True)
    rostered_hr = pd.concat([rostered_hr, rostered_hr_w2], ignore_index=True)
    additional_hr = pd.concat([additional_hr, additional_hr_w1], ignore_index=True)
    additional_hr = pd.concat([additional_hr, additional_hr_w2], ignore_index=True)

  #Remove irrelevant rows
  timesheets = timesheets.dropna(subset = ['Employee ID'])

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

  rostered_hr = pd.merge(rostered_hr, employees[['Employee ID', 'First Name', 'Last Name', 'Company']], how='left', on=['Employee ID'])
  rostered_hr_col = [
  'Employee ID',
  'First Name',
  'Last Name',
  'Preferred Name',
  'Company',
  'Store',
  'Date',
  'Hours'
  ]
  rostered_hr = rostered_hr[rostered_hr_col]
  rostered_hr['Date'] = pd.to_datetime(rostered_hr['Date']).dt.date

  bonus = rostered_hr.copy()

  # Stitch Store ID and drop rows which Store ID are not found
  sheet_id = '1rqOeBjA9drmTnjlENvr57RqL5-oxSqe_KGdbdL2MKhM'
  sheet_name = 'StoreReference'
  url = f'https://docs.google.com/spreadsheets/d/{sheet_id}/gviz/tq?tqx=out:csv&sheet={sheet_name}'
  store_ref = pd.read_csv(url)

  bonus = pd.merge(bonus, store_ref, on=['Store'], how = 'left')

  bonus.dropna(subset=['Store ID'], inplace = True)

  store_crm_config = {
    'sheet_id': '1aVcnah9Cp_PUvFiXgd2XRmBpLhumCqHUaqROaLcpYfc',
    'sheet_name': 'store'
  }
  store_crm = read_csv_from_config(store_crm_config)
  store_crm['opened_on'] = pd.to_datetime(store_crm['opened_on'])
  store_crm['closed_on'] = pd.to_datetime(store_crm['closed_on'])
  store_crm_col = ['store_id', 'recid_plo', 'menu_type']
  store_crm = store_crm[store_crm_col]
  store_crm = store_crm[~store_crm['recid_plo'].isna()]
  store_crm = store_crm.rename(columns={'store_id': 'Store ID'})
  
  bonus = pd.merge(bonus, store_crm[['Store ID', 'recid_plo']], on=['Store ID'], how = 'left')
  bonus['recid_plo'] = bonus['recid_plo'].astype(int)

  # Stich sales based on recid_plo & dates, skip if there is no Date
  start = bonus['Date'].min()
  end = bonus['Date'].max()

  start_str = start.strftime('%Y-%m-%d')
  end_str = end.strftime('%Y-%m-%d')

  recid_plo_list = bonus['recid_plo'].unique().tolist()
  recid_plo_list_str = ', '.join(str(id) for id in recid_plo_list)

  sheet_id = '1peA8effpeSTk3duIjxF46V-PrDD8tv3fubTCDEpD940'
  sheet_name = 'ops_bonus_exclusion'
  url = f'https://docs.google.com/spreadsheets/d/{sheet_id}/gviz/tq?tqx=out:csv&sheet={sheet_name}'
  exclusion_df = pd.read_csv(url)
  excluded_recid_plu = exclusion_df['recid_plu'].drop_duplicates()

  excluded_recid_plu_str = ', '.join(str(s) for s in excluded_recid_plu)

  # query = '''
  # SELECT ts2.recid_plo, ts.itemdate as Date, sum(ts.qty*ts.price) as Sales
  # FROM tbl_salesitems ts 
  # JOIN tbl_salesheaders ts2 on ts.recid_mixh = ts2.recid
  # WHERE ts.itemdate >= '{start}' and ts.itemdate <= '{end}' and ts2.recid_plo in ({recid_plo_list}) and ts.recid_plu not in ({excluded_recid_plu})
  # GROUP BY ts2.recid_plo, ts.itemdate
  # ORDER BY ts.itemdate ASC, recid_plo ASC
  # '''.format(start=start_str, end = end_str, recid_plo_list = recid_plo_list_str, excluded_recid_plu = excluded_recid_plu_str)
  # sales_df = pd.read_sql(query, mysql_engine)

  query = '''
  SELECT recid_plo, itemdate as Date, sum(net_amount + gst_amount) as Sales
  FROM d_txnlines
  WHERE itemdate >='{start}' and itemdate <= '{end}' and recid_plo in ({recid_plo_list}) and recid_plu not in ({excluded_recid_plu})
  GROUP BY recid_plo, itemdate
  ORDER BY itemdate ASC, recid_plo ASC
  '''.format(start=start_str, end = end_str, recid_plo_list = recid_plo_list_str, excluded_recid_plu = excluded_recid_plu_str)
  print(query)
  clickhouse_client = gong_cha_redcat_db_clickhouse_client
  sales_df = get_DataFrame_from_clickhouse(query, clickhouse_client)

  sales_df['Date'] = pd.to_datetime(sales_df['Date']).dt.date

  bonus = pd.merge(bonus, sales_df[['recid_plo', 'Date', 'Sales']], on=['recid_plo', 'Date'], how = 'left')

  # Stitch Target Sales & Bonus Rates
  sheet_id = '1rqOeBjA9drmTnjlENvr57RqL5-oxSqe_KGdbdL2MKhM'
  sheet_name = 'Targets'
  url = f'https://docs.google.com/spreadsheets/d/{sheet_id}/gviz/tq?tqx=out:csv&sheet={sheet_name}'
  targets = pd.read_csv(url)
  targets['Date'] = pd.to_datetime(targets['Date']).dt.date

  # Change Bonus Rates to 0, if Target Sales is not met
  bonus = pd.merge(bonus, targets[['Store ID', 'Date', 'Target Sales', 'Bonus Rate']], on=['Store ID', 'Date'], how = 'left')
  bonus['Bonus Rate'] = bonus['Bonus Rate'].where(bonus['Sales'] >= bonus['Target Sales'], 0)
  bonus['Bonus'] = bonus['Bonus Rate']  * bonus['Hours']

  # Work out additional_hr
  additional_hr = additional_hr.dropna(subset=['Employee ID'])
  additional_hr = pd.merge(additional_hr, employees[['Employee ID', 'First Name', 'Last Name', 'Company']], how='left', on=['Employee ID'])
  additional_hr_col = [
  'Employee ID',
  'First Name',
  'Last Name',
  'Preferred Name',
  'Company',
  'Store',
  'Date',
  'Hours'
  ]
  additional_hr = additional_hr[additional_hr_col]
  additional_hr['Date'] = pd.to_datetime(additional_hr['Date'])

  # Concat rostered_hr and additional_hr
  analysis = pd.concat([rostered_hr, additional_hr])

  # Concat Bouns on to Timesheets
  bonus_summary = bonus.groupby('Employee ID', as_index = False).agg({'Bonus':'sum'})
  timesheets = pd.merge(timesheets, bonus_summary, on=['Employee ID'], how = 'left')
  timesheets.fillna({'Bonus':0}, inplace = True)

  upsheets = pd.melt(
    timesheets, 
    id_vars=['Employee ID', 'First Name', 'Last Name', 'Company'],  # Columns to keep
    value_vars=['Ord', 'Sat', 'Sun', 'Pub', 'Eve 1', 'Eve 2', 'No. of Shifts', 'Personal Leave', 'Annual Leave', 'Unpaid Leave', 'Bonus'],  # Columns to unpivot
    var_name='type',
    value_name='hours'
  )

  type_replacement = {
    'Ord': 'Ordinary Hours',
    'Sat': 'Saturday',
    'Sun': 'Sunday',
    'Pub': 'Public Holiday Hours',
    'Eve 1': 'Late Evening Hours 10pm to Midnight',
    'Eve 2': 'Late Evening Hours Midnight - 6AM',
    'No. of Shifts': 'Laundry Allowance',
    'Personal Leave': "Personal/Carer's Leave",
    'Annual Leave': 'Annual Leave',
    'Unpaid Leave': 'Other Unpaid Leave',
    'Bonus': 'Bonus',
  }
  upsheets['type'] = upsheets['type'].replace(type_replacement)
  
  upsheets = upsheets[upsheets['hours'] != 0]
  upsheets['date']=bonus['Date'].min()
  upsheets = upsheets.rename(columns={'First Name': 'first_name', 'Last Name':'last_name'})


  upsheets['rate'] = ''
  upsheets['calculation_type'] = ''

  fixed_hours_calc_types = ["Personal/Carer's Leave", 'Annual Leave', 'Other Unpaid Leave']
  upsheets.loc[upsheets['type'].isin(fixed_hours_calc_types), 'calculation_type'] = 'FIXEDHOURS'

  fixed_amount_calc_types = ['Bonus']
  upsheets.loc[upsheets['type'].isin(fixed_amount_calc_types), 'calculation_type'] = 'FIXEDAMOUNT'
  upsheets.loc[upsheets['type'].isin(fixed_amount_calc_types), 'rate'] = upsheets['hours']
  upsheets.loc[upsheets['type'].isin(fixed_amount_calc_types), 'hours'] = ''

  upsheets['digit'] = upsheets['Employee ID'].apply(count_digits_before_decimal)
# Define a mapping of existing types to new types
  casual_mapping = {
      'Ordinary Hours': 'Casual Ordinary Hours',
      'Saturday': 'Casual Saturday',
      'Sunday': 'Casual Sunday',
      'Public Holiday Hours': 'Casual Public Holiday Hours',
      'Late Evening Hours 10pm to Midnight' : 'Casual Late Evening Hours 10pm to Midnight',
      'Late Evening Hours Midnight - 6AM' : 'Casual Late Evening Hours Midnight - 6AM',
  }
# Use map with fillna to keep original values if no match in casual_mapping
  upsheets.loc[upsheets['digit'] == 6, 'type'] = (
      upsheets.loc[upsheets['digit'] == 6, 'type']
      .map(casual_mapping)
      .fillna(upsheets.loc[upsheets['digit'] == 6, 'type'])
  )
  GCM = upsheets[upsheets['Company']=='GCM']
  HL = upsheets[upsheets['Company']=='HL']
  SS = upsheets[upsheets['Company']=='SS']
  upsheets_cols = ['Company','first_name', 'last_name', 'type', 'date','hours', 'rate', 'calculation_type']
  upsheets = upsheets[upsheets_cols]

  company_cols = ['first_name', 'last_name', 'type', 'date','hours', 'rate', 'calculation_type']
  GCM = GCM[company_cols]
  HL = HL[company_cols]
  SS = SS[company_cols]

  return timesheets, billings, over_threshold, analysis, bonus, upsheets, GCM, HL, SS


# # # END OF FUNCTIONS

import io
import streamlit as st

st.title('Timesheet & Billing')

uploaded_files = st.file_uploader("Choose Files", accept_multiple_files = True)

# Create an empty container
output = st.empty()

# if uploaded_files is not None:
if len(uploaded_files) > 0:
    ts, bl, ot, an, bo, up, gcm, hl, ss = calc_timesheets_n_billings(uploaded_files)

    buffer = io.BytesIO()

    with pd.ExcelWriter(buffer, engine='xlsxwriter') as writer:
    # Write each dataframe to a different worksheet.
        ts.to_excel(writer, sheet_name='Timesheet', index = False)
        bl.to_excel(writer, sheet_name='Billing', index = False)
        ot.to_excel(writer, sheet_name='Over Threshold', index = False)
        an.to_excel(writer, sheet_name='Analysis',index = False)
        bo.to_excel(writer, sheet_name='Bonus',index = False)
        up.to_excel(writer, sheet_name='Upsheets',index = False)
        gcm.to_excel(writer, sheet_name='Upsheets GCM',index = False)
        hl.to_excel(writer, sheet_name='Upsheets HL',index = False)
        ss.to_excel(writer, sheet_name='Upsheets SS',index = False)

    # Close the Pandas Excel writer and output the Excel file to the buffer
    writer.close()

    st.download_button(
        label="Download",
        data=buffer,
        file_name="Timesheet & Billing.xlsx",
        mime="application/vnd.ms-excel"
    )