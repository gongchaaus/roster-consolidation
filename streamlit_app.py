from collections import namedtuple
import altair as alt
import math
import pandas as pd
import streamlit as st

import pandas as pd
import datetime
import numpy as np
import time

from dateutil.relativedelta import relativedelta
from datetime import timedelta, date

import math
import re

import io

from tqdm import tqdm

def find_columns(df):
  for i in df.index:
    cols = []
    count = 0
    broken = False
    for j in range(len(df.iloc[i])):
      if(isinstance(df.iloc[i][j],str)):
        cols.append(df.iloc[i][j])
        count += 1
      if count == len(df.iloc[i]):
        broken = True
        break
    if broken:
      return i, cols

def dr_or_cr(row):
  if (row[-3] - row[-2] == row[-1]) & (row[-2] - row[-3] == row[-1]):
    return 'Unknown'
  elif row[-3] - row[-2] == row[-1]:
    return 'DR'
  elif row[-2] - row[-3] == row[-1]:
    return 'CR'
  else:
    return 'Unknown'

def replace_to_number(s):
    if type(s).__name__ == "str":
        s = s.strip()
        if s == "-":
            s = 0
        else:
            s = s.replace(",","").replace("$","")
            if s.find("(") >= 0 and s.find(")") >= 0:
                s = s.replace("(","-").replace(")","")
    return s

def output(df, account_names):
    buffer = io.BytesIO()
    writer = pd.ExcelWriter(buffer, engine='xlsxwriter')
    char_to_replace = {'[': ' ', ']': ' ', ':' : ' ', '*' : ' ', '?' : ' ', '/' : ' ', '\\':' '}
    account_names[:] = [x for x in account_names if x]
    account_names = list(dict.fromkeys(account_names))

    for an in account_names:
        df_an = df.loc[df['Account'] == an]
        sheet_name = an[0:31]

    # Iterate over all key-value pairs in dictionary
        for key, value in char_to_replace.items():
        # Replace key character with value character in string
            sheet_name = sheet_name.replace(key, value)

    # print(sheet_name)
        df_an.to_excel(writer, sheet_name = sheet_name)

    writer.save()
    processed_data = buffer.getvalue()
    return processed_data


st.title('Tax Ides & MGMTCNSLTNG')

df=pd.DataFrame(columns=[])

uploaded_file = st.file_uploader("Choose a file")
if uploaded_file is not None:
#read csv
    df=pd.read_csv(uploaded_file)
    index, columns = find_columns(df)
    df.columns = columns
    df = df[index+1:].reset_index(drop=True)

    labels = []
    statement_names = []
    account_names = []
    account_name = ''
    statement_name = 'PL'
    df_tmp = df['Date'].copy(deep=True)
    df_tmp = pd.to_datetime(df_tmp, errors='coerce')
    df_tmp.dropna(inplace=True)
    max_date = max(df_tmp)
    min_date = min(df_tmp)

    for i in tqdm(df.index):
    # for i in tqdm(range(50)):
      types=[]
      isOpeningBalance=False
      isClosingBalance=False
      startwithTotal=False

      for j in range(len(df.iloc[i])):
        types.append(type(df.iloc[i][j]))
        if df.iloc[i][j] == 'Opening Balance':
          isOpeningBalance = True
          # print('Opening')
        elif df.iloc[i][j] == 'Closing Balance':
          isClosingBalance = True
          # print('Closing')
        if type(df.iloc[i][j]) == type('str'):
          if  df.iloc[i][j].startswith('Total'):
            startwithTotal=True
            # print('Total found')
      # print(df.iloc[i])

      if type(pd.to_datetime(df.iloc[i][0], errors = 'ignore', dayfirst=True)) == type(pd.Timestamp.now()):
        labels.append('Transaction')
      elif all(element == type(0.00) for element in types):
        labels.append('Blank')
      elif types[0] == type('str') and all(element == type(0.00) for element in types[1:]):
        labels.append('Account Name')
        account_name = df.iloc[i][0]
      elif types[0] == type('str') and types[-1] == type('str') and startwithTotal:
        labels.append('Total')
      elif types[0] == type('str') and types[-1] == type('str') and isOpeningBalance:
        labels.append('Opening')
        df.iloc[i]['Description']='Opening Balance'
        df.iloc[i]['Date'] = min_date
        statement_name = 'BS'
      elif types[0] == type('str') and types[-1] == type('str') and isClosingBalance:
        labels.append('Closing')
        df.iloc[i]['Date'] = max_date
        df.iloc[i]['Description']='Closing Balance'
        statement_name = 'PL'
      else:
        labels.append('Error')
        print(df.iloc[i])
        print(types)
      statement_names.append(statement_name)
      account_names.append(account_name)

    df.iloc[:,-1] = df.iloc[:,-1].apply(lambda x: replace_to_number(x))
    df.iloc[:,-2] = df.iloc[:,-2].apply(lambda x: replace_to_number(x))
    df.iloc[:,-3] = df.iloc[:,-3].apply(lambda x: replace_to_number(x))
    df.iloc[:,-3] = pd.to_numeric(df.iloc[:,-3], errors = 'coerce')
    df.iloc[:,-2] = pd.to_numeric(df.iloc[:,-2], errors = 'coerce')
    df.iloc[:,-1] = pd.to_numeric(df.iloc[:,-1], errors = 'coerce')

    df['DRCR'] = df.apply (lambda row: dr_or_cr(row), axis=1)

    df['Label'] = labels
    df['Account'] = account_names
    df['Statement'] = statement_names

    # df.drop(df[(df['Label'] == 'Blank') | (df['Label']=='Account Name') | (df['Label']=='Total') | (df['Label']=='Closing')].index, inplace=True)
    df.drop(df[(df['Label'] == 'Blank') | (df['Label']=='Account Name')].index, inplace=True)
    df['Date'] = pd.to_datetime(df['Date'], errors='coerce')
    pl = df.loc[df['Statement'] =='PL']
    bs = df.loc[df['Statement']=='BS']


    df_xlsx = output(df, account_names)

    st.download_button(
         label="Download the fancy Excel",
         data=df_xlsx,
         file_name='outputs.xlsx'
     )
