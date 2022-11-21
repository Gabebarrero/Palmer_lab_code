#!/usr/bin/env python
# coding: utf-8

# In[2]:
import pandas as pd
import numpy as np
import os
from datetime import datetime
import datetime
import psycopg2
from sqlalchemy import create_engine
import sqlalchemy
import matplotlib.pyplot as plt
import seaborn as sns
# ## Read in the file 

# In[62]:


data = pd.read_excel('Italy #17 shipping sheet.xlsx')
# data
# def reformat(data, columns):
#     for col in columns:
#         data[col] = data[col].astype(str)
#         data[col] = [val.replace('.0','') for val in data[col]]
# reformat(data, ["15 digit RFID"])
# data
data




# ## Data check

# In[ ]:


data


# In[23]:


# fix for casting error 
data.rename(columns={'RFID':'rfid'}, inplace=True)

for value in data['rfid']:
    print(len(str(value)))
    if len(str(value)) < 15:
        print('val',value)
        row_idx= data[data['rfid'] == value].index.tolist()
        result=data.iloc[row_idx]
        for i in row_idx:
            data.loc[i, 'rfid']= f'93300{value}' ## change this 
    else:
        value = 15
        pass

for i in data['rfid']:
    if int(i) < 9:
        print(i)
        
data


### Read in Tissue table

# In[25]:


projects = ['p50_david_dietz_2020','p50_hao_chen_2020','p50_hao_chen_2020_rnaseq','p50_paul_meyer_2020',
            'u01_olivier_george_cocaine', 'u01_olivier_george_oxycodone', 'u01_olivier_george_scrub',
            'u01_peter_kalivas_italy', 'u01_peter_kalivas_us', 'u01_suzanne_mitchell', 'u01_tom_jhou' ]
def runQuery(query):
    connection = psycopg2.connect(database = "PalmerLab_Datasets",
                                  user = "postgres",
                                  password = "palmerlab-amapostgres",
                                  host = "palmerlab-main-database-c2021-08-02.c6sgfwysomht.us-west-2.rds.amazonaws.com",
                                  port = '5432')
    cursor = connection.cursor()
    if "SELECT" in query:
        table = pd.read_sql(query, con = connection)
    elif "UPDATE" in query:
        cursor.execute(query)
        print(query)
        connection.commit()
    elif "DELETE" in query:
        cursor.execute(query)
        print(query)
        connection.commit()
    else:
        return None
    if connection:
        cursor.close()
        connection.close()
        if "SELECT" in query:
            return table
        else:
            return
#change project here
current_project = 'sample_tracking.tissue'
wfu_master = runQuery("SELECT * FROM " + current_project)
# extraction = runQuery("SELECT * FROM sample_tracking.tissue")
# extraction
# wfu_master['project_name'].value_counts()
wfu_master


# # Run the following lines of code 

# In[32]:


def fixColumns(columns):
    for i in range(len(columns)):
        columns[i] = columns[i].lower().replace(" ", "")
        if "sir" in columns[i] or "father" in columns[i]:
            columns[i] = "sires"
        elif "dam" in columns[i] or "mother" in columns[i]:
            columns[i] = "dames"
        elif "animal" in columns[i] and "id" in columns[i]:
            columns[i] = "labanimalid"
        elif "access" in columns[i] and "id" in columns[i]:
            columns[i] = "accessid"
        elif "sex" in columns[i]:
            columns[i] = "sex"
        elif "rfid" in columns[i] or "ponder" in columns[i]:
            columns[i] = "rfid"
        elif ("d" in columns[i] and "o" in columns[i] and "b" in columns[i]) or "birth" in columns[i]:
            columns[i] = "dob"
        elif ("d" in columns[i] and "o" in columns[i] and "w" in columns[i]) or ("wean" in columns[i] and "date" in columns[i]):
            columns[i] = "dow"
        elif "ship" in columns[i] and "date" in columns[i]:
            columns[i] = "shipmentdate"
        elif "litter" in columns[i] and ("#" in columns[i] or "num" in columns[i]):
            columns[i] = "litternumber"
        elif "ship" in columns[i] and "box" in columns[i]:
            columns[i] = "shipmentbox"
        elif "comment" in columns[i] or "note" in columns[i]:
            columns[i] = "comments"
    return columns

for i in data.columns:
    print(type(i))
    fixColumns(i)
# fixColumns(data['15 digit RFID'])





# In[33]:


def checkDupCols(data):
    for col in data.columns:
        if ".1" in col:
            data[col] = data[col].astype(str)
            data[col.replace(".1", "")] = data[col.replace(".1", "")].astype(str)
            data[col] = [val.replace(".0", "") for val in data[col]]
            data[col.replace(".1", "")] = [val.replace(".0", "") for val in data[col].replace(".1", "")]
            if list(data[col] == data[col.replace(".1", "")]).count(True) != len(data[col]):
                print("Transponder ID Mismatch found")
                print(data[col])
                print(data[col.replace(".1", "")])
            del data[col]
    return data

checkDupCols(data)


# In[34]:


#Specific Errors

def date_error(data):
    #Meyer2020 Shipping Sheet #7
    a = (data['dob'] == '4/21/20221')
    data.loc[a,'dob'] = '4/21/2021'
    return data

def comment_error(data):
    #Meyer2020 Shipping Sheet #7
    #data["comments"] = "NA"
    if 'unnamed:18' in data.columns:
        data.loc[data['unnamed:18'] == 'DOUBLE PULLS','unnamed:18'] = np.nan
        data.loc[(data['rfid'] == 'AA1DCD6794'),'comments'] = "DOUBLE PULLS"
        data = data.drop(columns = ['unnamed:18'])
    return data
    
def column_error(data):
    #Meyer2020 Shipping sheet #7
    if '124rats' in data.columns:
        data = data.drop(columns=['124rats'])
    return data

def error_rows(data):
    #Meyer2020 Shipping sheet #7
    #Meyer2020 Shipping sheet #8
    if 'unnamed:5' in data.columns:
        print("Difference between unnamed:5 and rfid")
        display(data.loc[~(data['unnamed:5'] == data['rfid'])])
        print("unnamed:5 value counts")
        display(data['unnamed:5'].value_counts().to_string())
        print("Dropped unnamed:5")
        data = data.drop(columns = ['unnamed:5'])
    return data
        
def error_rfids(data):
    data['rfid'] = data['rfid'].str.upper()


# In[36]:


#change transponder ID into rfid
def rfid_column():
    if "Transponder ID" in data.columns:
        i = list(data.columns).index("Transponder ID")
        for index, row in data.iterrows():
            data.iloc[index, i] = data.iloc[index, i].upper()
            if data.iloc[index, i][:2] != "AA":
                data.iloc[index, i] = "AA" + data.iloc[index, i]
        checkDupCols(data)
        data.columns = fixColumns(list(data.columns))
rfid_column()


# In[37]:


# Check if data columns look correct
#print("Data columns:", list(data.columns))
#remove NA columns
def drop_na(data):
    list1 = data.columns
    #print(list1)
    data = data.dropna(axis=1, how='all')
    list2 = data.columns
    #print(list2)
    diff = list(set(list1).difference(list2))
    if len(diff) >= 1:
        print("\nData columns dropped due to all NA:", diff)
    return data


# In[38]:


def rfid_in_wfu(data):
    data["rfid"] = data["rfid"].astype(str)
    wfu_master_rfids = list(wfu_master["rfid"])
    data_rfids = list(data["rfid"])
    duplicate_rfids = list(set(wfu_master_rfids).intersection(set(data_rfids)))
    print(len(duplicate_rfids), "rfids already in WFU_master, out of", len(data_rfids))
    print(duplicate_rfids)
    fix = list(set(data_rfids) - set(wfu_master_rfids))
    

rfid_in_wfu(data)


# In[44]:


def qc_rfid(rfid, prefix, length):
    # Checking if prefix is present
    if rfid[:len(prefix)] != prefix:
        return False
    # Checking is rfid is correct length
    elif len(rfid) != length:
        return False
    return True

def check_rfid():
#     data = tissue.copy()
    #Check to make sure rfid for the project contains the correct naming convention
    project_metadata = pd.read_csv("project_metadata - project_metadata (1).csv", dtype = str)
    i_convention = list(project_metadata.columns).index("rfid_convention")
    subset = project_metadata[project_metadata["project_name"] == 'u01_olivier_george_oxycodone']
    subset.index = range(subset.shape[0])
    convention_list = subset.iloc[0, i_convention]
    convention_list = convention_list.split(";")
    for i in range(len(convention_list)):
        convention_list[i] = convention_list[i].replace("(", "").replace(")", "").split(",")
    for rfid in data["rfid"]:#data needs to be combination of all projec
        passed = False
        for convention in convention_list:
            prefix = convention[0]
            length = int(convention[1])
            passed = passed + qc_rfid(rfid, prefix, length)
        if not passed:
            print('RFID errors:')
            print(rfid, 'u01_olivier_george_oxycodone')
            display(data.loc[data['rfid']== rfid])
            
check_rfid()      
# data


# ## Check if looks good

# In[82]:



#RFIDs already in wfu
rfid_in_wfu(data)
#correct specific errors involving comments
data = comment_error(data)
#correct specific errors involving columns
data = column_error(data)
#Drop columns with all NA values
data = drop_na(data)
#column information
column_qc()
#rows with notable errors
data = error_rows(data)
# error fixing for rfids
error_rfids(data)
#find inccorrect rfids
check_rfid()
#sibling count
# data = siblings(data)
#plot relevant graphs
graph_columns()


# ## Reformatting DF change XXX

# In[61]:


data = data
project_name='XXX'
shippingbox='XXX'
freezer_location_of_tissue='XXX'
destination='XXX'
storage_box_of_tissue='XXX'
comments= 'XXX'
tissue= 'XXX'
data['tissue_type']=tissue
data['storage_box_of_tissue']=storage_box_of_tissue
data['comments']=comments
data['project_name']=project_name
data['shipping_box']=shippingbox
data['freezer_location_of_tissue']=freezer_location_of_tissue
data['destination']=destination
data=data[['rfid','tissue_type', 'storage_box_of_tissue',
          'project_name','comments','freezer_location_of_tissue','destination','shipping_box']]
data


# ## Insert DF into tissue table

# In[122]:


def insertQuery(query, data):
    connection = psycopg2.connect(database = "PalmerLab_Datasets",
                                  user = "postgres",
                                  password = "palmerlab-amapostgres",
                                  host = "palmerlab-main-database-c2021-08-02.c6sgfwysomht.us-west-2.rds.amazonaws.com",
                                  port = '5432')
    cursor = connection.cursor()
    if "INSERT" in query:
        cols = ",".join([str(i) for i in data.columns.tolist()])
        for i, row in data.iloc[0:1, :].iterrows():
            query = query + " (" + cols + ") VALUES (" + "%s,"*(len(row)-1) + "%s)"
            cursor.execute(query, tuple(row))
            print(list(row))
            connection.commit()
    else:
        return None
    if(connection):
        cursor.close()
        connection.close()
        return



# test=test.drop(columns=['cohort'])


# 9:21
project = "sample_tracking"
query = "INSERT INTO " + project + ".tissue"
data = data ## Change if needed
data.index = range(data.shape[0])
print(list(data.columns))
for index, row in data.iterrows():
    print(index)
    print(list(row))
    insertQuery(query, data.iloc[index:index+1, :]) #OJO print before you run so you dont add a bunch of random stuff

