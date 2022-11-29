#!/usr/bin/env python
# coding: utf-8

# In[ ]:


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
import tqdm


# ## Read in the file 

# In[ ]:


data = pd.read_excel('Box_1_Jhou_Spleens_15November22.xlsx')
data


# ## Data check

# In[ ]:


# fix for casting error 
def reformat(data, columns):
    for col in columns:
        data[col] = data[col].astype(str)
        data[col] = [val.replace('.0','') for val in data[col]]
reformat(data, ["15 digit RFID"])


# In[ ]:


# rename issue for RFID columns
data.rename(columns={'15 digit RFID':'rfid'}, inplace=True)


### this code checks RFID prefix using length, you need to change X to correct length
for value in data['rfid']:
    print(len(str(value)))
    if len(str(value)) < X:
        print('val',value)
        row_idx= data[data['rfid'] == value].index.tolist()
        result=data.iloc[row_idx]
        for i in row_idx:
            data.loc[i, 'rfid']= f'XX{value}' ## change this XX to correct prefix
    else:
        value = X
        pass

for i in data['rfid']: # prints any values under a specific number (checks if rfid code above works)
    if int(i) < 9:
        print(i)
        
data


# ## Read in Tissue table

# In[ ]:



def runQuery(query):
    connection = psycopg2.connect(database = "PalmerLab_Datasets",
                                  user = "postgres",
                                  password = "XXX", # change to real password
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
tissue_df = runQuery("SELECT * FROM " + current_project)
tissue_df


# # Run the following lines of code 

# In[ ]:


#Specific Errors

def comment_error(data): # if comment/note has incorrect name change the X to relevent name
    if 'X' in data.columns:
        data.loc[data['X'] == 'DOUBLE PULLS','X'] = np.nan
        # specific changes for rfid change XX
        data.loc[(data['rfid'] == 'XX'),'comments'] = "DOUBLE PULLS" 
        data = data.drop(columns = ['X'])
    return data
    
def column_error(data): # drop specific columns change XY
    if 'XY' in data.columns:
        data = data.drop(columns=['XY'])
    return data

def error_rows(data): #find if there are specific row errors, change Y
    if 'Y' in data.columns:
        print("Difference between Y and rfid")
        display(data.loc[~(data['Y'] == data['rfid'])])
        print("Y value counts")
        display(data['Y'].value_counts().to_string())
        print("Dropped Y")
        data = data.drop(columns = ['Y'])
    return data
        
def error_rfids(data):
    data['rfid'] = data['rfid'].str.upper()


# In[ ]:


# Check if data columns look correct
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


# In[ ]:


# Check if rfid from tissue sheet is in tissue DF
def rfid_in_tissue(data):
    data["rfid"] = data["rfid"].astype(str)
    tissue_df_rfids = list(tissue_df["rfid"])
    data_rfids = list(data["rfid"])
    duplicate_rfids = list(set(tissue_df_rfids).intersection(set(data_rfids)))
    print(len(duplicate_rfids), "rfids already in tissue_df, out of", len(data_rfids))
    print(duplicate_rfids)
    fix = list(set(data_rfids) - set(tissue_df_rfids))
    

rfid_in_tissue(data)


# In[ ]:


def check_rfid():
    #Check to make sure rfid for the project contains the correct naming convention
    project_metadata = pd.read_csv("project_metadata - project_metadata (1).csv", dtype = str) # this may change depending on how you are reading in the project metadata
    i_convention = list(project_metadata.columns).index("rfid_convention")
    subset = project_metadata[project_metadata["project_name"] == 'XXXX'] # change this to relevant project name
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
            print(rfid, 'XXX')
            display(data.loc[data['rfid']== rfid])
            
check_rfid()      


# ## Check if looks good

# In[ ]:


#RFIDs already in wfu
rfid_in_tissue(data)
#correct specific errors involving comments
data = comment_error(data)
#correct specific errors involving columns
data = column_error(data)
#Drop columns with all NA values 
data = drop_na(data)
#column information
#rows with notable errors
data = error_rows(data)
# error fixing for rfids
error_rfids(data)
#find inccorrect rfids
check_rfid()


# In[ ]:


data


# ## Reformatting DF change XXX

# In[ ]:


# all this information is either from KHAI or tissue sheet
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


# In[ ]:


# Check list
# Lengths match, project name is correct, tissue is correct, freezer is correct
# no RFID issues, if there are duplicates they are tail tissues, if baculum destination is Dean lab


# In[ ]:


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


# ## Insert DF into tissue table

# In[ ]:


def insertQuery(query, data):
    connection = psycopg2.connect(database = "PalmerLab_Datasets",
                                  user = "postgres",
                                  password = "XXXX", # change to real password
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


project = "sample_tracking"
query = "INSERT INTO " + project + ".tissue"
# check if Project + query is correct dataframe
data = data2 ## Change if needed
data.index = range(data.shape[0])
print(list(data.columns))
for index, row in tqdm(data.iterrows()): 
    print(index)
    print(list(row))
    insertQuery(query, data.iloc[index:index+1, :]) #CAREFUL print before you run so you dont add a bunch of random stuff

