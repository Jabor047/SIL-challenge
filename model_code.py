#!/usr/bin/env python
# coding: utf-8

# # SAVANNAH INFORMATICS LIMITED

# In[1]:


from google.cloud import bigquery
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt


# In[8]:


client = bigquery.Client()
query = "SELECT * from sil-interviews.kevinkarobia.kevin_claims"
df = client.query(query).to_dataframe()
df.head()


# ## Preprocessing

# In[ ]:


from sklearn.preprocessing import LabelEncoder


# In[9]:


df.columns


# In[10]:


# drop identifying columns
df.drop(["invoiceid", "invoiceprovider", "encounter_id", "invoice_linesid", "invoice_linesprovider", "encountersid",
         "encountersprovider", "encounter_code", "member_id", "parquetsid", "mondayapi_id"], axis=1, inplace=True)
df.head()


# In[11]:


df.dtypes


# In[12]:


def dateTuning(df, colname):
    df[colname] = pd.to_datetime(df[colname])
    df[colname+'_day'] = df[colname].dt.dayofweek
    df[colname+'_Month'] = df[colname].dt.month
    df.drop([colname], axis=1, inplace=True)
    return df

dateCols = ["diagnosed_date", "invoice_date", "started_on"]
for col in dateCols:
    df = dateTuning(df, col)

df.head()


# In[13]:


df.dtypes


# In[14]:


# the object columns to numerical values we can use in our model
categoricalCols = ["service_type", "invoice_linesname", "mondayapi_name", "card_type", "status"]

for catCol in categoricalCols:
    labelEnc = LabelEncoder()
    df[catCol] = labelEnc.fit_transform(df[catCol])


# ## Training

# In[16]:


from sklearn.model_selection import train_test_split
from sklearn.ensemble import RandomForestRegressor
from sklearn.metrics import mean_squared_error

Y = df["price"]
X = df.drop(["price"], axis=1)

# divide the dataset into test and training sets
xTrain, xTest, yTrain, yTest = train_test_split(X, Y, test_size=0.2, random_state=42)

# define the model
rfReg = RandomForestRegressor(n_estimators=128)

# training the model
rfReg.fit(xTrain, yTrain)


# In[17]:


# predict on the test set
yPred = rfReg.predict(xTest)

# Check the models performane
err = mean_squared_error(yTest, yPred)
err


# > The model performances poor due to minimal preprocessing done and hyper-parameter tuning done (due to time constraints). Some steps to improve the model's performance would include stacking of machine learning models and better hyper-parameter tuning

# In[ ]:




