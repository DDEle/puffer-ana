#!/usr/bin/env python
# coding: utf-8

# In[1]:


import pandas as pd
from collections import defaultdict
filepath = "./client_buffer_2021-02-08T11_2021-02-09T11.csv"
partialFilePath = "./client_buffer_partial.csv"


# In[2]:


class IterableBuffer(type):
    def __iter__(cls):
        return iter(cls.__name__)
class BufferData:
    __metaclass__ = IterableBuffer
    def __init__(self, time, expt_id, channel, event, cum_rebuf):
        self.time = time
        self.channel = channel
        self.event = event
        self.expt_id = expt_id
        self.cum_rebuf = cum_rebuf


# In[ ]:


file = filepath


# In[ ]:


df = pd.read_csv(file)


# In[ ]:


len(df)


# In[ ]:


if file!= partialFilePath:
    partial_df = df[:1000000]
    partial_df.to_csv("client_buffer_partial.csv",index=True)
    df = partial_df


# In[ ]:


partial_df.to_csv("client_buffer_partial.csv",index=True)


# In[3]:


#Split the big file into serveral small files, and each file size is 1000000
start = 0
interval = 999999
while (True):
    if (start + interval > len(df)):
        partial_df = df[start:len(df)-1]
        partial_df.to_csv("client_buffer_partial_"+str(start/1000000)+".csv",index=True)
        break
    else:
        partial_df = df[start:start+interval]
        partial_df.to_csv("client_buffer_partial_"+str(start/1000000)+".csv",index=True)
        start = start+interval+1


# In[27]:


def analyzeSmallFile(name):
    df = pd.read_csv(name)
    dataPts = defaultdict(list)
    clients = defaultdict(int)
    total = 0;
    dfSiz = len(df.index)
    print("start to read...."+name)
    for i in df.index:
        b = BufferData(df.iloc[i]["time (ns GMT)"], df.iloc[i]["expt_id"], df.iloc[i]["channel"], df.iloc[i]["event"], df.iloc[i]["cum_rebuf"])
        dataPts[(df.iloc[i]["session_id"], str(df.iloc[i]["index"]))].append(b) # df.iloc[i]: get the item from idx
        clients[df.iloc[i]["session_id"]] += 1
    # get the total user
    print("total data points: "+str(len(dataPts)))
    print("total client: "+ str(len(clients)))
    stallInfos = defaultdict(list)
    totalTime = 0.0
    totalStallTime = 0.0
    for d in dataPts:
        if len(dataPts[d]) > 3 and dataPts[d][1].event == "startup":
            totalTime = (float)(dataPts[d][len(dataPts[d])-1].time - dataPts[d][1].time)/1000000000
            totalStallTime = (float)(dataPts[d][len(dataPts[d])-1].cum_rebuf - dataPts[d][1].cum_rebuf)
            stallInfos[d] = [totalTime, totalStallTime]
    for d in stallInfos:
        item = stallInfos[d]
    totalTime = 0.0
    totalStallTime = 0.0
    for d in stallInfos:
        totalTime+=stallInfos[d][0]
        totalStallTime += stallInfos[d][1]
    print("totalTime:"+str(totalTime))
    print("totalSTallTime:"+str(totalStallTime))
    return [totalTime, totalStallTime]


# In[28]:


totalTime = []
totalStallTime = []
for i in range(0,32):
    [currTotal, currStall] = analyzeSmallFile("client_buffer_partial_"+str(i)+".0.csv")
    totalTime.append(currTotal)
    totalStallTime.append(currStall)


# In[29]:


print(totalTime)
print(totalStallTime[1])


# In[33]:


totalStallTime


# In[35]:


dayTotal = 0.0
dayStallTotal = 0.0
for i in range(0, len(totalTime)):
    dayTotal += totalTime[i]
    dayStallTotal += totalStallTime[i]


# In[36]:


avg = dayStallTotal/dayTotal


# In[37]:


print(avg)


# In[30]:


import json


# In[31]:


data = {}
data["info"] = []
fake = [1,2,3,4,5]
for i in range(0, len(totalTime)):
    data["info"].append({
        "totalTime":totalTime[i],
        "totalStallTime":totalStallTime[i]
    })


# In[32]:


with open('data.txt', 'w') as outfile:
    json.dump(data, outfile)


# In[ ]:


for i in range(3,32):
    [currTotal, currStall] = analyzeSmallFile("client_buffer_partial_"+str(i)+".0.csv")
    totalTime.append(currTotal)
    totalStallTime.append(totalStallTime)


# In[ ]:


for d in 


# In[ ]:


dataPts = defaultdict(list)
clients = defaultdict(int)
total = 0;
dfSiz = len(df.index)


# In[ ]:





# In[ ]:


b = BufferData(df.iloc[0]["time (ns GMT)"], df.iloc[0]["expt_id"], df.iloc[0]["channel"], df.iloc[0]["event"], df.iloc[0]["cum_rebuf"])


# In[ ]:





# In[ ]:


dfSiz


# In[ ]:


for i in df.index:
    b = BufferData(df.iloc[i]["time (ns GMT)"], df.iloc[i]["expt_id"], df.iloc[i]["channel"], df.iloc[i]["event"], df.iloc[i]["cum_rebuf"])
    dataPts[(df.iloc[i]["session_id"], str(df.iloc[i]["index"]))].append(b) # df.iloc[i]: get the item from idx
    clients[df.iloc[i]["session_id"]] += 1


# In[ ]:


# get the total user
print("total data points: "+str(len(dataPts)))
print("total client: "+ str(len(clients)))


# In[ ]:


dataPts[('j71je0LfT2ch+e8frsjDAtgDaGhYNv7BCwA92vG6XDA=', '0')]


# In[ ]:


stallInfos = defaultdict(list)
totalTime = 0.0
totalStallTime = 0.0
for d in dataPts:
    if len(dataPts[d]) > 3 and dataPts[d][1].event == "startup":
        totalTime = (float)(dataPts[d][len(dataPts[d])-1].time - dataPts[d][1].time)/1000000000
        totalStallTime = (float)(dataPts[d][len(dataPts[d])-1].cum_rebuf - dataPts[d][1].cum_rebuf)
        stallInfos[d] = [totalTime, totalStallTime]


# In[ ]:


for d in stallInfos:
    item = stallInfos[d]
    print(item)


# In[ ]:


totalTime = 0.0
totalStallTime = 0.0
for d in stallInfos:
    totalTime+=stallInfos[d][0]
    totalStallTime += stallInfos[d][1]


# In[ ]:





# In[ ]:





# In[ ]:


print((float)(totalStallTime/totalTime))


# In[ ]:




