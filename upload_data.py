import pandas as pd
import json
from pymongo.mongo_client import MongoClient

uri = "mongodb+srv://SachdevaVansh:mongodb@cluster0.1plyvnm.mongodb.net/?retryWrites=true&w=majority"

# Create a new client and connect to the server
client = MongoClient(uri)

#create database name and collection name
DATABASE_NAME="sensor_project"
COLLECTION_NAME="waferfault"

# read the data as a dataframe
df=pd.read_csv(r"D:\Python Projects\Wafer_fault_detection\notebooks\wafer_23012020_041211.csv")
df=df.drop("Unnamed: 0",axis=1)

## converting .csv to json
json_record=list(json.loads(df.T.to_json()).values())

#now dumpt the data into the dtabase
client[DATABASE_NAME][COLLECTION_NAME].insert_many(json_record)


# Send a ping to confirm a successful connection
try:
    client.admin.command('ping')
    print("Pinged your deployment. You successfully connected to MongoDB!")
except Exception as e:
    print(e)