import dask.dataframe as dd
from augment_data import get_df, get_augment_data, to_delete
import pandas as pd
import matplotlib.pyplot as plt
from pprint import pprint
from dask_jobqueue import SLURMCluster
from dask.distributed import Client
from time import sleep
from dask_ml.model_selection import train_test_split
import matplotlib.pyplot as plt
from math import sqrt
from streamz import Stream
from pprint import pprint
from datetime import datetime

months = dict()
days = dict()
vehicles = dict()

def get_basic_ddf(file):
    ddf = dd.read_parquet(file, engine='pyarrow')
    for col in to_delete:
        ddf = ddf.drop(col, axis=1)
    return ddf[ddf["plate_id"] != 'BLANKPLATE']

def temp_fun(row):
    for r in row:
        m = r["issue_date"].month
        d = r["issue_date"].weekday()
        
        if m not in months:
            months[m] = 0
        months[m] += 1
        
        if d not in days:
            days[d] = 0
        days[d] += 1

        v = r["vehicle_make"]
        #v = r["vehicle_body_type"]
        if v != None:
            if v not in vehicles:
                vehicles[v] = 0
            vehicles[v] += 1            

def sinker(a):
    pass

def stream_df(df):
    print("Opened file, starting stream")
    source = Stream()
    source.sliding_window(100).map(temp_fun).sink(sinker)
    cnt = 0
    for row in df.get_partition(0).sort_values(by="issue_date").iterrows():
        source.emit(row[1])
        cnt += 1
        if cnt % 10000 == 0:
            print(f"Streamed {cnt} rows")

    do_plot()

def do_plot():
    days_names = ["Monday", "Tuesday", "Wednesday", "Thursday", "Friday", "Saturday", "Sunday"]
    months_names = ["January", "Februray", "March", "April", "May", "June", "July", "August", "September", "October", "November", "December"]
    labels_d = [days_names[d] for d, _ in days.items()]
    labels_m = [months_names[m-1] for m, _ in months.items()]
    
    fig, ax = plt.subplots()
    ax.pie(list(months.values()), labels=labels_m)
    plt.savefig("Tickets_by_months", bbox_inches='tight')

    plt.clf()
    fig, ax = plt.subplots()
    ax.pie(list(days.values()), labels=labels_d)
    plt.savefig("Tickets_by_weekdays", bbox_inches='tight')

    plt.clf()
    vehicles_ = {k: v for k, v in sorted(vehicles.items(), key=lambda item: item[1], reverse=True)}
    n = 19
    vehicles1 = list(vehicles_.items())[:n]
    vehicles2 = sum(list(vehicles_.values())[n:])
    vehicles1.append(("OTHER", vehicles2))
    for i in range(len(vehicles1)):
        v, c = vehicles1[i]
        if v == "TOYOT":
            v = "TOYOTA"
        elif v == "NISSA":
            v = "NISSAN"
        elif v == "ME/BE":
            v = "MERCEDES BENZ"
        elif v == "CHEVR":
            v = "CHEVROLET"
        elif v == "HYUND":
            v = "HYUNDAI"
        elif v == "SUBAR":
            v = "SUBARU"
        elif v == "VOLKS":
            v = "VOLKSWAGEN"
        elif v == "INFIN":
            v = "INFINITY"
        vehicles1[i] = (v, c)
    fig, ax = plt.subplots()
    ax.pie([c for v, c in vehicles1], labels=[v for v, c in vehicles1])
    plt.savefig("Tickets_by_manufacturers", bbox_inches='tight')
    

if __name__ == '__main__':
    file = '/d/hpc/home/aj8977/Project/dataset.parquet'
    df = get_basic_ddf(file)
    stream_df(df)