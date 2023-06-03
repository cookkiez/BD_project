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
    

if __name__ == '__main__':
    file = '/d/hpc/home/aj8977/Project/dataset.parquet'
    df = get_basic_ddf(file)
    stream_df(df)