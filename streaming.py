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

borough_vals_to_write = []
street_vals_to_write = []
bor_dates = []
str_dates = []
str_skewness = []
bor_skewness = []

def get_basic_ddf(file):
    ddf = dd.read_parquet(file, engine='pyarrow')
    for col in to_delete:
        ddf = ddf.drop(col, axis=1)
    return ddf[ddf["plate_id"] != 'BLANKPLATE']

def get_median(ls):
    ls_sorted = ls.sort()
    if len(ls) % 2 != 0:
        m = int((len(ls)+1)/2 - 1)
        return ls[m]
    else:
        m1 = int(len(ls)/2 - 1)
        m2 = int(len(ls)/2)
        return (ls[m1]+ls[m2])/2

def get_mean(x):
    return sum(x)/len(x)


def get_std_dev(x, mean):
    std_dev = 0
    for val in x:
        std_dev += abs(val - mean)**2
    return sqrt(std_dev / len(x))

def temp_fun(row):   
    interesting_streets = ["Broadway", "WB N CONDUIT AVE @ S", "3rd Ave ", "WB N. CONDUIT BLVD @", 
                           "5th Ave", "2nd Ave", "NB CROSS BAY BLVD @", "EB BRUCKNER BLVD @ W", "Madison Ave",
                           "EB W 14TH STREET @ 5", "Lexington Ave", "WB HORACE HARDING EX", "1st Ave", 
                           "WB ASTORIA BLVD N @", "6th Ave"]
    streets = {s:0 for s in interesting_streets}
    boroughs = {}
    for el in row:
        b = el["violation_county"]
        if b != "None":
            if b not in boroughs:
                boroughs[b] = 0
            boroughs[b] += 1
        if el["street_name"] in interesting_streets:
            streets[el["street_name"]] += 1
    b_vals = list(boroughs.values())
    if len(b_vals) > 0:
        mean_bor = get_mean(b_vals)
        median_bor = get_median(b_vals)
        std_bor = get_std_dev(b_vals, mean_bor)
        b_skew = ((mean_bor - median_bor) * 3) / std_bor if std_bor != 0 else 0
        # print("\nTickets per borough\n","Mean: ", mean_bor, "Standard deviation: ", std_bor,
        #     "Max:", max(b_vals), "Min:", min(b_vals), "Median: ", median_bor, "Skewness: ", b_skew)
        borough_vals_to_write.append((mean_bor, median_bor, std_bor, max(b_vals), min(b_vals)))
        bor_dates.append(row[0]["issue_date"])
        bor_skewness.append(b_skew)

    street_vals = list(streets.values())
    if len(street_vals) > 0:
        mean_str = get_mean(street_vals)
        median_str = get_median(street_vals)
        std_str = get_std_dev(street_vals, mean_str)
        str_skew = ((mean_str - median_str) * 3) / std_str if std_str != 0 else 0
        # print("\nTickets per street\n", "Mean: ", mean_str, "Standard deviation: ", std_str,
        #     "Max:", max(street_vals), "Min:", min(street_vals), "Median: ", median_str, "Skewness", str_skew)
        street_vals_to_write.append((mean_str, median_str, std_str,max(street_vals), min(street_vals)))
        str_dates.append(row[0]["issue_date"])
        str_skewness.append(str_skew)
    return ""

def do_plot(vals, dates, name):
    means = []
    medians = []
    stds = []
    maxs = []
    mins = []
    dates_labels = []
    zipped = list(zip(dates, vals))
    zipped.sort(key=lambda x: x[0])
    prev_date = zipped[0][0]
    cnt = 0
    means_tmp = 0
    med_tmp = 0
    std_tmp = 0
    max_tmp = 0
    min_tmp = 0
    for i, (date, row) in enumerate(zipped):
        (a, b, c, e, f) = row
        means_tmp += a
        med_tmp += b
        std_tmp += c
        max_tmp += e
        min_tmp += f
        cnt += 1
        if prev_date != date:
            dates_labels.append(date)
            means.append(means_tmp / cnt)
            medians.append(med_tmp / cnt)
            stds.append(std_tmp / cnt)
            maxs.append(max_tmp / cnt)
            mins.append(min_tmp / cnt)
            cnt = 0
            means_tmp = 0
            med_tmp = 0
            std_tmp = 0
            max_tmp = 0
            min_tmp = 0
            prev_date = date
    
    ys = [means, medians, stds,  maxs, mins]
    labels = ["Mean", "Median", "Standard deviation", "Maximum", "Minimum"]
    plt.figure()
    for i, y in enumerate(ys):
        plt.plot(dates_labels, y, label=labels[i])
    plt.legend()
    plt.xticks(rotation = 45)
    plt.savefig(name, bbox_inches='tight')

def sinker(a):
    pass

def get_dates_vals_skew(dates, skews):
    zipped_str = list(zip(dates, skews))
    zipped_str.sort(key=lambda x: x[0])
    prev_date = zipped_str[0][0]
    new_list = []
    new_date = []
    cnt = 0 
    skew_tmp = 0
    for i, (date, row) in enumerate(zipped_str):
        skew_tmp += row
        cnt += 1
        if date != prev_date:
            new_date.append(prev_date)
            new_list.append(skew_tmp / cnt)
            skew_tmp = 0
            cnt = 0
            prev_date = date
    return new_date, new_list

def stream_df(df):
    from datetime import datetime
    print("Opened file, starting stream")
    source = Stream()
    source.sliding_window(100).map(temp_fun).sink(sinker)
    cnt = 0
    dt = datetime(2022, 6, 30)
    for row in df.get_partition(0).sort_values(by="issue_date").iterrows():
    # for row in df.sort_values(by="issue_date").iterrows():
        # curr_date = datetime.strptime(row[1]["issue_date"], '%m/%d/%y')
        if row[1]["issue_date"] > dt:
            source.emit(row[1])
            cnt += 1
            #sleep(0.05)
            if cnt % 10000 == 0:
                print(f"Streamed {cnt} rows")
    do_plot(borough_vals_to_write, bor_dates, "Boroughs_streaming.png")
    do_plot(street_vals_to_write, str_dates, "Streets_streaming.png")

    plt.figure()
    s_dates, s_skews = get_dates_vals_skew(str_dates, str_skewness)
    b_dates, b_skews = get_dates_vals_skew(bor_dates, bor_skewness)
    plt.plot(s_dates, s_skews, label="Streets skewness")
    plt.plot(b_dates, b_skews,label="Boroughs skewness")
    plt.xticks(rotation = 45)
    plt.legend()
    plt.savefig("Skewness.png", bbox_inches='tight')

if __name__ == '__main__':
    file = '/d/hpc/home/aj8977/Project/dataset.parquet'
    df = get_basic_ddf(file)
    stream_df(df)
    
    