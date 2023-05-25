import pandas as pd
from fastparquet import ParquetFile, write
from requests import get
from time import sleep
from datetime import datetime
from sklearn.neighbors import KDTree
import dask.dataframe as dd
from dask.distributed import Client, wait
import dask.array as da
from dask_jobqueue import SLURMCluster
import numpy as np
import subprocess as sp
uid = int(sp.check_output('id -u', shell=True).decode('utf-8').replace('\n',''))
portdash = 10000 + uid

def get_augment_data():
    df_museums = pd.read_csv('MUSEUM.csv')
    lats = []
    lngs = []
    names = []
    for row in df_museums["the_geom"]:
        _, lng, lat = row.split(" ")
        lats.append(lat.replace(')', ''))
        lngs.append(lng.replace('(', ''))

    # Drop duplicates cus there is lots of same schools (elementary, middle, high) with same name and location
    df_schools = pd.read_csv(
        '2019_-_2020_School_Locations.csv').drop_duplicates('NTA_Name', keep='first')
    names = df_museums['NAME'].tolist() + df_schools['NTA_Name'].tolist()
    lats = lats + df_schools["LATITUDE"].tolist()
    lngs = lngs + df_schools["LONGITUDE"].tolist()

    places_of_interest = pd.DataFrame(columns=["name", "lat", "lng"])
    places_of_interest['name'] = names
    places_of_interest['lat'] = lats
    places_of_interest['lng'] = lngs
    return places_of_interest


def write_dask_parquet(df):
    file_to_save = '/d/hpc/home/aj8977/Project/dataset_augmented_full.parquet'
    df.to_parquet(file_to_save, overwrite=True, engine='pyarrow')


def name_fun(ddf, streets_df, kdtree):
    name = ddf["street_name"] 
    row = streets_df.loc[streets_df["street_name"] == name]
    # print(ddf, row, row['lat'], row['lng'], sep='\n')
    if len(row.values) == 0 or row["lat"].values[0] == 'None' or row["lng"].values[0] == 'None':
        return ''
    dist, ind = kdtree.query(
             row[["lat", "lng"]].values.reshape(1, -1), k=3)
    return places_of_interest.loc[ind[0][0]]['name']

def dist_fun(ddf, streets_df, kdtree):
    name = ddf["street_name"] 
    row = streets_df.loc[streets_df["street_name"] == name]
    # print(ddf, row, row['lat'], row['lng'], sep='\n')
    if len(row.values) == 0 or row["lat"].values[0] == 'None' or row["lng"].values[0] == 'None':
        return -1 
    dist, ind = kdtree.query(
             row[["lat", "lng"]].values.reshape(1, -1), k=3)
    return dist[0][0]


def get_closest_place_of_interest(places_of_interest, df, streets_df):
    kdtree = KDTree(places_of_interest[["lat", "lng"]].values)
    cluster = SLURMCluster(cores=20, memory="10000M",
                           scheduler_options={"dashboard_address": f":{portdash}"},
                           walltime='05:00:00',job_extra_directives=["--reservation=fri-vr"])
    cluster.scale(5)
    client = Client(cluster)
    print(client, client.dashboard_link)
    while ((client.status == "running") and (len(client.scheduler_info()["workers"]) < 2)):
        sleep(5)
        print("Waiting for workers...")
    print("Got workers!")
    # divs = df.set_index('summons_number').divisions
    # unique_divisions = list(dict.fromkeys(list(divs)))
    # df = df.set_index('summons_number', divisions=unique_divisions)
    df_scat = client.scatter(df).result()
    # df = df.assign(closest_interest_place=lambda x: '', dist_to_closest_interest_place=lambda x: '')
    meta = df_scat.head().apply(name_fun, axis=1, args=(streets_df, kdtree))
    df_scat['closest_interest_place'] = df_scat.apply(name_fun, axis=1, args=(streets_df, kdtree),
                     meta=meta)
    print("\n----Got names")
    meta = df_scat.head().apply(dist_fun, axis=1, args=(streets_df, kdtree))
    df_scat['dist_to_closest_interest_place'] = df_scat.apply(dist_fun, axis=1, args=(streets_df, kdtree),
                     meta=meta)
    print("\n----Got distances")
    df_scat['closest_interest_place'] = df_scat['closest_interest_place'].astype('str') 
    # df['dist_to_closest_interest_place'] = df['dist_to_closest_interest_place'].astype('float64')
    # print(df['dist_to_closest_interest_place'])
    # print(df['closest_interest_place'])
    # print(df.head(), streets_df.head())

    # for i, street in streets_df.iterrows():
    #     # print(street, type(street["lat"]), street["lat"] is None, type(None))
    #     if street["street_name"] is None or street["lat"] == 'None' or street["lng"] == 'None':
    #         continue
    #     dist, ind = kdtree.query(
    #         street[["lat", "lng"]].values.reshape(1, -1), k=3)
    #     closest_interest_place = places_of_interest.loc[ind[0][0]]
    #     rows = df.loc[df["street_name"] == street["street_name"]]
    #     df["closest_interest_place"] = df["closest_interest_place"].mask(df["street_name"] == street["street_name"], closest_interest_place["name"])
    #     df["dist_to_closest_interest_place"] = df["dist_to_closest_interest_place"].mask(df["street_name"] == street["street_name"], dist)
    #     if i % 1000 == 0:
    #         print(i)
    # print(a_df)

    
    # print(df.head())
    # print(df.divisions, df.known_divisions)
    return df_scat

# pf = ParquetFile('dataset.parquet')
streets_df = dd.read_parquet('/d/hpc/home/aj8977/Project/streets_full.parquet', 
                         engine='pyarrow',).drop_duplicates().compute()
ddf = dd.read_parquet('/d/hpc/home/aj8977/Project/dataset.parquet', 
                         engine='pyarrow')#.repartition(partition_size="800MB")
print("Read parquet file:", ddf.head(), ddf.npartitions)
places_of_interest = get_augment_data()

augmented_df = get_closest_place_of_interest(places_of_interest, ddf, streets_df)
print(augmented_df.head())
# print(augmented_df["closest_interest_place"])
write_dask_parquet(augmented_df)
