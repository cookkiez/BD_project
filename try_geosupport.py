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


def get_data_from_api(df):
    print("Getting data from api")
    lats = []
    lngs = []
    api_url = 'https://geosearch.planninglabs.nyc/v2/search?text='
    cnt = 0

    for row in df['street_name']:
        response = get(api_url + row).json()
        feats = response['features']
        lng, lat = feats[0]['geometry']['coordinates'] if len(
            response['features']) != 0 else (None, None)
        lats.append(lat)
        lngs.append(lng)
        # sleep(0.1)
        cnt += 1
        if cnt % 100 == 0:
            print(cnt, datetime.now())
    df['lat'] = lats
    df['lng'] = lngs
    df = df[df['lat'].notna()]
    df = df[df['lng'].notna()]
    return df


def test(row):
    api_url = 'https://geosearch.planninglabs.nyc/v2/search?text='
    # print(row)
    return get(api_url + row).json()


def get_data_from_api_dask(ddf):
    import time
    start_time = time.time()
    lats = []
    lngs = []
    cluster = SLURMCluster(cores=10, processes=1, memory="800M",
                            scheduler_options={"dashboard_address": f":{portdash}"})
    cluster.scale(1)
    client = Client(cluster)
    futures = client.map(test, ddf['street_name'])
    cnt = 0
    a = client.gather(futures)
    print("\n\n------GOT ALL RESULTS")
    for response in a:
        feats = response['features']
        lng, lat = feats[0]['geometry']['coordinates'] if len(feats) != 0 else (None, None)
        lats.append(str(lat))
        lngs.append(str(lng))
        cnt += 1
    print((time.time() - start_time))
    chunks = ddf.map_partitions(lambda x: len(x)).compute().to_numpy()
    la = da.from_array(lats, chunks=tuple(chunks))
    ln = da.from_array(lngs, chunks=tuple(chunks))
    ddf['lat'] = la
    ddf['lng'] = ln

    cluster.close()
    client.close()
    return ddf


def write_parquet(df):
    file_to_save = 'dataset-lat-lng.parquet'
    write(file_to_save, df)


def write_dask_parquet(df):
    file_to_save = '/d/hpc/home/aj8977/Project/dataset-lat-lng.parquet'
    dd.to_parquet(df, file_to_save, overwrite=True, engine='fastparquet')


def get_closest_place_of_interest(places_of_interest, df):
    kdtree = KDTree(places_of_interest[["lat", "lng"]].values)
    interest_places = []
    dist_to_place = []
    for i, row in df.iterrows():
        dist, ind = kdtree.query(
            row[["lat", "lng"]].values.reshape(1, -1), k=3)
        closet_interest_place = places_of_interest.loc[ind[0][0]]
        interest_places.append(closet_interest_place["name"])
        dist_to_place.append(dist[0][0])

    df["closest_interest_place"] = interest_places
    df["dist_to_closest_interest_place"] = dist_to_place
    return df

print("Hello")
# pf = ParquetFile('dataset.parquet')
rChunk = dd.read_parquet('/d/hpc/home/aj8977/Project/dataset.parquet', engine='fastparquet')
print("Read parquet file:", rChunk.shape)
ddf = get_data_from_api_dask(rChunk)
print("New dataframe shape: ", ddf.shape)
# # places_of_interest = get_augment_data()

# # augmented_df = get_closest_place_of_interest(places_of_interest, df)
write_dask_parquet(ddf)
