import pandas as pd
from requests import get
import dask.dataframe as dd
from dask.distributed import Client, wait
import dask.array as da
from dask_jobqueue import SLURMCluster
import subprocess as sp
from fastparquet import write
uid = int(sp.check_output('id -u', shell=True).decode('utf-8').replace('\n',''))
portdash = 10000 + uid

def test(row):
    api_url = 'https://geosearch.planninglabs.nyc/v2/search?text='
    if row is None:
        return []
    return get(api_url + row).json()

def write_parquet(df):
    print("WRITE", df.head())
    # write('/d/hpc/home/aj8977/Project/streets.parquet', df)
    df.to_parquet('/d/hpc/home/aj8977/Project/streets.parquet', overwrite=True, engine='fastparquet', ignore_divisions=True, write_index=False)

def get_data_from_api_dask(ddf):
    import time
    start_time = time.time()
    lats = []
    lngs = []
    cluster = SLURMCluster(cores=4, memory="5000M",
                            scheduler_options={"dashboard_address": f":{portdash}"})
    cluster.scale(5)
    print(cluster)
    client = Client(cluster)
    print(client)
    futures = client.map(test, ddf['street_name'])
    cnt = 0
    for response in client.gather(futures):
        if type(response) == type([]):
            lng, lat = (None, None)
        else:
            feats = response['features']
            lng, lat = feats[0]['geometry']['coordinates'] if len(feats) != 0 else (None, None)
        lats.append(str(lat))
        lngs.append(str(lng))
        cnt += 1
    print((time.time() - start_time))
    # chunks = ddf.map_partitions(lambda x: len(x)).compute().to_numpy()
    # la = da.from_array(lats, chunks=tuple(chunks))
    # ln = da.from_array(lngs, chunks=tuple(chunks))
    la = pd.DataFrame(lats, columns=["lat"])
    ln = pd.DataFrame(lngs, columns=["lng"])
    ddf['lat'] = la["lat"]
    ddf['lng'] = ln["lng"]
    # cluster.close()
    # client.close()
    return ddf


ddf = dd.read_parquet('/d/hpc/home/aj8977/Project/dataset_small.parquet', 
                         engine='fastparquet',)

streets_ddf = ddf.drop_duplicates(subset='street_name')
df = dd.from_pandas(pd.DataFrame(data=streets_ddf["street_name"], columns=['street_name']), npartitions=1)
print(df.head())
df = get_data_from_api_dask(df)
print(df.head())
write_parquet(df)