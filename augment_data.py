import pandas as pd
from requests import get
from time import sleep
from datetime import datetime
from sklearn.neighbors import KDTree
import dask.dataframe as dd
from dask.distributed import Client, wait
from dask_jobqueue import SLURMCluster
import pyarrow as pa


to_delete = ['issue_date', 'vehicle_body_type', 'street_code1', 'street_code2', 'street_code3', 'vehicle_expiration_date',
             'violation_location', 'issuer_command', 'violation_time', 'time_first_observed', 'house_number', 'intersecting_street',
             'date_first_observed', 'sub_division', 'violation_legal_code', 'from_hours_in_effect', 'to_hours_in_effect', 
             'meter_number', 'violation_post_code', 'violation_description', 'no_standing_or_stopping_violation', 'hydrant_violation', 'double_parking_violation'
]

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
    museum = [True] * len(df_museums['NAME'].tolist()) + [False] * len(df_schools['NTA_Name'].tolist()) 
    lats = lats + df_schools["LATITUDE"].tolist()
    lngs = lngs + df_schools["LONGITUDE"].tolist()

    places_of_interest = pd.DataFrame(columns=["name", "lat", "lng", 'museum'])
    places_of_interest['name'] = names
    places_of_interest['lat'] = lats
    places_of_interest['lng'] = lngs
    places_of_interest['museum'] = museum
    return places_of_interest


def write_dask_parquet(df):
    file_to_save = '/d/hpc/home/aj8977/Project/dataset_augmented_full_3.parquet'
    print("Storing to parquet")
    # ddf = df.persist()
    print("GOT HERE")
    # for col in to_delete:
    #     df = df.drop(col, axis=1)
    # df = df.set_index("summons_number")
    schema = pa.Schema.from_pandas(df.head(), preserve_index=False)
    print(schema, schema[0])
    df.to_parquet(file_to_save, compression="snappy", engine="pyarrow", schema=schema, write_index=False)


def name_fun(ddf, streets_df, kdtree, places):
    name = ddf["street_name"] 
    row = streets_df.loc[streets_df["street_name"] == name]
    # print(ddf, row, row['lat'], row['lng'], sep='\n')
    if len(row.values) == 0 or row["lat"].values[0] == 'None' or row["lng"].values[0] == 'None':
        return ''
    dist, ind = kdtree.query(
             row[["lat", "lng"]].values.reshape(1, -1), k=3)
    return ind[0][0]

def dist_fun(ddf, streets_df, kdtree):
    name = ddf["street_name"] 
    row = streets_df.loc[streets_df["street_name"] == name]
    # print(ddf, row, row['lat'], row['lng'], sep='\n')
    if len(row.values) == 0 or row["lat"].values[0] == 'None' or row["lng"].values[0] == 'None':
        return -1 
    dist, ind = kdtree.query(
             row[["lat", "lng"]].values.reshape(1, -1), k=3)
    return dist[0][0]


def fun(df):
    for col in to_delete:
        df = df.drop(col, axis=1)
    places_of_interest = get_augment_data()
    kdtree = KDTree(places_of_interest[["lat", "lng"]].values)
    streets_df = dd.read_parquet('streets_full.parquet', 
                         engine='pyarrow',).drop_duplicates().compute()
    print("Getting names")
    meta = df.head().apply(name_fun, axis=1, args=(streets_df, kdtree, places_of_interest))
    df['closest_interest_place'] = df.apply(name_fun, axis=1, args=(streets_df, kdtree, places_of_interest),
                     meta=meta)
    print("Getting dists")
    meta = df.head().apply(dist_fun, axis=1, args=(streets_df, kdtree))
    print("Got dists meta")
    df['dist_to_closest_interest_place'] = df.apply(dist_fun, axis=1, args=(streets_df, kdtree),
                     meta=meta)
    print("Transforming")
    # df['closest_interest_place'] = df['closest_interest_place'].astype('str') 
    for col in df.columns:
        if type(df[col]) == type(object):
            df[col] = df[col].astype(str)
        if type(df[col]) == type(""):
            df[col] = df[col].str[:25]
    # df = df.set_index("closest_interest_place")
    return df


def get_closest_place_of_interest(df):
    # cluster = SLURMCluster(processes=5, cores=25, memory="8GB",
    #                        scheduler_options={"dashboard_address": f":{portdash}"},
    #                        walltime='05:00:00',job_extra_directives=["--reservation=fri-vr"])
    # cluster.scale(5)
    # client = Client(n_workers=4, threads_per_worker=5, memory_limit="7GB") 
    # client = Client(cluster)
    # print(client, client.dashboard_link)
    # divs = df.set_index('summons_number').divisions
    # unique_divisions = list(dict.fromkeys(list(divs)))
    # df = df.set_index('summons_number', divisions=unique_divisions)
    # df = df.set_index('summons_number')
    # while ((client.status == "running") and (len(client.scheduler_info()["workers"]) < 3)):
    #    sleep(10)
    #    print("Waiting for workers...")
    # print("Got workers!")
    # df_scat = client.scatter(df)
    # df = df.set_index('summons_number')
    # df_scat = df_scat.set_index('summons_number')
    # res = client.submit(fun, df_scat)
    # meta = df.head().apply(fun, axis=1)
    # res = df.map_partitions(fun, meta=meta)
    res = fun(df)
    print("GOT RESULT")
    # write_dask_parquet(res)
    # print("GOT RESULT")
    return res

def get_df(file):
    ddf = dd.read_parquet(file, engine='pyarrow')
    print("Read parquet file:", ddf.head())
    augmented_df = get_closest_place_of_interest(ddf)
    return augmented_df


def main():
    df = get_df('/d/hpc/home/aj8977/Project/dataset.parquet')


if __name__ == "__main__":
    main()

# pf = ParquetFile('dataset.parquet')

# print("END")
# print(augmented_df["closest_interest_place"])

