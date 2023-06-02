import dask.dataframe as dd
from augment_data import get_df, get_augment_data, to_delete
import pandas as pd
import matplotlib.pyplot as plt
from pprint import pprint
from dask_jobqueue import SLURMCluster
from dask.distributed import Client
from time import sleep

"""
results for most popular locations in nyc for parking tickets
                                         name        lat        lng  museum           count
297                    Mott Haven-Port Morris  40.816369 -73.922697   False          586621
292                             Fordham South   40.85644 -73.898821   False          449359
252                                Whitestone  40.786201 -73.817338   False          330980
195                       Morningside Heights  40.810705 -73.957409   False          191524
230                             College Point  40.778029 -73.842967   False          175062
154                            Bushwick South  40.686424  -73.91043   False          164195
245                                 Woodhaven  40.691467 -73.852792   False          143026
263                                 Rego Park  40.728012 -73.862561   False          142379
209  Schuylerville-Throgs Neck-Edgewater Park  40.831829 -73.827642   False          140747
136                   Cypress Hills-City Line  40.689213 -73.872983   False          134695

--------

results for num tickets per state
   state  tickets
0     NY  9706705
1     NJ  1229397
2     PA   449336
3     FL   309320
4     CT   229424
..   ...      ...
61    FO       30
62    SK       29
63    PE       29
64    NF        2
65    NT        1

---------

Most popular parking violations:
    ['PHTO SCHOOL ZN SPEED VIOLATION',
    'NO PARKING-STREET CLEANING',
    'FAIL TO DSPLY MUNI METER RECPT',
    'NO STANDING-DAY/TIME LIMITS',
    'NO PARKING-DAY/TIME LIMITS',
    'FIRE HYDRANT',
    'FAILURE TO STOP AT RED LIGHT',
    'INSP. STICKER-EXPIRED/MISSING',
    'BUS LANE VIOLATION',
    'REG. STICKER-EXPIRED/MISSING',
    'DOUBLE PARKING',
    'EXPIRED MUNI METER',
    'NO STANDING-BUS STOP',
    'NO STANDING-COMM METER ZONE',
    'FRONT OR BACK PLATE MISSING']

--------

Least popular parking violations:
    ['NO STANDING-OFF-STREET LOT',
    'REMOVE/REPLACE FLAT TIRE',
    'BLUE ZONE',
    'ALTERING INTERCITY BUS PERMIT',
    'NO OPERATOR NAM/ADD/PH DISPLAY',
    'EXPIRED METER',
    'NO STANDING EXCP DP',
    'MARGINAL STREET/WATER FRONT',
    'ELEVATED/DIVIDED HIGHWAY/TUNNL',
    'FAILURE TO DISPLAY BUS PERMIT',
    'UNAUTHORIZED PASSENGER PICK-UP',
    'OVERTIME STDG D/S',
    'OT PARKING-MISSING/BROKEN METR',
    'VACANT LOT',
    'NO STOP/STANDNG EXCEPT PAS P/U']

--------

street_name
Broadway                138249
WB N CONDUIT AVE @ S    109881
3rd Ave                 103746
WB N. CONDUIT BLVD @     81076
5th Ave                  72413
2nd Ave                  63084
NB CROSS BAY BLVD @      61571
EB BRUCKNER BLVD @ W     60451
Madison Ave              59221
EB W 14TH STREET @ 5     58492
Lexington Ave            55500
WB HORACE HARDING EX     52572
1st Ave                  52304
WB ASTORIA BLVD N @      50587
6th Ave                  50060
"""

def temp_fun(df):
    return df.compute()


def do_augmented_analysis(file):
    import geopandas as gpd
    import geoplot as gplt
    import geoplot.crs as gcrs


    client = Client(n_workers=3, threads_per_worker = 2, memory_limit='8G')
    places_of_interest = get_augment_data()
    nyc_boroughs = gpd.read_file(gplt.datasets.get_path('nyc_boroughs'))
    ax = nyc_boroughs.plot(figsize=(32,28), zorder=0)
    # print(places_of_interest)
    df = get_df(file)
    grouped_df = df.groupby(['closest_interest_place']).count()
    print("grouped")
    grouped_df.visualize(filename="grouped_test.svg")
    # future = client.compute(grouped_df)
    client = Client(n_workers=8, threads_per_worker=16, memory_limit="8GB") 
    #merged = client.submit(temp_fun, grouped_scattered).result()
    merged = dd.merge(places_of_interest, grouped_df[["summons_number"]], how='inner', left_index=True, right_index=True).compute()
    print("Merged")
    gdf = gpd.GeoDataFrame(
        merged, geometry=gpd.points_from_xy(merged.lng, merged.lat), crs="EPSG:4326"
    )
    print("Plotting")
    gdf["color"] = gdf.apply(lambda row: 'gold' if row['museum'] else 'black', axis=1)
    print(gdf["color"])
    gdf.plot(ax=ax, markersize=[v * 0.5 for v in merged["summons_number"].values], marker='*', color=gdf['color'], zorder=1)
    largest_vals = merged.nlargest(10, 'summons_number')
    print(largest_vals)
    plt.axis('off')
    plt.savefig("Interest_locations_popularity_05_test.png", bbox_inches='tight')
    client.shutdown()

def get_basic_ddf(file):
    ddf = dd.read_parquet(file, engine='pyarrow')
    for col in to_delete:
        ddf = ddf.drop(col, axis=1)
    return ddf[ddf["plate_id"] != 'BLANKPLATE']

def get_count_by_reg_plt(file):
    reg_plts_counted = get_basic_ddf(file).plate_id.value_counts().nlargest(15).compute()
    reg_plts_counted.plot.bar()
    plt.xticks(range(15), reg_plts_counted.index)
    plt.savefig("Registration_plates.png", bbox_inches='tight')

def get_count_by_reg_state(file):
    import plotly.express as px
    basic_ddf = get_basic_ddf(file)
    reg_state_counted = basic_ddf[basic_ddf["registration_state"] != "99"].registration_state.value_counts().compute()
    states_df = pd.DataFrame({"state": reg_state_counted.index, "tickets": reg_state_counted.values})
    # print(states_df)
    fig = px.choropleth(states_df,
                    locations='state', 
                    locationmode="USA-states", 
                    scope="usa",
                    color='tickets',
                    color_continuous_scale="Viridis_r",           
    )
    fig.write_image("Registration_states.png")

def get_count_by_violation_code(file):
    basic_ddf = get_basic_ddf(file).violation_code.value_counts()
    # print(basic_ddf)
    most_popular_violations = basic_ddf.nlargest(15).compute()
    least_popular_violations = basic_ddf.nsmallest(15).compute()
    most_popular_violations.plot.bar()
    plt.xticks(range(15), most_popular_violations.index)
    plt.savefig("Popular_violations.png", bbox_inches='tight')
    violations = pd.read_csv('violation_codes.csv')
    # print(least_popular_violations.index)
    most_viol = [violations.loc[violations["violation_code"] == int(code)]["DEFINITION"].values for code in list(most_popular_violations.index)]
    least_viol = [violations.loc[violations["violation_code"] == int(code)]["DEFINITION"].values for code in list(least_popular_violations.index)]
    most_viol = [v[0] for v in most_viol if len(v) > 0]
    least_viol = [v[0] for v in least_viol if len(v) > 0]
    # print(most_popular_violations.index)
    pprint(most_viol)
    pprint(least_viol)

def get_violations_by_reg_plt(file):
    basic_ddf = get_basic_ddf(file)[["plate_id", "violation_code"]]
    most_reg_plts = list(basic_ddf.plate_id.value_counts().nlargest(15).index)
    rows_by_person = basic_ddf.loc[basic_ddf["plate_id"].isin(most_reg_plts)].compute()
    violations = pd.read_csv('violation_codes.csv')
    rows_by_person["violation_code"] = rows_by_person["violation_code"].astype("int64")
    merged_df = pd.merge(rows_by_person, violations, on='violation_code')
    grouped = merged_df.groupby("plate_id")
    summed_by_price = grouped.sum("price1")
    most_viol = grouped.violation_code.value_counts()
    summed_by_price.plot.bar(y='price1')
    plt.xticks(range(15), summed_by_price.index)
    plt.savefig("Payed_by_reg_plt.png", bbox_inches='tight')
    plt.figure()
    most_viol.plot.bar(y='count')
    plt.savefig("most_viol_by_reg_plt.png", bbox_inches='tight')


def get_streets_with_most_tickets(file):
    basic_ddf = get_basic_ddf(file)
    # print(basic_ddf)
    counts = basic_ddf.street_name.value_counts().nlargest(15).compute()
    counts.plot.bar()
    plt.xticks(range(15), counts.index)
    plt.savefig("Most_popular_streets.png", bbox_inches='tight')
    print(counts)


if __name__ == "__main__":
    file = '/d/hpc/home/aj8977/Project/dataset.parquet'
    do_augmented_analysis(file)
    # get_count_by_reg_plt(file)
    # get_count_by_reg_state(file)
    # get_count_by_violation_code(file)
    # get_violations_by_reg_plt(file)
    # get_streets_with_most_tickets(file)
    print("Done")


"""
TODO:
    - count by registration plate -> done
    - count by registration state -> done
    - count by violation code -> done
    - count by violations per person -> done
    - amount person has payed for tickets -> done
    - streets with most tickets -> done
"""