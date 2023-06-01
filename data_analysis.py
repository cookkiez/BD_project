import dask.dataframe as dd
from augment_data import get_df, get_augment_data, to_delete
import pandas as pd
import matplotlib.pyplot as plt
from pprint import pprint

"""
results for most popular locations in nyc for parking tickets
                                         name                lat                 lng  museum           count
319                                Co-op City          40.874035          -73.831669   False              78
209  Schuylerville-Throgs Neck-Edgewater Park          40.831829          -73.827642   False              28
96                       Queens Museum of Art   40.7458428647494   -73.8467627581779    True              11
288                              East Tremont          40.850131           -73.89163   False              11
307                               Hunts Point          40.814034          -73.886834   False              11
302                                Mount Hope          40.848344          -73.903821   False              10
108               South Street Seaport Museum  40.70660335756895  -74.00372094057576    True               9
230                             College Point          40.778029          -73.842967   False               9
297                    Mott Haven-Port Morris          40.816369          -73.922697   False               9
88                    New York Transit Museum  40.69052369812635   -73.9900259713183    True               8

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
"""

def do_augmented_analysis(file):
    import geopandas as gpd
    import geoplot as gplt
    import geoplot.crs as gcrs

    places_of_interest = get_augment_data()
    nyc_boroughs = gpd.read_file(gplt.datasets.get_path('nyc_boroughs'))
    ax = nyc_boroughs.plot(figsize=(32,28), zorder=0)
    # print(places_of_interest)
    df = get_df(file)
    grouped_df = df.groupby(['closest_interest_place']).count()
    print("grouped")
    merged = dd.merge(places_of_interest, grouped_df[["summons_number"]].compute(), how='inner', left_index=True, right_index=True)
    print("Merged")
    # print(merged)
    gdf = gpd.GeoDataFrame(
        merged, geometry=gpd.points_from_xy(merged.lng, merged.lat), crs="EPSG:4326"
    )
    print("Plotting")
    gdf["color"] = gdf.apply(lambda row: 'gold' if row['museum'] else 'black', axis=1)
    print(gdf["color"])
    gdf.plot(ax=ax, markersize=[x * 20 for x in merged["summons_number"].values], marker='*', color=gdf['color'], zorder=1)
    largest_vals = merged.nlargest(10, 'summons_number')
    print(largest_vals)
    plt.axis('off')
    plt.savefig("Interest_locations_popularity.png", bbox_inches='tight')

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


if __name__ == "__main__":
    file = '/d/hpc/home/aj8977/Project/dataset_small.parquet'
    # do_augmented_analysis(file)
    # get_count_by_reg_plt(file)
    # get_count_by_reg_state(file)
    # get_count_by_violation_code(file)
    get_violations_by_reg_plt(file)
    print("Done")


"""
TODO:
    - count by registration plate -> done
    - count by registration state -> done
    - count by violation code -> done
    - count by violations per person -> done
    - amount person has payed for tickets -> done
    - count by issuer precinct
    - count by vehicle color
    - count by unregistered vehicle
    - count by vehicle make
"""