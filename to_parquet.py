import pandas as pd
import numpy as np
import fastparquet as fp
import dask.dataframe as dd

dtypes= {
    'Summons Number' : int, 'Plate ID' : str, 'Registration State' : str, 'Plate Type' : str, 'Issue Date' : str,
    'Violation Code' : str, 'Vehicle Body Type' : str, 'Vehicle Make' : str, 'Issuing Agency' : str, 'Street Code1' : int,
    'Street Code2' : int, 'Street Code3' : int, 'Vehicle Expiration Date' : int, 'Violation Location' : str, 'Violation Precinct' : int,
    'Issuer Precinct' : int, 'Issuer Code' : int, 'Issuer Command' : str, 'Issuer Squad' : str, 'Violation Time' : str,
    'Time First Observed' : str, 'Violation County' : str, 'Violation In Front Of Or Opposite' : str, 'House Number' : str, 'Street Name' : str,
    'Intersecting Street' : str, 'Date First Observed' : int, 'Law Section' : int, 'Sub Division' : str, 'Violation Legal Code' : str,
    'Days Parking In Effect' : str, 'From Hours In Effect' : str, 'To Hours In Effect' : str, 'Vehicle Color' : str, 'Unregistered Vehicle?' : str,
    'Vehicle Year' : int, 'Meter Number' : str, 'Feet From Curb' : int, 'Violation Post Code' : str, 'Violation Description' : str,
    'No Standing or Stopping Violation' : str, 'Hydrant Violation' : str, 'Double Parking Violation' : str, 
    }

df = dd.read_csv('/d/hpc/projects/FRI/bigdata/data/NYTickets/2023_april.csv', blocksize='64MB',dtype=dtypes,
    parse_dates=['Issue Date'], low_memory=False
)

names = [
    'summons_number', 'plate_id', 'registration_state', 'plate_type', 'issue_date',
    'violation_code', 'vehicle_body_type', 'vehicle_make', 'issuing_agency', 'street_code1',
    'street_code2', 'street_code3', 'vehicle_expiration_date', 'violation_location', 'violation_precinct',
    'issuer_precinct', 'issuer_code', 'issuer_command', 'issuer_squad', 'violation_time',
    'time_first_observed', 'violation_county', 'violation_in_front_of_or_opposite', 'house_number', 'street_name',
    'intersecting_street', 'date_first_observed', 'law_section', 'sub_division', 'violation_legal_code',
    'days_parking_in_effect', 'from_hours_in_effect', 'to_hours_in_effect', 'vehicle_color', 'unregistered_vehicle',
    'vehicle_year', 'meter_number', 'feet_from_curb', 'violation_post_code', 'violation_description',
    'no_standing_or_stopping_violation', 'hydrant_violation', 'double_parking_violation', 
]

new_dict = {}

for col1, col2 in zip(dtypes, names):
    new_dict[col1] = col2

df = df.rename(columns=new_dict)
print(df.head())
dd.to_parquet(df, '/d/hpc/home/aj8977/Project/dataset.parquet', overwrite=True, engine='fastparquet')