import pandas as pd
import numpy as np
import pyarrow as pa
import fastparquet as fp

df = pd.read_csv('dataset.csv', dtype={
    'summons_number' : int, 'plate_id' : str, 'registration_state' : str, 'plate_type' : str, 'issue_date' : str,
    'violation_code' : str, 'vehicle_body_type' : str, 'vehicle_make' : str, 'issuing_agency' : str, 'street_code1' : int,
    'street_code2' : int, 'street_code3' : int, 'vehicle_expiration_date' : int, 'violation_location' : str, 'violation_precinct' : int,
    'issuer_precinct' : int, 'issuer_code' : int, 'issuer_command' : str, 'issuer_squad' : str, 'violation_time' : str,
    'time_first_observed' : str, 'violation_county' : str, 'violation_in_front_of_or_opposite' : str, 'house_number' : str, 'street_name' : str,
    'intersecting_street' : str, 'date_first_observed' : int, 'law_section' : int, 'sub_division' : str, 'violation_legal_code' : str,
    'days_parking_in_effect' : str, 'from_hours_in_effect' : str, 'to_hours_in_effect' : str, 'vehicle_color' : str, 'unregistered_vehicle' : str,
    'vehicle_year' : int, 'meter_number' : str, 'feet_from_curb' : int, 'violation_post_code' : str, 'violation_description' : str,
    'no_standing_or_stopping_violation' : str, 'hydrant_violation' : str, 'double_parking_violation' : str, 
    },
    parse_dates=['issue_date']
)

# df = pd.read_csv('dataset.csv',dtype={
#     'Summons Number' : int, 'Plate ID' : str, 'Registration State' : str, 'Plate Type' : str, 'Issue Date' : str,
#     'Violation Code' : str, 'Vehicle Body Type' : str, 'Vehicle Make' : str, 'Issuing Agency' : str, 'Street Code1' : int,
#     'Street Code2' : int, 'Street Code3' : int, 'Vehicle Expiration Date' : int, 'Violation Location' : str, 'Violation Precinct' : int,
#     'Issuer Precinct' : int, 'Issuer Code' : int, 'Issuer Command' : str, 'Issuer Squad' : str, 'Violation Time' : str,
#     'Time First Observed' : str, 'Violation County' : str, 'Violation In Front Of Or Opposite' : str, 'House Number' : str, 'Street Name' : str,
#     'Intersecting Street' : str, 'Date First Observed' : int, 'Law Section' : int, 'Sub Division' : str, 'Violation Legal Code' : str,
#     'Days Parking In Effect' : str, 'From Hours In Effect' : str, 'To Hours In Effect' : str, 'Vehicle Color' : str, 'Unregistered Vehicle' : str,
#     'Vehicle Year' : int, 'Meter Number' : str, 'Feet From Curb' : int, 'Violation Post Code' : str, 'Violation Description' : str,
#     'No Standing or Stopping Violation' : str, 'Hydrant Violation' : str, 'Double Parking Violation' : str, 
#     },
#     parse_dates=['Issue Date']
# )

parquet_file = 'dataset.parquet'
fp.write(parquet_file, df, compression='GZIP')