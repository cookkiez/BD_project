import numpy as np
import pandas as pd

import tables as tb

df = pd.read_csv('dataset.csv', dtype={
    'summons_number' : np.int64, 'plate_id' : str, 'registration_state' : str, 'plate_type' : str, 'issue_date' : str,
    'violation_code' : np.int64, 'vehicle_body_type' : str, 'vehicle_make' : str, 'issuing_agency' : str, 'street_code1' : np.int64,
    'street_code2' : np.int64, 'street_code3' : np.int64, 'vehicle_expiration_date' : np.int64, 'violation_location' : str, 'violation_precinct' : np.int64,
    'issuer_precinct' : np.int64, 'issuer_code' : np.int64, 'issuer_command' : str, 'issuer_squad' : str, 'violation_time' : str,
    'time_first_observed' : str, 'violation_county' : str, 'violation_in_front_of_or_opposite' : str, 'house_number' : str, 'street_name' : str,
    'intersecting_street' : str, 'date_first_observed' : np.int64, 'law_section' : np.int64, 'sub_division' : str, 'violation_legal_code' : str,
    'days_parking_in_effect' : str, 'from_hours_in_effect' : str, 'to_hours_in_effect' : str, 'vehicle_color' : str, 'unregistered_vehicle' : str,
    'vehicle_year' : np.int64, 'meter_number' : str, 'feet_from_curb' : np.int64, 'violation_post_code' : str, 'violation_description' : str,
    'no_standing_or_stopping_violation' : str, 'hydrant_violation' : str, 'double_parking_violation' : str, 
    },
    # parse_dates=['issue_date']
)

# {
#         'summons_number' : 'int64', 'plate_id' : 'str', 'registration_state' : 'str', 'plate_type' : 'str', 'issue_date' : 'str',
#         'violation_code' : 'int64', 'vehicle_body_type' : 'str', 'vehicle_make' : 'str', 'issuing_agency' : 'str', 'street_code1' : 'int64',
#         'street_code2' : 'int64', 'street_code3' : 'int64', 'vehicle_expiration_date' : 'int64', 'violation_location' : 'str', 'violation_precinct' : 'int64',
#         'issuer_precinct' : 'int64', 'issuer_code' : 'int64', 'issuer_command' : 'str', 'issuer_squad' : 'str', 'violation_time' : 'str',
#         'time_first_observed' : 'str', 'violation_county' : 'str', 'violation_in_front_of_or_opposite' : 'str', 'house_number' : 'str', 'street_name' : 'str',
#         'intersecting_street' : 'str', 'date_first_observed' : 'int64', 'law_section' : 'int64', 'sub_division' : 'str', 'violation_legal_code' : 'str',
#         'days_parking_in_effect' : 'str', 'from_hours_in_effect' : 'str', 'to_hours_in_effect' : 'str', 'vehicle_color' : 'str', 'unregistered_vehicle' : 'str',
#         'vehicle_year' : 'int64', 'meter_number' : 'str', 'feet_from_curb' : 'int64', 'violation_post_code' : 'str', 'violation_description' : 'str',
#         'no_standing_or_stopping_violation' : 'str', 'hydrant_violation' : 'str', 'double_parking_violation' : 'str', 
#         }

# df = pd.read_csv('dataset.csv', dtype={
#     'summons_number' : str, 'plate_id' : str, 'registration_state' : str, 'plate_type' : str, 'issue_date' : str,
#     'violation_code' : str, 'vehicle_body_type' : str, 'vehicle_make' : str, 'issuing_agency' : str, 'street_code1' : str,
#     'street_code2' : str, 'street_code3' : str, 'vehicle_expiration_date' : str, 'violation_location' : str, 'violation_precinct' : str,
#     'issuer_precinct' : str, 'issuer_code' : str, 'issuer_command' : str, 'issuer_squad' : str, 'violation_time' : str,
#     'time_first_observed' : str, 'violation_county' : str, 'violation_in_front_of_or_opposite' : str, 'house_number' : str, 'street_name' : str,
#     'intersecting_street' : str, 'date_first_observed' : str, 'law_section' : str, 'sub_division' : str, 'violation_legal_code' : str,
#     'days_parking_in_effect' : str, 'from_hours_in_effect' : str, 'to_hours_in_effect' : str, 'vehicle_color' : str, 'unregistered_vehicle' : str,
#     'vehicle_year' : str, 'meter_number' : str, 'feet_from_curb' : str, 'violation_post_code' : str, 'violation_description' : str,
#     'no_standing_or_stopping_violation' : str, 'hydrant_violation' : str, 'double_parking_violation' : str, 
#     },
#     parse_dates=['issue_date']
# )

# print(df.dtypes)
# hdf = pd.HDFStore('dataset2.h5')
# hdf.append("test", df, data_columns=df.columns)
# hdf.close()

f_zlib  = tb.Filters(complib ="zlib", complevel=9)

types = { "object": 'S20', 'int64': np.int64 }

with tb.File('dataset_compressed.h5','w') as h5f:
    arr_dt = np.dtype([(c, types[str(df[c].dtype)]) for c in df.columns])
    numpy_arr = np.empty(shape=(len(df.index),), dtype=arr_dt)
    for c in df.columns:
        numpy_arr[c] = df[c]
    h5f.create_table('/','test', obj=numpy_arr, filters=f_zlib)