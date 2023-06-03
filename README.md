# Parking Tickets in New York City

This repository contains the code and analysis for a project on parking tickets in New York City. The project focuses on exploring and analyzing a dataset of parking tickets issued in the city, as well as augmenting the data with additional datasets and performing streaming analysis.

## Dataset

The main dataset used in this project is a collection of parking tickets issued in New York City. The dataset contains information such as the ticket number, license plate number, violation code, violation description, location, date, and more. The dataset was stored in both Parquet and HDF5 formats, with Parquet being the preferred format due to its efficiency.

In addition to the parking tickets dataset, two additional datasets were used for data augmentation. These datasets include information about museums and schools in New York City, which were merged with the parking tickets dataset to provide additional insights.

## Tasks

The project is divided into several tasks, each focusing on a different aspect of the data analysis process. The tasks include:

1. Storing the dataset: The parking tickets dataset was stored in both Parquet and HDF5 formats, with Parquet proving to be more efficient in terms of file size. The dataset was also partitioned into smaller partitions for easier processing.

2. Data augmentation: Additional datasets containing information about museums and schools in New York City were merged with the parking tickets dataset to provide additional context and insights. Geolocation data for street names in the parking tickets dataset was also collected using the Geosearch API.

3. Exploratory analysis: Various aspects of the parking tickets dataset were analyzed, including the states in which cars were registered, the most common violations, the most ticketed vehicles, and the most common streets for parking violations. Visualizations were created to showcase the findings.

4. Streaming analysis: A streaming analysis was performed on a subset of the parking tickets dataset. Rolling statistics, such as mean, median, standard deviation, minimum, maximum, and skewness, were calculated for both boroughs and streets to observe trends and patterns over time.

5. Other tasks: Additional tasks were performed, such as analyzing the costs associated with parking violations for the most ticketed vehicles and identifying the most popular schools and museums near issued tickets.

## Code

The code for the project is organized into several files:

- `to_parquet.py`: Stores the parking tickets dataset in Parquet format.
- `to_hdf5.py`: Stores the parking tickets dataset in HDF5 format.
- `data_analysis.py`: Performs exploratory analysis on the parking tickets dataset and generates visualizations.
- `streaming.py`: Conducts streaming analysis on a subset of the parking tickets dataset.
- `other_tasks.py`: Includes code for other tasks, such as analyzing costs and identifying popular schools and museums.

## Results

The analysis of the parking tickets dataset revealed interesting insights. The most common states in which cars were registered included New York, New Jersey, Pennsylvania, and even Florida. Several vehicles stood out for having an unusually high number of parking tickets, some accumulating costs close to the median yearly salary in the United States. The most common violations included school zone speed violations, no parking for street cleaning, and failure to display a municipal meter receipt. The analysis also identified popular streets for parking violations, with Broadway being the most common.

The streaming analysis provided rolling statistics for both boroughs and streets, allowing for the observation of trends and patterns over time. The statistics included mean, median, standard deviation, minimum, maximum, and

 skewness. By analyzing these rolling statistics, it was possible to identify areas with high variability in the number of parking tickets issued.

## Conclusion

The project successfully explored and analyzed a dataset of parking tickets in New York City. Through the use of data augmentation and various analysis techniques, interesting patterns and insights were uncovered. The results can be used to inform future policies and decision-making related to parking regulations and enforcement in the city.