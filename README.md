# Standardization of Job Vacancy Features
## Overview
This project aims to standardize the features of job vacancies in the test dataset based on the statistics calculated from the training dataset.

The input data consists of two CSV files, test.csv and train.csv, with the same format, where each line represents a job vacancy and has 11 columns, including an ID and 10 feature types.

## Dataset
The `train.csv` dataset is used to calculate the mean and standard deviation of each feature type, 
which will be used to standardize the feature values in the `test.csv` dataset.

The `test.csv` dataset contains job vacancy data with features that need to be standardized.

## Data Description
The `train.csv` and `test.csv` dataset has 1000 rows and 11 columns:

- Dimension 1: `id_job` - **[int]** vacancy identifier
- Dimension 10: `features_type_1_{i}` - **[List[float]]** list of features of a specific type for a vacancy

## Requirements for Output Data
The output of this project is the csv file that contains the following set of columns (features) for each vacancy from `test.csv`:

- Dimension 1: `id_job` - **[int]** vacancy identifier
- Dimension 10: `features_type_1_stand_{i}` - **[List[float]]** The result of standardization (z-score normalization) of the input feature `feature_type_1_{i}`

To standardize the feature values, the mean and standard deviation of each feature type from the `train.csv` dataset are calculated. 

The `features_type_1_stand_{i}` values in the result dataset are computed using the formula:

```
(features_type_1_{i} - mean(features_type_1_{i})) / std(features_type_1_{i})
```
where `mean(features_type_1_{i})` is the average value of `feature_type_1_{i}` among all lines from `train.csv`,
and `std(features_type_1_{i})` is the standard deviation of `feature_type_1_{i}` among all lines from `train.csv`.

Additionally, `max_feature_type_1_index` is included in the result dataset, which is an index value of the maximum value of the feature `feature_type_1_{i}` for each job vacancy.

## Instructions
To run the project, you need to execute the `main.py` script, 
which will generate the output csv file. 

This file can be used to test the performance of models that use standardized feature values.

## References
[Feature Scaling - Standardization (Z-score Normalization)](https://en.wikipedia.org/wiki/Feature_scaling#Standardization_(Z-score_Normalization))