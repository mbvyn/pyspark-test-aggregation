import pyspark

from spark.spark import Spark
from utils.data_aggregation import compute_means_stdevs, apply_standardization, add_max_feature_index
from utils.data_preprocessing import get_feature_columns, get_feature_type

# Initiate Spark instance and create DataFrames
spark = Spark('feature_aggregation')

options = {
    "inferSchema": True,
    "header": True,
}

test_ds = spark.create_dataframe('resources/test.csv', options=options).cache()
train_ds = spark.create_dataframe('resources/train.csv', options=options)

# Preprocess feature columns
feature_cols = spark.broadcast_value(get_feature_columns(test_ds)).value
first_feature_col = feature_cols[0]

# Create columns name for result dataframe
feature_type = get_feature_type(first_feature_col)
stand_col_name = feature_type + 'stand_'
max_index_col_name = 'max_' + feature_type + 'index'

# Aggregate dataframes
means_stdevs = spark.broadcast_value(compute_means_stdevs(train_ds, feature_cols)).value
z_standardization_df = apply_standardization(test_ds, feature_cols, stand_col_name, means_stdevs)
max_feature_df = add_max_feature_index(z_standardization_df, feature_cols, max_index_col_name).cache()

# Prepare and write result dataframe
result_df = max_feature_df.drop(*feature_cols)
spark.write_dataframe(result_df, 'test_transformed', options=options)
