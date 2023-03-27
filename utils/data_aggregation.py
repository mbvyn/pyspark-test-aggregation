from pyspark.sql.types import FloatType
from pyspark.sql.functions import col, mean, stddev, udf, lit, expr, array, array_max


def compute_means_stdevs(df, feature_cols):
    """
    Compute the mean and standard deviation of the specified feature columns.

    :param df: A Spark DataFrame containing the feature columns.
    :param feature_cols: A list of the feature columns.
    :return: A dictionary with keys "mean_feature_col_name" and "stdev_feature_col_name",
             where feature_col_name is the name of a feature column and
             the corresponding value is the mean and standard deviation of that
             feature column.
    """
    means_stdevs = df.select([mean(col(c)).alias("mean_" + c) for c in feature_cols] +
                             [stddev(col(c)).alias("stdev_" + c) for c in feature_cols])
    return means_stdevs.collect()[0]


def standardize_udf(feature, mean, stdev):
    """
    Standardize a feature value using its mean and standard deviation.

    :param feature: The value of the feature.
    :param mean: The mean of the feature.
    :param stdev: The standard deviation of the feature.
    :return: The standardized value of the feature.
    """
    if stdev == 0:
        return 0.0
    else:
        return (feature - mean) / stdev


def apply_standardization(df, feature_cols, column_name, means_stdevs):
    """
    Apply z-score standardization to the specified feature columns in a Spark DataFrame.

    :param df: A Spark DataFrame containing the feature columns to be standardized.
    :param feature_cols: A list of the feature columns to be standardized.
    :param column_name: The name of the column to be added to the DataFrame to store the standardized values.
    :param means_stdevs: A dictionary with keys "mean_feature_col_name" and "stdev_feature_col_name", where
                         feature_col_name is the name of a feature column and the corresponding value is the mean and
                         standard deviation of that feature column.
    :return: A Spark DataFrame with a new column containing the standardized values of the specified feature columns.
    """
    z_standardize_udf = udf(lambda feature, mean, stdev: standardize_udf(feature, mean, stdev), FloatType())

    for i, col_name in enumerate(feature_cols):
        df = df.withColumn(column_name + str(i),
                           z_standardize_udf(col(col_name),
                                             lit(means_stdevs["mean_" + col_name]),
                                             lit(means_stdevs["stdev_" + col_name]))
                           )
    return df


def add_max_feature_index(df, feature_cols, column_name):
    """
    Add a column to a Spark DataFrame with the index of the maximum feature value among the specified columns.

    :param df: A Spark DataFrame containing the feature columns.
    :param feature_cols: A list of the feature columns.
    :param column_name: The name of the column to be added to the DataFrame to store the maximum feature value index.
    :return: A Spark DataFrame with a new column containing the index of the maximum feature value index among the
             specified columns.
    """
    max_feature = array_max(array(*feature_cols))
    first_feature_index = df.columns.index(feature_cols[0])

    max_feature_df = df.withColumn(column_name, max_feature)

    max_feature_index = expr('array_position(array({}), {})'.format(','.join(feature_cols), column_name))

    df_with_max_feature_and_index = max_feature_df.withColumn(column_name,
                                                              max_feature_index + first_feature_index - 1)
    return df_with_max_feature_and_index
