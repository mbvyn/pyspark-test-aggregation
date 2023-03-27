import re


def get_feature_columns(df):
    """
    Returns a list of feature columns from a Spark DataFrame.

    :param df: A Spark DataFrame containing the feature columns.
    :return: A list of feature columns.
    """
    return [feature_col for feature_col in df.columns if feature_col.startswith('feature_type_')]


def get_feature_type(feature_col):
    """
    Returns the feature type from a given feature column.

    :param feature_col: A feature column.
    :return: A string representing the feature type.
    """
    return re.match(r'(\w+_\w+_\d+_)\d+', feature_col).group(1)
