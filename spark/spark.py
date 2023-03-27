from pyspark.sql import SparkSession


class Spark:
    def __init__(self, app_name):
        """
        Initialize the Spark object and create the SparkSession.
        """
        self.__partition_number = 1
        self.spark = SparkSession.builder.master('local[*]').appName(app_name).getOrCreate()

    def create_dataframe(self, input_path, file_format='csv', options=None):
        """
        Read the input file into a Spark DataFrame.

        :param input_path: The path to the input file.
        :param file_format: The format of the input file (default: 'csv').
        :param options: A dictionary of options to pass to the Spark reader (optional).
        :return: A Spark DataFrame containing the input data.
        """
        if options is None:
            options = {}
        reader = self.spark.read.format(file_format).options(**options)

        return reader.load(input_path)

    def write_dataframe(self, df, output_path, file_format='csv', options=None):
        """
        Write the output file from a Spark DataFrame.

        :param df: A Spark DataFrame containing the output data.
        :param output_path: The path to the output file.
        :param file_format: The format of the output file (default: 'csv').
        :param options: A dictionary of options to pass to the Spark writer (optional).
        """
        if options is None:
            options = {}
        writer = df.coalesce(self.__partition_number).write.format(file_format).options(**options)
        writer.save(output_path)

    def broadcast_value(self, value):
        """
         Broadcast the given value to all Spark workers.

         :param value: The value to be broadcasted.
         :return: A broadcast variable containing the value.
         """
        return self.spark.sparkContext.broadcast(value)
