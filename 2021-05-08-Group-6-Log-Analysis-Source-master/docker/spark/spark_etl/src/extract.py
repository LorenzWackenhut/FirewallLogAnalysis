from pyspark.sql.types import (
    ArrayType,
    StructField,
    StructType,
    StringType,
    IntegerType,
    BooleanType,
    TimestampType,
    ArrayType,
)
from pyspark.sql.functions import (
    col,
    when,
    count,
    lit,
    to_timestamp,
    regexp_replace,
    concat,
    max,
    monotonically_increasing_id,
    window,
    split,
    udf,
)
from datetime import datetime
from os import path

class Extract:
    """
    A class used to extract raw log files

    Attributes
    ----------
    path_file : string
        path to the log file

    sample_size : int
        number of lines to process

    Methods
    -------
    extract()
        returns dataframe
        runs all methods to extract the data

    """

    def __init__(self, path_file, sample_size=None):
        self.path_file = path_file
        self.sample_size = sample_size

    def extract(self):
        self._read_file()
        self._filter_logs()
        self._add_id()
        self._add_ipCol()
        return self.df

    def _read_file(self):
        if self.sample_size is not None:
            self.df = spark.read.text(self.path_file).limit(self.sample_size)
        else:
            self.df = spark.read.text(self.path_file)

    def _filter_logs(self):
        self.df = self.df.filter(
            col("value").rlike(r"^.*gw01.extranet.frachtwerk.de filterlog.*$")
        )

    def _add_id(self):
        """Creates an unique hash that functions as an id"""
        self.df = self.df.withColumn("id", monotonically_increasing_id())

    def _add_ipCol(self):
        self.df = self.df.withColumn(
            "ip_version", split(self.df["value"], ",").getItem(8)
        )
