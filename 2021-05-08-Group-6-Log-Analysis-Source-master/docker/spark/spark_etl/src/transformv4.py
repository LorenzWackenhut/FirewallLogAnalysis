from .transform import Transform

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


class TransformV4(Transform):
    """
    A class used to transform extracted ip6 log files

    Attributes
    ----------
    df : dataframe
        ip4 data in tabular format

    col_schema : dictionairy
        used to cast column tipes

    final_cols : list
        list of columns in right order to be selected

    Methods
    -------
    transform()
        returns dataframe
        runs all methods to transform the dataframe; calls parent class
    """

    def __init__(self, df, col_schema, final_cols):
        super().__init__(df, col_schema, final_cols)

    def transform(self):
        super()._split_df()
        super()._split_timestamp()
        # super()._transform_blanks()
        # super()._drop_null_columns()
        self._string_to_array()
        super()._apply_dtypes()
        super()._reorder_df()
        return self.df

    def _string_to_array(self):
        str_to_arr_udf = udf(
            lambda x: x.split(";") if x is not None else None, ArrayType(StringType())
        )
        self.df = self.df.withColumn("options", str_to_arr_udf(self.df["options"]))


def isEntryInDangerousIpList(value, ip_list):
    if value in ip_list:
        return 1
    return 0


def isEntryInDangerousIpListUDF(ip_list):
    return udf(lambda value: isEntryInDangerousIpList(value, ip_list))