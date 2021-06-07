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

class Transform:
    """
    A class used to transform extracted ip4 log files

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
        runs all methods to transform the dataframe
    """

    def __init__(self, df, col_schema, final_cols):
        self.df = df
        self.col_schema = col_schema
        self.final_cols = final_cols

    def transform(self):
        self._split_df()
        self._split_timestamp()
        self._transform_blanks()
        # self._drop_null_columns()
        self._apply_dtypes()
        self._reorder_df()
        return self.df

    def _split_df(self):
        split_df = split(self.df["value"], ",")
        for i, col in enumerate(self.col_schema):
            self.df = self.df.withColumn(col, split_df.getItem(i))

    def _split_timestamp(self):
        split_timestamp = split(
            self.df["timestamp_rule"], " gw01.extranet.frachtwerk.de filterlog: "
        )
        self.df = (
            self.df.withColumn("timestamp", split_timestamp.getItem(0))
            .withColumn("rule_number", split_timestamp.getItem(1))
            .withColumn("timestamp", regexp_replace("timestamp", "  ", " "))
        )

        self.df = self.df.drop("timestamp_rule", "value")

    def _transform_blanks(self):
        # https://stackoverflow.com/questions/33287886/replace-empty-strings-with-none-null-values-in-dataframe/39008555#39008555
        def blank_as_null(column):
            return when(col(column) != "", col(column)).otherwise(None)

        to_convert = set(self.df.columns)
        to_convert.remove("id")
        list_null = [
            blank_as_null(x).alias(x) if x in to_convert else x for x in self.df.columns
        ]
        self.df = self.df.select(*list_null)

    def _drop_null_columns(self):
        rows_with_data = (
            self.df.select(*self.df.columns)
            .groupby()
            .agg(*[max(c).alias(c) for c in self.df.columns])
            .take(1)[0]
        )
        cols_to_drop = [
            c for c, const in rows_with_data.asDict().items() if const == None
        ]
        self.df = self.df.drop(*cols_to_drop)

    def _apply_dtypes(self):
        for col in self.df.columns:
            if col == "timestamp":
                # pass
                # https://www.programiz.com/python-programming/datetime/
                try:
                    timestamp_udf = udf(
                        lambda x: datetime.strptime(x, "%b %d %H:%M:%S").replace(
                            year=2020
                        ),
                        TimestampType(),
                    )
                    self.df = self.df.withColumn(col, timestamp_udf(self.df[col]))
                except Exception as e:
                    print(e)
            else:
                try:
                    self.df = self.df.withColumn(
                        col, self.df[col].cast(self.col_schema[col])
                    )
                except Exception as e:
                    print(e)

    def _reorder_df(self):
        self.df = self.df.select(self.final_cols)