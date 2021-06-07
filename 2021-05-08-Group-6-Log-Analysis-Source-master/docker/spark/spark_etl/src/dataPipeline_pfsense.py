from .countryMapping import CountryMapping
from .extract import Extract
from .load import Load
from threatDectectionBandwith import ThreatDectectionBandwith
from threatDectectionLogFrequency import ThreatDectectionLogFrequency
from transform import Transform
from transformv4 import TransformV4

from os import path
import yaml
from pyspark.sql import SparkSession

spark = (
    SparkSession.builder.master("pfsense")
    .config("spark.master", "local")
    .config("spark.driver.memory", "8g")
    .config("spark.executor.memory", "8g")
    .config("spark.jars", "/job/spark_etl/postgresql-42.2.18.jar",)
    .getOrCreate()
)

logger = spark._jvm.org.apache.log4j
logger.LogManager.getLogger("org").setLevel(logger.Level.ERROR)
logger.LogManager.getLogger("akka").setLevel(logger.Level.ERROR)

def main():
    def build_path(name_file):
        path_script = path.dirname(path.abspath(__file__))
        return path.join(path_script, name_file)

    def load_config(path_config):
        path_config_full = build_path(path_config)
        with open(path_config_full) as f:
            config = yaml.load(f, Loader=yaml.FullLoader)
        return config

    def filter_ipVersion(df, version):
        df_ipVersion = df.filter(df["ip_version"] == version).drop("ip_version")
        return df_ipVersion

    def divide_df(df, index):
        df1 = df.select(df.columns[:index])
        cols = df.columns[index + 1 :]
        cols.insert(0, "id")
        # cols = list(set(cols))
        # print(cols)
        df2 = df.select(cols)
        return df1, df2

    config = load_config("config.yml")
    pfSenseLog = Extract(build_path(config["source_path"]), config["sample_size"])
    df = pfSenseLog.extract()
    df_v4 = filter_ipVersion(df, 4)
    df_v6 = filter_ipVersion(df, 6)

    transformV4 = TransformV4(df_v4, config["schema_v4"], config["final_cols_v4"])
    transformV6 = Transform(df_v6, config["schema_v6"], config["final_cols_v6"])
    df_v4 = transformV4.transform()
    df_v6 = transformV6.transform()

    threat_bandwith = ThreatDectectionBandwith(df_v4)
    df_v4 = threat_bandwith.detect()
    threat_frequency = ThreatDectectionLogFrequency(df_v4)
    df_v4 = threat_frequency.detect()
    map_country = CountryMapping(df_v4)
    df_v4 = map_country.run()

    df_v4_log, df_v4_ip = divide_df(df_v4, 9)
    df_v6_log, df_v6_ip = divide_df(df_v6, 9)
    df_log = df_v4_log.union(df_v6_log).orderBy("timestamp")

    if config["write_mode"] == "database":
        load_v4 = Load(
            df_v4_ip,
            "ip4",
            write_mode="database",
            db_url=config["db_url"],
            db_properties=config["db_properties"],
        )
        load_v6 = Load(
            df_v6_ip,
            "ip6",
            write_mode="database",
            db_url=config["db_url"],
            db_properties=config["db_properties"],
        )
        load_log = Load(
            df_log,
            "pfsense_log",
            write_mode="database",
            db_url=config["db_url"],
            db_properties=config["db_properties"],
        )

    else:
        load_v4 = Load(
            df_v4_ip, build_path(config["dest_path_v4"]), write_mode="parquet"
        )
        load_v6 = Load(
            df_v6_ip, build_path(config["dest_path_v6"]), write_mode="parquet"
        )
        load_log = Load(
            df_log, build_path(config["dest_path_log"]), write_mode="parquet"
        )

    load_v4.load()
    load_v6.load()
    load_log.load()


if __name__ == "__main__":
    main()