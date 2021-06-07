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
import requests


def addIpMetaData(distinctIps):
    addIpMetaDataUDF = udf(getCountryInformationFromIp)
    return distinctIps.withColumn(
        "source_country_code", addIpMetaDataUDF(col("source_ip"))
    )


def getCountryInformationFromIp(ip):
    try:
        response = requests.get("https://api.ip.sb/geoip/{}".format(ip), timeout=15)
        jsonResponse = response.json()
        # meta = {
        #     'location': '{},{}'.format(jsonResponse['latitude'] if 'latitude' in jsonResponse else None, jsonResponse['longitude'] if 'longitude' in jsonResponse else None),
        #     'organization': jsonResponse['organization'] if 'organization' in jsonResponse else None,
        #     'region': jsonResponse['region'] if 'region' in jsonResponse else None,
        #     'country': jsonResponse['country_code'] if 'country_code' in jsonResponse else None,
        #     'city': jsonResponse['city'] if 'city' in jsonResponse else None,
        # }
        meta = jsonResponse["country_code"] if "country_code" in jsonResponse else None
        print(meta)
        return meta
    except requests.Timeout:
        print("Timeout")
        return "Timeout"
    except requests.RequestException as e:
        print(e.__str__())
        return "404"

def isEntryInDangerousIpList(value, ip_list):
    if value in ip_list:
        return 1
    return 0


def isEntryInDangerousIpListUDF(ip_list):
    return udf(lambda value: isEntryInDangerousIpList(value, ip_list))


class ThreatDectectionLogFrequency:
    """
    A class used to detect security threats based on log frequency in pfsense_log dataframe

    Attributes
    ----------
    df : DataFrame
        parsed pfsense_log data

    Methods
    -------
    detect()
        Returns pfsense_log data with added threat_type_frequency column
    """

    def __init__(self, df):
        self.df = df

    def detect(self):
        df_grouped = self._groupLogEntriesByMinuteAndSourceIp()
        ip_list = self._getDangerousSourceIPs(df_grouped)
        return self._markDangerousLogEntries(ip_list)

    def _groupLogEntriesByMinuteAndSourceIp(self):
        return (
            self.df.groupBy("source_ip", window("timestamp", "1 minute"))
            .count()
            .withColumnRenamed("sum(source_ip)", "count")
        )

    def _getDangerousSourceIPs(self, df_grouped):
        # mean value for logs per minute is 117 (Jupyter Notebook = describe_time)
        return (
            df_grouped.select("source_ip")
            .where(col("count") >= 117)
            .distinct()
            .rdd.map(lambda row: row[0])
            .collect()
        )

    def _markDangerousLogEntries(self, ip_list):
        # Add threat_type column to config
        return self.df.withColumn(
            "threat_type_frequency",
            isEntryInDangerousIpListUDF(ip_list)(col("source_ip")),
        )


def addIpMetaData(distinctIps):
    addIpMetaDataUDF = udf(getCountryInformationFromIp)
    return distinctIps.withColumn(
        "source_country_code", addIpMetaDataUDF(col("source_ip"))
    )
