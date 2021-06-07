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


def isEntryInDangerousIpList(value, ip_list):
    if value in ip_list:
        return 1
    return 0


def isEntryInDangerousIpListUDF(ip_list):
    return udf(lambda value: isEntryInDangerousIpList(value, ip_list))

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

        
class ThreatDectectionBandwith:
    """
    A class used to detect security threats based on bandwith in pfsense_log dataframe

    Attributes
    ----------
    df : DataFrame
        parsed pfsense_log data

    Methods
    -------
    detect()
        Returns pfsense_log data with added threat_type_bandwith column
    """

    def __init__(self, df):
        self.df = df

    def detect(self):
        df_grouped = self._groupBandwithByMinuteAndSourceIp()
        df_grouped.withColumn
        ip_list = self._getDangerousSourceIPs(df_grouped)
        return self._markDangerousLogEntries(ip_list)

    def _groupBandwithByMinuteAndSourceIp(self):
        return (
            self.df.groupBy("source_ip", window("timestamp", "1 minute"))
            .sum("data_length")
            .withColumnRenamed("sum(data_length)", "count")
        )

    def _getDangerousSourceIPs(self, df_grouped):
        # mean value for bandwith per minute is 98970.71 (Jupyter Notebook = describe_bandwith)
        return (
            df_grouped.select("source_ip")
            .where(col("count") >= 98971)
            .distinct()
            .rdd.map(lambda row: row[0])
            .collect()
        )

    def _markDangerousLogEntries(self, ip_list):
        # Add threat_type column to config
        return self.df.withColumn(
            "threat_type_bandwith",
            isEntryInDangerousIpListUDF(ip_list)(col("source_ip")),
        )