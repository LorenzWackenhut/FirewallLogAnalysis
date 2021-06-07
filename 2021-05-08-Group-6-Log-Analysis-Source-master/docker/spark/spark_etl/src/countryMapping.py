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

class CountryMapping:
    """
    A class used to add country information to pfsense_log data

    Attributes
    ----------
    df : DataFrame
        parsed pfsense_log data

    Methods
    -------
    run()
        Returns pfsense_log data with added source_country_code column
    """

    def __init__(self, df):
        self.df = df

    def run(self):
        # filtering all ip addresses which do not appear more than 100 times over the course of 3 months
        # time approx. 2 hours
        distinctIps = (
            self.df.groupBy("source_ip")
            .count()
            .filter(col("count") >= 100)
            .sort(col("count").desc())
        )
        distinctIpsMeta = addIpMetaData(distinctIps)
        self.df = self.df.join(distinctIpsMeta, on=["source_ip"], how="left")
        return self.df.select(
            [
                "id",
                "timestamp",
                "rule_number",
                "tracker_id",
                "real_interface",
                "reason",
                "action",
                "traffic_direction",
                "ip_version",
                "tos",
                "ecn",
                "ttl",
                "ipv4_id",
                "offset",
                "flags",
                "protocoll_id",
                "protocoll_text",
                "length",
                "source_ip",
                "destination_ip",
                "source_port",
                "destination_port",
                "data_length",
                "tcp_flag",
                "sequence_num",
                "ack",
                "window",
                "options",
                "threat_type_bandwith",
                "threat_type_frequency",
                "source_country_code",
            ]
        )