##############################################################################
#                          SFU Researching Computing Group                   #
#                                 Alexandre Lopes                            #
#                                  alopes@sfu.ca                             #
#                                    09/26/2018                              #
##############################################################################

import sys
import time
import argparse
import requests
import logging

from pyspark.sql import Row
from pyspark.sql import SparkSession
from logging import StreamHandler
from logging.handlers import TimedRotatingFileHandler

log = logging.getLogger('mediaCollectorsLogger')
log.setLevel(logging.INFO)

def parse_args():

    """Parse command line arguments."""
    parser = argparse.ArgumentParser(description="Execute stress tests in websites using spark cluster")

    parser.add_argument("url",
                        type=str,
                        help="Url to perform resquests")

    parser.add_argument("-m",
                        "--maps",
                        type=int,
                        default=100,
                        help="Number of maps")

    parser.add_argument("-c",
                        "--resquests-per-map",
                        type=int,
                        default=10,
                        help="Number of requests per executor")

    parser.add_argument("-rc",
                        "--response-compare",
                        type=str,
                        default="",
                        help="Response file to compare the responses")

    return parser.parse_args()


def log_setup():

    format = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
    rotateHandler = TimedRotatingFileHandler("logs/web-stress-test", when="midnight")
    rotateHandler.suffix += ".log"
    rotateHandler.setFormatter(format)

    log.addHandler(rotateHandler)
    log.addHandler(StreamHandler(sys.stdout))

    return rotateHandler


def fm_resquest(param):

    url, resquests_per_map = param
    results = []
    for i in range(resquests_per_map):
        start_time = time.time()
        response = requests.get(url)
        results.append(("time", time.time() - start_time))
        results.append(("response", response.text))

    return results


if __name__ == '__main__':

    args = parse_args()
    spark = SparkSession \
        .builder \
        .appName("Spark web stress test")\
        .getOrCreate()

    spark.sparkContext.setLogLevel("ERROR")

    rotateHandler = log_setup()

    try:
        log.info("Parameters: %s", args)

        expected = None
        if args.response_compare:
            expected = spark.sparkContext.parallelize([(args.url, 1)])\
                .flatMap(fm_resquest)\
                .filter(lambda row: row[0] == "response")\
                .take(1)[0][1]

        rddParams = spark.sparkContext.parallelize([(args.url, args.resquests_per_map)] * args.maps, args.maps)
        rddResults = rddParams.flatMap(fm_resquest)
        rddResults.cache()

        dfTime = rddResults.filter(lambda row: row[0] == 'time')\
                        .map(lambda row: Row(time=row[1]))\
                        .toDF()

        if expected:
            rddMatch = rddResults.filter(lambda row: row[0] == 'response')\
                .map(lambda row: True if row[1] == expected else False)

            log.info("Response file comparison Matches: %s Non matches: %s",
                     rddMatch.filter(lambda r: r).count(),
                     rddMatch.filter(lambda r: not r).count())

        dfTime.createOrReplaceTempView("times")
        spark.sql("SELECT count(time) as Count, avg(time) as Avg, min(time) as Min, max(time) as Max FROM times").show()
    finally:
        rotateHandler.doRollover()

























