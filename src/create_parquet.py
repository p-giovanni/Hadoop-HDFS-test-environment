#
# File: create_parquet.py
# Author(s): Ing. Giovanni Rizzardi - Summer 2017
# Project: Sky - table_crawler
#
# ----------------------------------------
# Utility script: creates a parquet file from a given
# csv files.
# The parquet file is named as the csv file but for the
# csv extension.
# Parameters:
#   1. csv file - from local file system. Must have
#      the columns name as first row and the field
#      separator is the comma;
#   2. output directory in hdfs file system. Must
#      already exists;
# ----------------------------------------

import os
import sys
import time
import datetime
import logging
from logging.handlers import RotatingFileHandler

import findspark

from pyspark.sql.types import *
from pyspark.sql.types import Row
from pyspark.sql.functions import *

import argparse, configparser
#from snakebite.client import AutoConfigClient

# ----------------------------------------
# init_logger
# ----------------------------------------
def init_logger(log_dir, log_level, std_out_log_level=logging.ERROR):
    """
    Logger initializzation for file logging and stdout logging with
    different level.

    :param log_dir: path for the logfile;
    :param log_level: logging level for the file logger;
    :param std_out_log_level: logging level for the stdout logger;
    :return:
    """
    root = logging.getLogger()
    dap_format = '%(asctime)s %(name)s %(levelname)s %(message)s'
    formatter = logging.Formatter(dap_format)
    # File logger.
    root.setLevel(logging.DEBUG)
    fh = RotatingFileHandler(os.path.join(log_dir, "hadoop-test-table-creation.log"), maxBytes=1000000, backupCount=5)
    fh.setLevel(log_level)
    fh.setFormatter(formatter)
    root.addHandler(fh)

    # Stdout logger.
    ch = logging.StreamHandler(sys.stdout)
    ch.setLevel(std_out_log_level)
    ch.setFormatter(formatter)
    root.addHandler(ch)

    for _ in ("urllib3", "matplotlib"):
        logging.getLogger(_).setLevel(logging.ERROR)

# ----------------------------------------
# run_column_change
# ----------------------------------------
def run_column_change(source_hdfs_directory, destination_hdfs_directory):
    """

    :param source_hdfs_directory:
    :param destination_hdfs_directory:
    :return:
    """
    rv = 1
    sql_table = "original_table"

    try:
        spark, sqlContext, spark_context = start_spark_session()

        # ------------------------------------------
        # Create a temp SQL table.
        # ------------------------------------------
        df_orig = spark.read.format('parquet').load(source_hdfs_directory)
        df_orig.createOrReplaceTempView(sql_table)
        df_orig.show(20, False)

        # ------------------------------------------------
        # Execute a SQL command that changes all the rows.
        # ------------------------------------------------
        sql_cmd = "select ingestionTimestamp * 1000 as ingestionTimestamp_x, * from {table}".format(table=sql_table)
        print(sql_cmd)
        df_updated_col = spark.sql(sql_cmd)

        # ------------------------------------------------------------
        # Move the new column on the old one and drop the temp column.
        # ------------------------------------------------------------l
        df_new_provider = df_updated_col.withColumn("ingestionTimestamp", col("ingestionTimestamp_x")).drop(col("ingestionTimestamp_x"))

        # ------------------------------------------------------------
        # Write the new dataframe into the destination path.
        # ------------------------------------------------------------
        df_new_provider.write.mode("overwrite").parquet(destination_hdfs_directory)
        df_new_provider.show(20, False)
        rv = 0

    except Exception as ex:
        print("ERROR - exception caught: {0}".format(str(ex)))

    print("run_column_change ({0})<<".format(str(rv)))
    return rv

# ----------------------------------------
# set_spark_environment
# ----------------------------------------
def set_spark_environment():
    # os.environ['HADOOP_HOME'] = '/home/add-on/hadoop'
    os.environ['HADOOP_CONF_DIR'] = '/home/giovanni/code-sky/dockers/hadoop/etc/hadoop'
    os.environ['SPARK_HOME'] = "/home/add-on/spark"

# ----------------------------------------
# start_spark_session
# ----------------------------------------
def start_spark_session():
    """
    Initiates the Spark session and returns it to the
    caller.
    :return:
    """
    log = logging.getLogger('start_spark_session')
    log.info("start_spark_session >>")

    findspark.init(os.environ['SPARK_HOME'])
    import pyspark
    from pyspark.sql import SparkSession, SQLContext

    try:
        conf = pyspark.SparkConf().setAppName("create_parquet")
        spark_context = pyspark.SparkContext(conf=conf)
        spark = SparkSession.builder.getOrCreate()
        sqlContext = SQLContext(spark_context)
    except Exception as ex:
        log.fatal("SESSION CREATION - Unexpected exception: {0}".format(str(ex)))
        sys.exit(1)

    log.info("start_spark_session <<")
    return spark, sqlContext, spark_context

# ----------------------------------------
# validate_in_file
# ----------------------------------------
def validate_in_file(in_file):
    """
    Check the existence  of the input file name and
    returns the file name without the directory component
    (if any).
    The file extension is changed to .parquet
    :param in_file:
    :param no_extension:
    :return:
    """
    log = logging.getLogger('validate_in_file')
    log.info("validate_in_file ({0}) >>".format(in_file))
    fi_name = None
    try:
        if not os.path.isfile(in_file):
            msg = "The csv input file doesn't exist (" + in_file + ")"
            log.fatal(msg)
            sys.exit(1)
        fi = os.path.basename(in_file)
        fi_name = os.path.splitext(fi)[0] + ".parquet"
    except Exception as ex:
        log.error("validate_in_file - Exception - {e}".format(e=ex))

    log.info("validate_in_file ({0}) <<".format(fi_name))
    return fi_name

# ----------------------------------------
# verify_out_dir
# ----------------------------------------
#def verify_out_dir(out_dir):
#    """
#
#    :param out_dir:
#    :return:
#    """
#    log = logging.getLogger('verify_out_dir')
#    log.info("verify_out_dir ({0}) >>".format(out_dir))
#    rv = False
#    try:
#        cli = AutoConfigClient()
#        stat = cli.stat([out_dir])
#        if stat['file_type'] != 'd':
#            log.error("The out parameterd must be a directory.")
#        else:
#            rv = True
#
#    except Exception as ex:
#        log.fatal(" - Unexpected exception: {0}".format(str(ex)))
#        sys.exit(1)
#    log.info("verify_out_dir ({0}) >>".format(rv))
#    return rv

# ----------------------------------------
# create_table_with_schema
# ----------------------------------------
def create_table_with_schema(hdfs_host):
    """
    Crea un dataframe con una struttura data e lo salva su
    HDFS locale.

    :param hdfs_host:
    :return:
    """
    log = logging.getLogger('create_table_with_schema')
    log.info("create_table_with_schema >>")
    rv = 1
    spark = None
    try:
        spark, sqlContext, spark_context = start_spark_session()

        schema = StructType([
            StructField("orderId",            StringType(), True),
            StructField("originalOrderId",    StringType(), True),
            StructField("activityType",       StringType(), True),
            StructField("householdId",        StringType(), False),
            StructField("originatingSystem",  StringType(), True),
            StructField("amount",             StringType(), True),
            StructField("proposition",        StringType(), True),
            StructField("requestId",          StringType(), True),
            StructField("provider",           StringType(), True),
            StructField("activityTimestamp",  LongType(),   True),
            StructField("providerTerritory",  StringType(), True),
            StructField("ingestionTimestamp", LongType(),   False),
            StructField("datetime",           StringType(),   True),
        ])
        rows = [
            Row("001", "OR001", "ACT", "H001", "TEST", "3.4", "NOWTV", "RID001", "NOWTV", 1560861072001, "ES", 1560861071, "2019-06-18"),
            Row("002", "OR002", "ACT", "H002", "TEST", "3.4", "NOWTV", "RID001", "NOWTV", 1560861072002, "ES", 1560861072, "2019-06-18"),
            Row("003", "OR003", "ACT", "H003", "TEST", "3.4", "NOWTV", "RID001", "NOWTV", 1560861072003, "ES", 1560861073, "2019-06-18"),
            Row("004", "OR004", "ACT", "H004", "TEST", "3.4", "NOWTV", "RID001", "NOWTV", 1560861072004, "ES", 1560861074, "2019-06-18"),
            Row("005", "OR005", "ACT", "H005", "TEST", "3.4", "NOWTV", "RID001", "NOWTV", 1560861072005, "ES", 1560861075, "2019-06-18"),
            Row("006", "OR006", "ACT", "H006", "TEST", "3.4", "NOWTV", "RID001", "NOWTV", 1560861072006, "ES", 1560861076, "2019-06-18"),
            Row("007", "OR007", "ACT", "H007", "TEST", "3.4", "NOWTV", "RID001", "NOWTV", 1560861072007, "ES", 1560861077, "2019-06-18"),
            Row("008", "OR008", "ACT", "H008", "TEST", "3.4", "NOWTV", "RID001", "NOWTV", 1560861072008, "ES", 1560861078, "2019-06-18"),
            Row("009", "OR009", "ACT", "H009", "TEST", "3.4", "NOWTV", "RID001", "NOWTV", 1560861072009, "ES", 1560861079, "2019-06-18"),
            Row("010", "OR010", "ACT", "H010", "TEST", "3.4", "NOWTV", "RID001", "NOWTV", 1560861072010, "ES", 1560861080, "2019-06-18")
        ]
        rdd = spark_context.parallelize(rows)

        df = sqlContext.createDataFrame(rdd, schema)
        df.write.parquet("{hh}/raw_ita/refound_success".format(hh=hdfs_host), mode='overwrite')
        rv = 0

    except Exception as ex:
        msg = "Unexpected exception: {0}".format(str(ex))
        log.fatal(msg)
        print(msg)
    finally:
        if spark is not None:
            spark.stop()

    log.info("create_table_with_schema ({rv}) <<".format(rv=rv))
    return rv

# ----------------------------------------
# create_dlq_table_with_schema
# ----------------------------------------
def create_dlq_table_with_schema(hdfs_host):
    """
    Crea una tabella simile a quella della DLQ per i test
    integrati sul monitoraggio degli eventi scartati.

    :param hdfs_host:
    :return:
    """
    log = logging.getLogger('create_dlq_table_with_schema')
    log.info("create_dlq_table_with_schema >>")
    rv = 1
    spark = None
    try:
        now = int(time.time()*1000.0)
        yesterday = now - (24*60*60*1000)
        two_days_ago = now - 2*(24*60*60*1000)

        s_now = datetime.datetime.fromtimestamp(now/1000)
        s_yesterday = datetime.datetime.fromtimestamp(yesterday/1000)
        s_two_days_ago = datetime.datetime.fromtimestamp(two_days_ago/1000)

        spark, sqlContext, spark_context = start_spark_session()

        schema = StructType([
            StructField("ingestionTimestamp", LongType(),            False),
            StructField("message",            StringType(),          False),
            StructField("sourceName",         StringType(),          False),
            StructField("territory",          StringType(),          False),
            StructField("errors",             ArrayType(StringType()), False),
            StructField("ingestionDatetime",  DateType(),            False)
        ])

        rows = [
            Row(now          ,"dlq message 00" ,"today" ,"ES", ["E_00" ,"SE_00"] ,s_now         ),
            Row(now          ,"dlq message 01" ,"today" ,"ES", ["E_01" ,"SE_01"] ,s_now         ),
            Row(now          ,"dlq message 02" ,"today" ,"ES", ["E_02" ,"SE_02"] ,s_now         ),
            Row(yesterday    ,"dlq message 03" ,"yesterday" ,"ES", ["E_03" ,"SE_03"] ,s_yesterday   ),
            Row(yesterday    ,"dlq message 04" ,"yesterday" ,"ES", ["E_04" ,"SE_04"] ,s_yesterday   ),
            Row(yesterday    ,"dlq message 05" ,"yesterday" ,"ES", ["E_05" ,"SE_05"] ,s_yesterday   ),
            Row(yesterday    ,"dlq message 06" ,"yesterday" ,"ES", ["E_06" ,"SE_06"] ,s_yesterday   ),
            Row(yesterday    ,"dlq message 07" ,"yesterday" ,"ES", ["E_07" ,"SE_07"] ,s_yesterday   ),
            Row(yesterday    ,"dlq message 08" ,"yesterday" ,"ES", ["E_08" ,"SE_08"] ,s_yesterday   ),
            Row(two_days_ago ,"dlq message 19" ,"source" ,"ES", ["E_19" ,"SE_19"] ,s_two_days_ago),
            Row(two_days_ago ,"dlq message 10" ,"source" ,"ES", ["E_it " ,"SE_10"] ,s_two_days_ago),
            Row(two_days_ago ,"dlq message 11" ,"source" ,"ES", ["E_11" ,"SE_11"] ,s_two_days_ago),
            Row(two_days_ago ,"dlq message 12" ,"source" ,"ES", ["E_12" ,"SE_12"] ,s_two_days_ago)
        ]
        rdd = spark_context.parallelize(rows)

        df = sqlContext.createDataFrame(rdd, schema).coalesce(1)
        df.write.parquet("{hh}/dlq_ita/es/datasource_xxx/dlq_example".format(hh=hdfs_host), mode='overwrite')
        df.show(20, False)

        rows = [
            Row(now          ,"dlq message 00" ,"source" ,"AT", ["E_00" ,"SE_00"] ,s_now         ),
            Row(now          ,"dlq message 01" ,"source" ,"AT", ["E_01" ,"SE_01"] ,s_now         ),
            Row(now          ,"dlq message 02" ,"source" ,"AT", ["E_02" ,"SE_02"] ,s_now         ),
            Row(now          ,"dlq message 03" ,"source" ,"AT", ["E_03" ,"SE_03"] ,s_yesterday   ),
            Row(yesterday    ,"dlq message 08" ,"source" ,"AT", ["E_08" ,"SE_08"] ,s_yesterday   ),
            Row(two_days_ago ,"dlq message 11" ,"source" ,"AT", ["E_11" ,"SE_11"] ,s_two_days_ago),
            Row(two_days_ago ,"dlq message 12" ,"source" ,"AT", ["E_12" ,"SE_12"] ,s_two_days_ago)
        ]
        rdd = spark_context.parallelize(rows)
        
        df = sqlContext.createDataFrame(rdd, schema).coalesce(1)
        df.write.parquet("{hh}/dlq_ita/at/datasource_xxx/dlq_example".format(hh=hdfs_host), mode='overwrite')
        df.show(20, False)

        rv = 0

    except Exception as ex:
        msg = "Unexpected exception: {0}".format(str(ex))
        log.fatal(msg)
        print(msg)
    finally:
        if spark is not None:
            spark.stop()

    log.info("create_dlq_table_with_schema ({rv}) <<".format(rv=rv))
    return rv

# ----------------------------------------
# create_table_from_csv
# ----------------------------------------
def create_table_from_csv(csv_in, hdfs_dir, no_extension=False):
    log = logging.getLogger('create_table_from_csv')
    log.info("create_table_from_csv >>")
    rv = 1
    spark, sqlContext, spark_context = None, None, None
    try:
        # Verify in and out parameters.
        file_name = validate_in_file(csv_in, no_extension)
        spark, sqlContext, spark_context = start_spark_session()

        csv_file = spark.read.csv('file://' + csv_in, header=True, mode="DROPMALFORMED")

        hdfs_file = hdfs_dir + "/" + file_name
        log.debug("Write / overwrite : {0}".format(hdfs_file))
        csv_file.write.parquet(hdfs_file, mode='overwrite')

        # TODO: the following line for partitioned parquet. Add new main params.
        # csv_file.write.partitionBy('table').parquet(hdfs_file, mode='overwrite')
        rv = 0

    except Exception as ex:
        msg = "Unexpected exception: {0}".format(str(ex))
        log.fatal(msg)
        print(msg)
    finally:
        if spark is not None:
            spark.stop()
    log.info("create_table_from_csv <<")
    return rv

# ----------------------------------------
# create_table_from_csv_with_schema
# ----------------------------------------
def create_table_from_csv_with_schema(schema, csv_in, hdfs_dir, hdfs_table_name):
    """

    :param schema:
    :param csv_in:
    :param hdfs_dir:
    :param hdfs_table_name:
    :return:
    """
    log = logging.getLogger('create_table_from_csv')
    log.info("create_table_from_csv_with_schema >>")
    rv = 1
    spark, sqlContext, spark_context = None, None, None
    try:
        file_name = validate_in_file(csv_in)
        spark, sqlContext, spark_context = start_spark_session()
        csv_file = spark.read.csv('file://' + csv_in, header=True, mode="DROPMALFORMED", schema=schema)

        hdfs_file = hdfs_dir + "/" + hdfs_table_name
        csv_file.write.parquet(hdfs_file, mode='overwrite')
        rv = 0

        df = spark.read.format("parquet").load(hdfs_file)
        df.show(20, False)

    except Exception as ex:
        msg = "Unexpected exception: {0}".format(str(ex))
        log.fatal(msg)
        print(msg)
    finally:
        if spark is not None:
            spark.stop()
    log.info("create_table_from_csv_with_schema <<")
    return rv

# ----------------------------------------
# read_table
# ----------------------------------------
def read_table(table_path):
    log = logging.getLogger('create_table_from_csv')
    log.info("read_table >>")
    log.debug("HDFS path: {p}".format(p=table_path))
    rv = 1
    spark, sqlContext, spark_context = None, None, None
    try:
        spark, sqlContext, spark_context = start_spark_session()
        df = spark.read.format("parquet").load(table_path)
        df.printSchema()
        df.show(600, False)

    except Exception as ex:
        msg = "Unexpected exception: {0}".format(str(ex))
        log.fatal(msg)
        print(msg)
    finally:
        if spark is not None:
            spark.stop()
    log.info("read_table <<")
    return rv

# ----------------------------------------
# soc
# ----------------------------------------
def soc(csv_path,hdfs_host):
    """

    :param csv_path:
    :param hdfs_host:
    :return:
    """
    log = logging.getLogger('create_table_from_csv')
    log.info("soc >>")
    log.debug("Data path: {p}".format(p=csv_path))

    table_name = "soc"
    hdfs_dir = "/test/soc/compare/input/provider=NOWTV/proposition=NOWTV/activityType=STREAM_STOP"

    rv = 1
    spark, sqlContext, spark_context = None, None, None
    try:
        spark, sqlContext, spark_context = start_spark_session()

        abs_path = os.path.abspath(csv_path)
        df = spark.read.csv('file://{s}'.format(s=abs_path), header=True, mode="DROPMALFORMED")

        #col_names = csv_file.schema.names
        #fields = [StructField(field_name, StringType(), True) for field_name in col_names]
        #fields[13].dataType = LongType()
        #schema = StructType(fields)

        df = df.withColumn("streamPosition", df.streamPosition.cast(LongType()))

        hdfs_file = "{hh}{d}/{t}".format(hh=hdfs_host,d=hdfs_dir,t=table_name)
        log.debug("HDFS path = {p}".format(p=hdfs_file))

        df.write.parquet(hdfs_file, mode='overwrite')
        rv = 0

        df = spark.read.format("parquet").load(hdfs_file)
        df.show(20, False)
        df.printSchema()


    #| -- activityTimestamp: string(nullable=true)
    #| -- applicationId: string(nullable=true)
    #| -- deviceId: string(nullable=true)
    #| -- devicePool: string(nullable=true)
    #| -- generatedId: string(nullable=true)
    #| -- geoIP: struct(nullable=true)
    #| | -- ipAddress: string(nullable=true)
    #| -- householdId: string(nullable=true)
    #| -- ipAddress: string(nullable=true)
    #| -- outcome: string(nullable=true)
    #| -- personaId: string(nullable=true)
    #| -- providerTerritory: string(nullable=true)
    #| -- providerVariantId: string(nullable=true)
    #| -- serviceKey: string(nullable=true)
    #| -- streamPosition: long(nullable=true)
    #| -- streamingTicket: string(nullable=true)
    #| -- subscriptionType: string(nullable=true)
    #| -- userId: string(nullable=true)
    #| -- userType: string(nullable=true)
    #| -- videoId: string(nullable=true)


    except Exception as ex:
        msg = "Unexpected exception: {0}".format(str(ex))
        log.fatal(msg)
        print(msg)
    finally:
        if spark is not None:
            spark.stop()
    log.info("soc <<")
    return rv

# ----------------------------------------
# main
# ----------------------------------------
def main(option):
    log = logging.getLogger('main')
    log.info("main >>")
    set_spark_environment()

    rv = 1

    hdfs_host = option.hdfs_host
    log.info("HDFS host: {hh}".format(hh=hdfs_host))

    if option.tables_choices == "integration-daily-check":
        rv = create_table_from_csv('/home/giovanni/code-sky/dockers/hadoop/test-scripts/test-data-01.csv',
                                   '/raw_ita', True)
        if rv == 0:
            rv = create_table_from_csv('/home/giovanni/code-sky/dockers/hadoop/test-scripts/test-data-02.csv',
                                       '/raw_ita', True)
        if rv == 0:
            schema = StructType([
                StructField("orderId",            StringType(), True),
                StructField("ingestionTimestamp", LongType(),   False),
                StructField("date",               DateType(),   True),
            ])
            rv = create_table_from_csv_with_schema(schema,
                                                   "/home/giovanni/code-sky/dockers/hadoop/test-scripts/test-data-03.csv",
                                                   "/raw_ita/",
                                                   "date_type_column_test")

        if rv != 0:
            log.error("Table creation in error.")

    elif option.soc:
        rv = soc(option.soc[0], hdfs_host=hdfs_host)
    elif option.tables_choices == "dlq-daily-checks":
        rv = create_dlq_table_with_schema(hdfs_host)
    elif option.tables_choices == "ingestion-timestamp-change":
        rv = create_table_with_schema(hdfs_host)
        if rv == 0:
            rv = run_column_change("{hh}/raw_ita/refound_success".format(hh=hdfs_host)
                                  ,"{hh}/raw_ita/refound_success_tmp".format(hh=hdfs_host))
    elif option.tables_choices == "table-column-in-seconds":
        schema = StructType([
            StructField("orderId", StringType(), True),
            StructField("ingestionTimestampInSeconds", LongType(), False),
            StructField("productName", StringType(), True),
        ])
        rv = create_table_from_csv_with_schema(schema=schema,
                                               csv_in="/home/giovanni/code-sky/dockers/hadoop/test-scripts/test-data-04.csv",
                                               hdfs_dir="{hh}/raw_ita/".format(hh=hdfs_host),
                                               hdfs_table_name="table_with_column_in_seconds")
    elif option.read:
        log.info("Path to read: ${table}".format(table=option.read[0]))
        read_table("/".join([hdfs_host, option.read[0]]))

    log.info("main <<")
    return rv

# ----------------------------------------
#
# ----------------------------------------
if __name__ == '__main__':
    print("Start ...")
        
    init_logger("/tmp", logging.INFO, logging.INFO)
    parser = argparse.ArgumentParser(description="Hadoop test table creation.")

    tables_choices = ['integration-daily-check', "dlq-daily-checks", "table-column-in-seconds", "ingestion-timestamp-change"]
    parser.add_argument("--soc"
                        ,"-soc"
                        , nargs=1
                        , help="File to the SOC csv data file.")

    parser.add_argument("--tables_choices"
                        ,"-tc"
                        , choices=tables_choices
                        , type=str.lower
                        , help="Chose among the various test table to create.")
    parser.add_argument("--hdfs_host", "-hh"
                        , default="hdfs://0.0.0.0:8020"
                        , type=str.lower
                        , help="Give the HDFS host:port to be used.")
    parser.add_argument("--read", "-r", nargs=1,
                        help="Reads and shows the table in the given path.")

    args = parser.parse_args()

    rv = main(args)

    print("Done!")
    sys.exit(rv)
    
