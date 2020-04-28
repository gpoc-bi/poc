#!/usr/bin/env python
# coding: utf-8


from pyspark.sql import SparkSession
from pyspark.context import SparkContext

from pyspark.sql.types import *
from pyspark.sql.functions import *
from pyspark.sql.window import *
import sys, time
import datetime as datetm
import json
from datetime import date


sc = SparkContext.getOrCreate()
GCP_BUCKET = "gs://device_unlock_inbound/"


log4jLogger = sc._jvm.org.apache.log4j
log = log4jLogger.LogManager.getLogger("RollingFile")
log.setLevel(log4jLogger.Level.INFO)

spark = SparkSession.builder.appName("Device Unlock OL").getOrCreate()

# Define the input file schema
inputSchema = StructType([StructField('ACCOUNT_NUM', DecimalType(precision=9), False),
                          StructField('CONTACT_EMAIL', StringType(), True),
                          StructField('FIRST_NAME', StringType(), True),
                          StructField('IMEI', StringType(), False),
                          StructField('MSISDN', StringType(), True),
                          StructField('IS_CHARGEABLE', StringType(), True),
                          StructField('IS_UNLOCKABLE_BY_CODE', StringType(), True),
                          StructField('IS_UNLOCK_CODE_AVAILABLE', StringType(), True),
                          StructField('IS_LOCK_STATUS_VERIFIABLE', StringType(), True),
                          StructField('IS_DEVICE_OUT_OF_MIN_CONTRACT', StringType(), True),
                          StructField('IS_CHARGE_REJECTED', StringType(), True),
                          StructField('ACCOUNT_TYPE', StringType(), True),
                          StructField('DATE_TIMESTAMP', TimestampType(), False),
                          StructField('REQUEST_STATUS', StringType(), False),
                          StructField('STATUS_REASON', StringType(), False)])


# Validate string column length
def validate_str_len(input_file, col_name, col_length):
    invalid_row_cnt = input_file.where(length(col_name) > col_length).count()
    print(invalid_row_cnt)
    if invalid_row_cnt > 0:
        log.error('Column length for column ' + col_name + ' in file ' + input_filename + ' is not as per IC')
        raise Exception('Column length for column ' + col_name + ' in file ' + input_filename + ' is not as per IC')


def process_device_unlocking_ol(input_filename, output_filename, curr_processed_date):
    # Read the input file
    input_intermediate = GCP_BUCKET + input_filename
   # input_file = sc.textFile("D:/" + input_filename)
    #input_file = spark.read.csv(input_intermediate, inferSchema=True, header=True)
    input_file = sc.textFile(input_intermediate)

    header = input_file.first()
    input_file = input_file.filter(lambda line: line != header).map(lambda line: line.split("|^"))
    trailer_count = input_file.filter(lambda col: len(col) == 1).collect()[0][0]
    input_file = input_file.filter(lambda col: len(col) > 1).toDF(inputSchema.fieldNames())

    input_file = input_file.withColumn("ACCOUNT_NUM", input_file['ACCOUNT_NUM'].cast(DecimalType(precision=9)))
    input_file = input_file.withColumn("DATE_TIMESTAMP", to_timestamp("DATE_TIMESTAMP", "yyyyMMdd HH:mm:ss"))

    # Validate record count with trailer details
    rec_count = input_file.count()

    if rec_count != int(trailer_count):
        log.error('Incomplete file!!!!! Record count does not match trailer details.')
        raise Exception('Incomplete file!!!!! Record count does not match trailer details.')

        # Validate not null columns
    null_count = input_file.where(
        col('ACCOUNT_NUM').isNull() | col('IMEI').isNull() | col('DATE_TIMESTAMP').isNull() | col(
            'REQUEST_STATUS').isNull() | col('STATUS_REASON').isNull()).count()
    if null_count > 0:
        log.error('File ' + input_filename + ' has null value for not null column')
        raise Exception('File ' + input_filename + ' has null value for not null column')

    # Validate string column length
    validate_str_len(input_file, 'CONTACT_EMAIL', 128)
    validate_str_len(input_file, 'FIRST_NAME', 100)
    validate_str_len(input_file, 'IMEI', 15)
    validate_str_len(input_file, 'MSISDN', 11)
    validate_str_len(input_file, 'IS_CHARGEABLE', 1)
    validate_str_len(input_file, 'IS_UNLOCKABLE_BY_CODE', 1)
    validate_str_len(input_file, 'IS_UNLOCK_CODE_AVAILABLE', 1)
    validate_str_len(input_file, 'IS_LOCK_STATUS_VERIFIABLE', 1)
    validate_str_len(input_file, 'IS_DEVICE_OUT_OF_MIN_CONTRACT', 1)
    validate_str_len(input_file, 'IS_CHARGE_REJECTED', 1)
    validate_str_len(input_file, 'ACCOUNT_TYPE', 3)
    validate_str_len(input_file, 'REQUEST_STATUS', 1)
    validate_str_len(input_file, 'STATUS_REASON', 255)

    # Write to output file
    output_file = input_file.select(['ACCOUNT_NUM', 'IMEI'] + [c for c in input_file.columns if c not in ['ACCOUNT_NUM','IMEI']]).withColumn('date_recorded', lit(curr_processed_date))
    print(output_file.show())
   #output_file.toPandas().to_csv(GCP_BUCKET + output_filename, index=False, header=False, sep=",", date_format="%Y%m%d %H:%M:%S")
    output_file.write.format('csv').option('emptyValue', 'null').save(GCP_BUCKET + output_filename)
    output_file.write.format('bigquery').option('table', 'OL_AMDOCS_PROD.T_DEVICE_UNLOCK_G').option("temporaryGcsBucket", "device_unlock_inbound").mode('append').save()


def main():
    log.info("Start of _main_ method in device_unlocking class");

    # Obtain the file date to be processed
    CONFIG_FILE = GCP_BUCKET + "g_last_processed_date_ol.txt/"
    CONFIG_FILE1 = GCP_BUCKET + "g_last_processed_date_ol.txt"
    curr_processed_date_file = spark.read.csv(CONFIG_FILE).select(
        from_unixtime(unix_timestamp('_c0', 'yyyyMMdd'), 'yyyyMMdd').alias('_c0'))
    last_processed_date = datetm.datetime.strptime((curr_processed_date_file.collect())[0][0], '%Y%m%d').date()
    print("Last processed file read from config file is " + str(last_processed_date))

    log.info("last processed date : " + str(last_processed_date));

    # last processed date is grt than today's date then skip the process
    if (last_processed_date < date.today()):
            iteration_no = date.today() - last_processed_date
            log.info("Backlog No : " + str(int(iteration_no.days) - 1));
            curr_processed_date = (last_processed_date + datetm.timedelta(days=(1))).strftime('%Y%m%d')
            input_filename = "DeviceUnlockReport_" + curr_processed_date + ".txt"
            output_filename = "device_unlock_" + curr_processed_date + ".dat"
            process_device_unlocking_ol(input_filename, output_filename, curr_processed_date)
            config_file = curr_processed_date_file.withColumn('_c0', lit(curr_processed_date)) 
            print(config_file.show())
            #config_file.write.csv(CONFIG_FILE,header=True,mode = 'overwrite')
            

    else:
            log.info("Last run date is same as current date")
            return


if __name__ == "__main__":
    try:
        main()
        log.info(" ol process has been completed successfully");
    except:
        e = sys.exc_info()[0]
        raise






