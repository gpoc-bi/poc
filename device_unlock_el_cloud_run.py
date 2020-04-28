#!/usr/bin/env python
# coding: utf-8

from pyspark.sql import SparkSession
from pyspark.context import SparkContext

from datetime import date
from pyspark.sql.functions import monotonically_increasing_id
from pyspark.sql import types
from pyspark.sql.types import StructType
from pyspark.sql.types import StructField
from datetime import date
from pyspark.sql import SQLContext
from pyspark import sql
from pyspark import SparkContext
from pyspark.sql.functions import lit
from pyspark.sql.functions import to_timestamp
import datetime
import time
import json
import datetime as dt
import pyspark.sql.functions as F
import sys,time

sc = SparkContext.getOrCreate()
sqlContext = sql.SQLContext(sc)

spark = SparkSession.builder.appName("Device Unlock OL").getOrCreate()


log4jLogger = sc._jvm.org.apache.log4j
log = log4jLogger.LogManager.getLogger("RollingFile")
log.setLevel(log4jLogger.Level.INFO)

GCP_BUCKET = "gs://device_unlock_inbound/"

class schemas:
    # Schema definations for all the files

    device_unlock_schema = StructType([StructField('ACCOUNT_NUM', types.IntegerType(), False),
                                       StructField('IMEI', types.StringType(), False),
                                       StructField('CONTACT_EMAIL', types.StringType(), True),
                                       StructField('FIRST_NAME', types.StringType(), True),                                      
                                       StructField('MSISDN', types.StringType(), True),
                                       StructField('IS_CHARGEABLE', types.StringType(), True),
                                       StructField('IS_UNLOCKABLE_BY_CODE', types.StringType(), True),
                                       StructField('IS_UNLOCK_CODE_AVAILABLE', types.StringType(), True),
                                       StructField('IS_LOCK_STATUS_VERIFIABLE', types.StringType(), True),
                                       StructField('IS_DEVICE_OUT_OF_MIN_CONTRACT', types.StringType(), True),
                                       StructField('IS_CHARGE_REJECTED', types.StringType(), True),
                                       StructField('ACCOUNT_TYPE', types.StringType(), True),
                                       StructField('DATE_TIMESTAMP', types.TimestampType(), True),
                                       StructField('REQUEST_STATUS', types.StringType(), True),
                                       StructField('STATUS_REASON', types.StringType(), True),
                                       StructField('DATE_RECORDED', types.DateType(), True)])

    device_unlock_cust_schema = StructType([StructField('DEV_UNLCK_CUST_ID', types.IntegerType(), False),
                                            StructField('ACCOUNT_NUM', types.IntegerType(), True),
                                            StructField('IMEI', types.StringType(), False),
                                            StructField('MSISDN', types.StringType(), True),
                                            StructField('FIRST_NAME', types.StringType(), True),
                                            StructField('CONTACT_EMAIL', types.StringType(), True),
                                            StructField('DATE_RECORDED', types.DateType(), True)])

    device_unlock_rsn_schema = StructType([StructField('UNLOCK_STATUS_RSN_ID', types.IntegerType(), False),
                                           StructField('REQUEST_STATUS', types.StringType(), True),
                                           StructField('STATUS_REASON', types.StringType(), False),
                                           StructField('ACCOUNT_NUM', types.IntegerType(), True),
                                           StructField('IMEI', types.StringType(), True),
                                           StructField('DATE_TIMESTAMP', types.TimestampType(), True),
                                           StructField('DATE_RECORDED', types.DateType(), True)])

    device_unlock_fact_schema = StructType([StructField('DEV_UNLCK_CUST_ID', types.IntegerType(), False),
                                            StructField('UNLOCK_STATUS_ID', types.IntegerType(), False),
                                            StructField('ACCOUNT_TYPE', types.StringType(), True),
                                            StructField('UNLOCK_STATUS_RSN_ID', types.IntegerType(), True),
                                            StructField('DATE_ID', types.StringType(), True),
                                            StructField('TIME_ID', types.StringType(), False),
                                            StructField('COUNT', types.IntegerType(), True),
                                            StructField('DATE_RECORDED', types.DateType(), True)])
                                            
                                            
                                            
    device_unlock_status_schema = StructType([StructField('UNLOCK_STATUS_ID', types.IntegerType(), False),
                                            StructField('IS_CHARGEABLE', types.StringType(), True),
                                            StructField('IS_UNLOCKABLE_BY_CODE', types.StringType(), True),
                                            StructField('IS_UNLOCK_CODE_AVAILABLE', types.StringType(), True),
                                            StructField('IS_LOCK_STATUS_VERIFIABLE', types.StringType(), True),
                                            StructField('IS_DEVICE_OUT_OF_MIN_CONTRACT', types.StringType(), True),
                                            StructField('IS_CHARGE_REJECTED', types.StringType(), True)])


# generic typecasting method
#def typeCast(dataframe, schemas):
#    dfschema = dict(dataframe.dtypes)
#    for name in schemas.fieldNames():
#        if (dfschema[name] != schemas[name].dataType.typeName()):
#            if (schemas[name].dataType.typeName() == "timestamp"):
#                dataframe = dataframe.withColumn(name, to_timestamp(name, "yyyy-MM-dd HH:mm:ss"))
#            elif (schemas[name].dataType.typeName() == "date"):
#                dataframe = dataframe.withColumn(name, F.to_date(dataframe[name], "yyyyMMdd"))
#            else:
#                dataframe = dataframe.withColumn(name, dataframe[name].cast(schemas[name].dataType.typeName()))
#    return dataframe

def typeCast(dataframe,schemas):
    dfschema = dict(dataframe.dtypes)
    for name in schemas.fieldNames():
        if(dfschema[name] != schemas[name].dataType.typeName()):
            if(schemas[name].dataType.typeName()=="timestamp"):
                dataframe = dataframe.withColumn(name,to_timestamp(name, "yyyy-MM-dd'T'HH:mm:ss.SSSXXX"))      
            elif(schemas[name].dataType.typeName()=="date"):
                dataframe = dataframe.withColumn(name,F.to_date(dataframe[name], "yyyyMMdd"))
            else:
                dataframe = dataframe.withColumn(name,dataframe[name].cast(schemas[name].dataType.typeName()))
    return dataframe
    
    
# generic dimension process method
def process_dimension_files(df, surrogate_seed, columns):
    df_with_part = df.repartition(1).withColumn(columns[0], monotonically_increasing_id() + surrogate_seed + 1)
    selected_colums = df_with_part.select(columns)
    #  selected_colums.write.csv(filename,mode='overwrite',sep='|',header=True)
    return selected_colums, df_with_part.agg({columns[0]: "max"}).collect()[0][0]


#
def process_fact_device_unlock(df, df_cust, df_rsn,df_status, columns):
    # processing fact 
    output =df.join(df_cust.select(['ACCOUNT_NUM','DEV_UNLCK_CUST_ID']),['ACCOUNT_NUM'],'inner')\
     .join(df_rsn.select(['ACCOUNT_NUM','UNLOCK_STATUS_RSN_ID','IMEI','DATE_TIMESTAMP']),['ACCOUNT_NUM','IMEI','DATE_TIMESTAMP'],'inner')
    
    print("after first join")    
    print(output.show())
     
    #output = output.replace('null','U',subset=schemas.device_unlock_status_schema.fieldNames()[1:])
    output = output.na.fill('U',subset=schemas.device_unlock_status_schema.fieldNames()[1:])
    print("after first fill ")    
    print(output.show())
    
    output = output.replace('','U',subset=schemas.device_unlock_status_schema.fieldNames()[1:])
    
    print("after second fill ")    
    print(output.show())
    
    output = output.join(df_status,schemas.device_unlock_status_schema.fieldNames()[1:],'inner')
    print("after join with seeed table ")    
    print(output.show())
    
    output=output.withColumn("DATE_ID",output["DATE_TIMESTAMP"].cast(types.DateType()))
    output = output.withColumn("TIME_ID",F.date_format(output["DATE_TIMESTAMP"], "HH:mm:ss"))
    output = output.withColumn("COUNT",lit(1))

    output.select(columns)    
    
    print(output.show())
    #writing into file
    #output.select(columns).write.csv(filename,mode='overwrite',sep='|',header=True)
    return output.select(columns) 

def process_device_unlocking_el(ol_filename, device_unlock_config, fileDate):
    df = sc.textFile(ol_filename).map(lambda line: line.split(",")).toDF(schemas.device_unlock_schema.fieldNames())
   

    print("ol data before typecast")
    print(df.show())
    # type casting to match with schema structure
    df = typeCast(df, schemas.device_unlock_schema)
    
    print("ol data after typecast")
    print(df.show())

    #device_unlock_cust_filename = "D:/dim_device_unlock_cust_" + str(fileDate) + ".dat"
    #dev_unlck_status_rsn_filename = "D:/dim_dev_unlck_status_rsn_" + str(fileDate) + ".dat"
    #fact_device_unlock_filename = "D:/fact_device_unlock_" + str(fileDate) + ".dat"
    
    #modify to match bigquery schema
    df_status = spark.read.csv(GCP_BUCKET + "dim_device_unlock_status.csv",schema=schemas.device_unlock_status_schema)

    # Processing device_unlock_cust
    max_cust_surr_key = device_unlock_config["max_cust_id"]
    rt_val_cust = process_dimension_files(df, max_cust_surr_key, schemas.device_unlock_cust_schema.fieldNames())
    device_unlock_config["max_cust_id"] = rt_val_cust[1]

    print("TDIM_DVEICE_UNLOCK_CUST")
    print(rt_val_cust[0].show())
    rt_val_cust[0].write.format('bigquery') \
        .option('table', 'MIRPROD.T_DIM_DEVICE_UNLOCK_CUST_G') \
        .option("temporaryGcsBucket", "device_unlock_inbound") \
        .mode('append') \
        .save()

    # Processing device_unlock_rsn
    max_rsn_surr_key = device_unlock_config["max_status_rsn_id"]
    rt_val_rsn = process_dimension_files(df, max_rsn_surr_key, schemas.device_unlock_rsn_schema.fieldNames())
    device_unlock_config["max_status_rsn_id"] = rt_val_rsn[1]

    print("T_DIM_DEV_UNLCK_STATUS_RSN")
    print(rt_val_rsn[0].show())
    rt_val_rsn[0].write.format('bigquery') \
        .option('table', 'MIRPROD.T_DIM_DEV_UNLCK_STATUS_RSN_G') \
        .option("temporaryGcsBucket", "device_unlock_inbound") \
        .mode('append') \
        .save()

    # Processing fact_device_unlock
    fact_out = process_fact_device_unlock(df, rt_val_cust[0], rt_val_rsn[0],df_status,
                                          schemas.device_unlock_fact_schema.fieldNames())
    print("T_FACT_DEVICE_UNLOCK")
    print(fact_out.show())
    fact_out.write.format('bigquery') \
        .option('table', 'MIRPROD.T_FACT_DEVICE_UNLOCK_G') \
        .option("temporaryGcsBucket", "device_unlock_inbound") \
        .mode('append') \
        .save()

    return device_unlock_config


def main():
    log.info("Start of _main_ method in device_unlocking class");

    try:
        # Reading config json file
      #  with open(GCP_BUCKET + 'device_unlock_config.json') as f:
      device_unlock_config = {"last_processed": "20200427", "max_status_rsn_id": 1, "max_cust_id": 1}
      print("hello")
    except:
        e = sys.exc_info()[0]
       # log.error(e)
        raise

    # last processed date
    last_processed_date = datetime.datetime.strptime('20200427', '%Y%m%d').date()
   #  last_processed_date = '20200414'
    log.info("last processed date : " + str(last_processed_date));

    # last processed date is grt than today's date then skip the process
    if (last_processed_date < date.today()):
        iteration_no = date.today() - last_processed_date

        log.info("Backlog No : " + str(int(iteration_no.days) - 1));

        # iterating through the files
        for i in range(0, iteration_no.days):
            fileDate = (last_processed_date + dt.timedelta(days=(1 + i))).strftime('%Y%m%d')
            ol_filename = GCP_BUCKET + "device_unlock_" + fileDate + ".dat/"

            # process ol file
            device_unlock_config_updated = process_device_unlocking_el(ol_filename, device_unlock_config, fileDate)

            # update & write to file last processed value
           # device_unlock_config_updated["last_processed"] = fileDate

          #  with open(GCP_BUCKET + 'device_unlock_config.json', 'w') as outfile:
           #     json.dump(device_unlock_config_updated, outfile)
          #  outfile.close()


    else:
        log.info("Last run date is same as current date")
        return


if __name__ == "__main__":
    try:
        main()
        log.info(" El process has been completed successfully");
    except:
        e = sys.exc_info()[0]
        raise







