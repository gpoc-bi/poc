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
GCP_BUCKET_OUT = "gs://device_unlock_outbound/"

process_name = "Device unlock EL"
pyspark_job_name ="Device_Unlock_EL_GCP.py"
type_of_load = "EL"
reject_records=0


process_stats_schema=StructType([StructField('process_name', types.StringType(), False),
                          StructField('pyspark_job_name', types.StringType(), True),
                          StructField('type_of_load', types.StringType(), True),
                          StructField('redords_read', types.IntegerType(), False),
                          StructField('records_loaded', types.IntegerType(), True),
                          StructField('records_rejected', types.IntegerType(), True),
                          StructField('start_time', types.TimestampType(), True),
                          StructField('end_time', types.TimestampType(), True),
                          StructField('source_file_name', types.StringType(), True),
                          StructField('target_table_name', types.StringType(), True)])

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
   
     
    #output = output.replace('null','U',subset=schemas.device_unlock_status_schema.fieldNames()[1:])
    output = output.na.fill('U',subset=schemas.device_unlock_status_schema.fieldNames()[1:])
   
    
    output = output.replace('','U',subset=schemas.device_unlock_status_schema.fieldNames()[1:])
    
    
    output = output.join(df_status,schemas.device_unlock_status_schema.fieldNames()[1:],'inner')
    
    
    output=output.withColumn("DATE_ID",output["DATE_TIMESTAMP"].cast(types.DateType()))
    output = output.withColumn("TIME_ID",F.date_format(output["DATE_TIMESTAMP"], "HH:mm:ss"))
    output = output.withColumn("COUNT",lit(1))

    output.select(columns)    
    
 
    return output.select(columns) 

def process_device_unlocking_el(ol_filename, device_unlock_config, fileDate):
    df = sc.textFile(ol_filename).map(lambda line: line.split(",")).toDF(schemas.device_unlock_schema.fieldNames())
   


    df = typeCast(df, schemas.device_unlock_schema)
    
    #modify to match bigquery schema
    df_status = spark.read.csv(GCP_BUCKET + "dim_device_unlock_status.csv",schema=schemas.device_unlock_status_schema)
    start_time=datetime.datetime.now()
    # Processing device_unlock_cust
    max_cust_surr_key = device_unlock_config["max_cust_id"]
    rt_val_cust = process_dimension_files(df, max_cust_surr_key, schemas.device_unlock_cust_schema.fieldNames())
    device_unlock_config["max_cust_id"] = rt_val_cust[1]
    

    
    rt_val_cust[0].write.format('bigquery') \
        .option('table', 'MIRPROD.T_DIM_DEVICE_UNLOCK_CUST_G') \
        .option("temporaryGcsBucket", "device_unlock_inbound") \
        .mode('append') \
        .save()
    print(" ")
    print("Loading data into MIRPROD.TDIM_DEVICE_UNLOCK_CUST_G has completed")
    end_time=datetime.datetime.now()
    pro_sts_cust=spark.createDataFrame([(process_name,pyspark_job_name,type_of_load,df.count(),rt_val_cust[0].count(),reject_records,start_time,end_time,ol_filename,'T_DIM_DEVICE_UNLOCK_CUST')], process_stats_schema)
   
    
    # Processing device_unlock_rsn
    start_time=datetime.datetime.now()
    max_rsn_surr_key = device_unlock_config["max_status_rsn_id"]
    rt_val_rsn = process_dimension_files(df, max_rsn_surr_key, schemas.device_unlock_rsn_schema.fieldNames())
    device_unlock_config["max_status_rsn_id"] = rt_val_rsn[1]

    
    
    rt_val_rsn[0].write.format('bigquery') \
        .option('table', 'MIRPROD.T_DIM_DEV_UNLCK_STATUS_RSN_G') \
        .option("temporaryGcsBucket", "device_unlock_inbound") \
        .mode('append') \
        .save()
    print(" ")
    print("Loading data into MIRPROD.T_DIM_DEV_UNLCK_STATUS_RSN_G has completed")    
    
    end_time=datetime.datetime.now()
    pro_sts_rsn=spark.createDataFrame([(process_name,pyspark_job_name,type_of_load,df.count(),rt_val_rsn[0].count(),reject_records,start_time,end_time,ol_filename,'T_DIM_DEV_UNLCK_STATUS_RSN')], process_stats_schema)
    process_stat=pro_sts_cust.union(pro_sts_rsn)

    # Processing fact_device_unlock
    
    fact_out = process_fact_device_unlock(df, rt_val_cust[0], rt_val_rsn[0],df_status,
                                          schemas.device_unlock_fact_schema.fieldNames())
    
    fact_out.write.format('bigquery') \
        .option('table', 'MIRPROD.T_FACT_DEVICE_UNLOCK_G') \
        .option("temporaryGcsBucket", "device_unlock_inbound") \
        .mode('append') \
        .save()
    print(" ")
    print("Loading data into MIRPROD.T_FACT_DEVICE_UNLOCK_G has completed")   
    end_time=datetime.datetime.now()
    pro_sts_fact=spark.createDataFrame([(process_name,pyspark_job_name,type_of_load,df.count(),fact_out.count(),reject_records,start_time,end_time,ol_filename,'T_FACT_DEVICE_UNLOCK')], process_stats_schema)

    process_stat=process_stat.union(pro_sts_fact)
    process_stat.write.format('bigquery') \
        .option('table', 'COLLECT_STATISTICS_PROD.PROCESS_RUN_STATISTICS') \
        .option("temporaryGcsBucket", "device_unlock_inbound") \
        .mode('append') \
        .save()
    print(" ")
    print("Loading data into COLLECT_STATISTICS_PROD.PROCESS_RUN_STATISTICS has completed") 
    

    return device_unlock_config


def main():
    log.info("Start of _main_ method in device_unlocking class");

    try:
     
      date_today=date.today()
      date_tb_processed = (date_today - dt.timedelta(days=(1))).strftime('%Y%m%d')
      
      table_rsn = spark.read.format("bigquery").option("table","MIRPROD.T_DIM_DEV_UNLCK_STATUS_RSN_G").load().cache()
      table_rsn.createOrReplaceTempView("rsn")
      rsn_id = spark.sql('select max(UNLOCK_STATUS_RSN_ID) as max_unlock  from rsn').first()["max_unlock"]
      table_cust = spark.read.format("bigquery").option("table","MIRPROD.T_DIM_DEVICE_UNLOCK_CUST_G").load().cache()
      table_cust.createOrReplaceTempView("cust")
      cust_id = spark.sql('select max(DEV_UNLCK_CUST_ID) as max_cus from cust').first()["max_cus"]

      
      device_unlock_config = {"last_processed": date_tb_processed, "max_status_rsn_id": rsn_id, "max_cust_id": cust_id}
     
    except:
        e = sys.exc_info()[0]
        raise

    # last processed date
    last_processed_date = datetime.datetime.strptime(date_tb_processed, '%Y%m%d').date()

    if (last_processed_date < date.today()):
        iteration_no = date.today() - last_processed_date

        for i in range(0, iteration_no.days):
            fileDate = (last_processed_date + dt.timedelta(days=(1 + i))).strftime('%Y%m%d')
            ol_filename = GCP_BUCKET_OUT + "device_unlock_" + fileDate + ".dat/"

            # process ol file
            print("Starting El process - dimension build and fact build")
            device_unlock_config_updated = process_device_unlocking_el(ol_filename, device_unlock_config, fileDate)



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







