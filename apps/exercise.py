from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType,FloatType,TimestampType,DateType,LongType	
from pyspark.sql.functions import concat_ws
from pyspark.sql.window import Window

import pyspark
from pyspark.sql.functions import *
import pyspark.sql.functions as f
    

def init_spark():
  sql = SparkSession.builder\
    .appName("ex-app")\
    .getOrCreate()
  sc = sql.sparkContext
  return sql,sc

def checkCase(item):
    if item == 'A' or item == 'B':
        return item
    else: 
        return None

check_case = udf(lambda z: checkCase(z), StringType())


def main():
    sql,sc = init_spark()
    sql_context = pyspark.SQLContext(sc)
    ## load ingested data
    sc._jsc.hadoopConfiguration().set("fs.s3a.access.key",'minio_access_key')
    sc._jsc.hadoopConfiguration().set("fs.s3a.secret.key",'minio_secret_key')
    sc._jsc.hadoopConfiguration().set('fs.s3a.path.style.access', 'true')
    sc._jsc.hadoopConfiguration().set('fs.s3a.impl', 'org.apache.hadoop.fs.s3a.S3AFileSystem')
    sc._jsc.hadoopConfiguration().set("fs.s3a.endpoint", 'http://minio:9000')
    sc._jsc.hadoopConfiguration().set("fs.s3a.path.style.access", "true")
    sc._jsc.hadoopConfiguration().set("fs.s3a.attempts.maximum", "1")
    sc._jsc.hadoopConfiguration().set("fs.s3a.connection.establish.timeout", "5000")
    sc._jsc.hadoopConfiguration().set("fs.s3a.connection.timeout", "5000")
    schema = StructType([
            StructField("user", StringType(), False),
            StructField("session_id", StringType(), False),
            StructField("timestamp", LongType(), True),
            StructField("page", StringType(), True),
        ])
    df1 = sql.read.option("header", "true").schema(schema).csv("s3a://data/dataset1.csv")
    df1=df1.withColumn("timestamp", to_timestamp(col("timestamp")))    
    df1=df1.withColumn("page", f.upper("page"))
    df1=df1.withColumn("session_id", f.upper("session_id"))
    df1=df1.withColumn("user", f.upper("user"))

    #drop na fields
    df1=df1.na.drop()
    #user, session are unique
    df1=df1.dropDuplicates(['user', 'session_id','timestamp'])

    # check if page is in format PAGE+number ( 1 to 10)
    split_col = pyspark.sql.functions.split(df1['page'], 'PAGE')
    df1 = df1.withColumn('page_num', split_col.getItem(1))
    df1=df1.withColumn(
      "page_num_value",
      f.col("page_num").cast("int").isNotNull()
    )
    df1=df1.filter(df1["page_num_value"] == True)
    df1=df1.filter(col("page_num").between(1, 10))
    df1=df1.drop("page_num_value")
    df1=df1.drop("page_num")

    df1 = df1.withColumnRenamed('timestamp', 'browse_timestamp')
    df1 = df1.withColumnRenamed('user', 'user_id')
    #check if valid columns should contain page+number,user,session start with specific letter
    df1=df1.filter(df1.page.rlike("^PAGE[0-9]"))
    df1=df1.filter(df1.user_id.rlike("^U[0-9]"))
    df1=df1.filter(df1.session_id.rlike("^S[0-9]"))
    


    schema = StructType([
            StructField("user", StringType(), False),
            StructField("session_id", StringType(), False),
            StructField("timestamp", LongType(), True),
            StructField("transaction", StringType(), True),
        ])

    df2 = sql.read.option("header", "true").schema(schema).csv("s3a://data/dataset2.csv")
    df2=df2.withColumn("timestamp", to_timestamp(col("timestamp")))    
    df2=df2.withColumn("transaction", f.upper("transaction"))
    df2=df2.withColumn("session_id", f.upper("session_id"))
    df2=df2.withColumn("user", f.upper("user"))

    df2=df2.na.drop()
    # a user can't have the same transaction in the same session
    df2=df2.dropDuplicates(['user', 'session_id','timestamp'])
    # A or B transcations are valid
    df2=df2.withColumn("transaction", check_case("transaction")).na.drop()


    df2 = df2.withColumnRenamed('timestamp', 'transaction_timestamp')
    df2 = df2.withColumnRenamed('user', 'user_id')

    #user,session start with specific letter
    df2=df2.filter(df2.user_id.rlike("^U[0-9]"))
    df2=df2.filter(df2.session_id.rlike("^S[0-9]"))
    # df2=df2.filter((col("transaction") == "A") | (col("transaction") == "B"))


    ## calculate time spend per session


    #store clean data in postgres as user_browsing
    df1.write.format("jdbc")\
    .option("url", "jdbc:postgresql://postgres:5432/test") \
    .mode("append").option("driver", "org.postgresql.Driver").option("dbtable", "user_browsing") \
    .option("user", "test").option("password", "test").save()

    # df1.toPandas().to_csv("/opt/spark-data/browsing.csv")




    # df2.toPandas().to_csv("/opt/spark-data/transcations_pre_cleared.csv")


    #join both dataframes to locate average time to purchase
    df3=df1.join(df2, (df1.user_id == df2.user_id) & (df1.session_id == df2.session_id), 'inner').\
      select(df1.user_id,df1.session_id,df1.browse_timestamp,df1.page,df2.transaction_timestamp,df2.transaction).orderBy(col('user_id'),col('session_id'))
    # df3.toPandas().to_csv("/opt/spark-data/user_total_journey.csv")
    df3.write.format("jdbc")\
    .mode("append").option("url", "jdbc:postgresql://postgres:5432/test") \
    .option("driver", "org.postgresql.Driver").option("dbtable", "user_total_journey") \
    .option("user", "test").option("password", "test").save()


    ## here we end up with with the of the user in the site. keeping the first row (earliest) in the group by will give us
    ## the first page the user visited and the last page the user visited with the purchase
    w = Window.partitionBy(["user_id","session_id"]).orderBy(asc('browse_timestamp'))
    df4=df3.withColumn('row',row_number().over(w)).filter(col('row') == 1).drop('row')

    df4.write.format("jdbc")\
    .mode("append").option("url", "jdbc:postgresql://postgres:5432/test") \
    .option("driver", "org.postgresql.Driver").option("dbtable", "user_journey_to_buy_visited") \
    .option("user", "test").option("password", "test").save()

    df4=df4.withColumn(
    "date_diff_min", 
    f.round((f.col("transaction_timestamp").cast("long") - f.col("browse_timestamp").cast("long"))/60.,2)
    )

    ## it's not possible to have negative time spend per session, same session_id but diff date
    # df4.toPandas().to_csv("/opt/spark-data/time_spend_per_session_all_diff.csv")

    

    #filter those dates from transactions 
    # how long is a trascantion kept ? Can we have a transcation longer than a day ? 
    df6=df4.filter(df4.date_diff_min > 0)
    df6=df6.filter(df4.date_diff_min < 1440)

    
    # df6.toPandas().to_csv("/opt/spark-data/time_spend_per_session_only_diff.csv")
    df6.write.format("jdbc")\
    .mode("append").option("url", "jdbc:postgresql://postgres:5432/test") \
    .option("driver", "org.postgresql.Driver").option("dbtable", "time_spend_per_ses") \
    .option("user", "test").option("password", "test").save()    


    # drop those transactions from transcations df and keep only valid transactions
    # store clean data in postgres as user_transactions

    df6=df6.select(df6.user_id,df6.session_id)
    df2=df2.join(df6, on=['user_id','session_id'], how='left_anti')
    # df2.toPandas().to_csv("/opt/spark-data/transactions_cleared.csv")
    df2.write.format("jdbc")\
    .mode("append").option("url", "jdbc:postgresql://postgres:5432/test") \
    .option("driver", "org.postgresql.Driver").option("dbtable", "user_transactions") \
    .option("user", "test").option("password", "test").save()



    # ## most transaction in pages..last page visited is where the transcaction happened
    w = Window.partitionBy(["user_id","session_id"]).orderBy(desc('browse_timestamp'))
    
    df6=df3.withColumn('row',row_number().over(w)).filter(col('row') == 1).drop('row')
    df6=df6.withColumn(
    "date_diff_last_page", 
    f.round((f.col("transaction_timestamp").cast("long") - f.col("browse_timestamp").cast("long"))/60.,2)
    )
    df6=df6.filter(df6.date_diff_last_page > 0)
    df6=df6.filter(df6.date_diff_last_page < 1440)
    df6.write.format("jdbc")\
    .mode("append").option("url", "jdbc:postgresql://postgres:5432/test") \
    .option("driver", "org.postgresql.Driver").option("dbtable", "end_transactions") \
    .option("user", "test").option("password", "test").save()
    # df6.toPandas().to_csv("/opt/spark-data/end_transactions.csv")

  
  
if __name__ == '__main__':
  main()

