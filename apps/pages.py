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

    #drop na
    df1=df1.na.drop()
    #user, session are unique
    df1.dropDuplicates(['user', 'session_id','timestamp'])

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

    #check if valid columns should contain page+number,user,session start with specific letter
    df1=df1.filter(df1.page.rlike("^PAGE[0-9]"))
    df1=df1.filter(df1.user.rlike("^U[0-9]"))
    df1=df1.filter(df1.session_id.rlike("^S[0-9]"))
    df1.toPandas().to_csv("/opt/spark-data/mycsv1.csv")
    
    df1.write.format("jdbc")\
    .option("url", "jdbc:postgresql://postgres:5432/test") \
    .option("driver", "org.postgresql.Driver").option("dbtable", "user_browsing") \
    .option("user", "test").option("password", "test").save()


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
    df2=df2.withColumn("transaction", check_case("transaction")).na.drop()

    df2=df2.filter(df2.user.rlike("^U[0-9]"))
    df2=df2.filter(df2.session_id.rlike("^S[0-9]"))
    
    df2.write.format("jdbc")\
    .option("url", "jdbc:postgresql://postgres:5432/test") \
    .option("driver", "org.postgresql.Driver").option("dbtable", "user_transactions") \
    .option("user", "test").option("password", "test").save()

    # df2.toPandas().to_csv("/opt/spark-data/mycsv3.csv")


    ## calculate time spend per session
    df1 = df1.withColumnRenamed('timestamp', 'browse_timestamp')
    df2 = df2.withColumnRenamed('timestamp', 'transaction_timestamp')

    df3=df1.join(df2, (df1.user == df2.user) & (df1.session_id == df2.session_id), 'inner').\
      select(df1.user,df1.session_id,df1.browse_timestamp,df1.page,df2.transaction_timestamp,df2.transaction)
    
    df3.toPandas().to_csv("/opt/spark-data/mycsv3.csv")

    w = Window.partitionBy(["user","session_id"]).orderBy(asc('browse_timestamp'))
    df4=df3.withColumn('row',row_number().over(w)).filter(col('row') == 1).drop('row')
    df4=df4.withColumn(
    "date_diff_min", 
    f.round((f.col("transaction_timestamp").cast("long") - f.col("browse_timestamp").cast("long"))/60.,2)
    )

    df4.write.format("jdbc")\
    .option("url", "jdbc:postgresql://postgres:5432/test") \
    .option("driver", "org.postgresql.Driver").option("dbtable", "time_spend_per_session") \
    .option("user", "test").option("password", "test").save()    
    # df4.toPandas().to_csv("/opt/spark-data/mycsv4.csv")



    # ## most visited pages
    df5=df5.groupBy("page").agg(f.count('user').alias('visits')).orderBy(col('visits').desc()).show(n=10)
    df5.write.format("jdbc")\
    .option("url", "jdbc:postgresql://postgres:5432/test") \
    .option("driver", "org.postgresql.Driver").option("dbtable", "visited_pages") \
    .option("user", "test").option("password", "test").save()

    # ## most transaction in pages
    df6=df3.groupBy("page").agg(f.count('user').alias('transactions')).orderBy(col('transactions').desc()).show(n=10)
    df6.write.format("jdbc")\
    .option("url", "jdbc:postgresql://postgres:5432/test") \
    .option("driver", "org.postgresql.Driver").option("dbtable", "most_transactions") \
    .option("user", "test").option("password", "test").save()
    