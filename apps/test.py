import pyspark
from pyspark.sql import SparkSession
conf_list = [
            ('spark.sql.shuffle.partitions', '1'),
            ('spark.mongodb.input.sampleSize', '1'),
            ('spark.app.name', 'mongo'),
            ('spark.mongodb.input.partitioner', 'MongoPaginateByCountPartitioner'),
            ('spark.mongodb.input.partitionerOptions.numberOfPartitions', '1'),
            ('spark.executor.heartbeatInterval', '35900s'),
            ('spark.network.timeout', '36000s')
        ]
configuration = pyspark.SparkConf().setAll(conf_list)
sc = pyspark.SparkContext(conf=configuration).getOrCreate()
sql_context = pyspark.SQLContext(sc)
spark_session = SparkSession.builder.config(conf=configuration).getOrCreate()


df=sql_context.read.format('com.mongodb.spark.sql.DefaultSource')\
    .option("uri", "mongodb://mongodb_container:27017/local") \
    .option("spark.mongodb.input.collection", "startup_log") \
    .load()

df.printSchema()

#create Temp view of df to view the data 
table = df.createOrReplaceTempView("df")

#to read table present in mongodb
query1 = spark_session.sql("SELECT * FROM df ")
query1.show(10000)
