import sys
import json

from pyspark.sql import SparkSession

from pyspark.sql.functions import expr, monotonically_increasing_id, col, row_number, explode, from_json
from pyspark.sql.window import Window
from pyspark.sql.types import StructType, IntegerType, StringType, MapType


if __name__ == "__main__":
    if len(sys.argv) != 4:
        print("""
        Usage: structured_kafka_wordcount.py <bootstrap-servers> <subscribe-type> <topics>
        """, file=sys.stderr)
        sys.exit(-1)

    bootstrapServers = sys.argv[1]
    subscribeType = sys.argv[2]
    topics = sys.argv[3]

    spark = SparkSession\
        .builder\
        .appName("StructuredKafkaTweetData")\
        .getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")

    # Create DataSet representing the stream of input lines from kafka
    # Kafka 클러스터 위치, 데이터 읽으려는 토픽 지정
    # spark.readStream () ; DataStreamReader 인터페이스 리턴해주면서 Streaming DataFrames 생성함
    df_json = spark\
        .read\
        .format("kafka")\
        .option("kafka.bootstrap.servers", bootstrapServers)\
        .option(subscribeType, topics)\
        .option("startingOffsets", "earliest")\
        .option("endingOffsets", "latest")\
        .option("failOnDataLoss", "false")\
        .load()
    # filter out empty values
    df_json = df_json.withColumn("value", expr("string(value)"))\
        .filter(col("value").isNotNull())\
    # get latest version of each record
    df_json = df_json.select("key", expr("struct(offset, value) r"))\
        .groupBy("key").agg(expr("max(r) r"))\
        .select("r.value")
    
    # decode the json values
    df = spark.read.json(
        df_json.rdd.map(lambda x: x.value), multiLine=True)

    # drop corrupt records
    if "_corrupt_record" in df.columns:
        df = (df
                .filter(col("_corrupt_record").isNotNull())
                .drop("_corrupt_record"))

    json_schema = df.schema.json()
    print(json_schema)

    obj = json.loads(json_schema)
    topic_schema = StructType.fromJson(obj)
    
    #  $topics 에서 subscribe 해오는 스트리밍 DataFrame임, DataFrameReader에 옵션을 제공해서 configuration 설정
    #  kafka.bootstrap.servers, 토픽 설정 필수, startingOffsets 옵션 따로 지정 안해줌-default) "latest" 
    # df.printSchema()

    # df.selectExpr("CAST(key AS STRING)","CAST(value AS STRING)")
    # json data일 때 ; ex) value schema: { "a": 1, "b": "string" }
    # schema = StructType().add("a", IntegerType()).add("b", StringType())
    parsed = spark \
    .readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", bootstrapServers) \
    .option(subscribeType, topics) \
    .load() \
    .select(from_json(col("value").cast("string"), topic_schema).alias("parsed_value"))

    # Project Relevant Columns
    # camera = parsed \
    # .select(explode("parsed_value.devices.cameras")) \
    # .select("value.*")

    # sightings = camera \
    # .select("device_id", "last_event.has_person", "last_event.start_time") \
    # .where(col("has_person") == True)

    depth0 = parsed.select("id","created_at", "text", "retweet_count", "favorite_count")
    # user 에서 뽑아올 정보
    user_df = parsed.select("user.*")
    user_df.createOrReplaceTempView("user_sql")
    user = spark.sql("select name, screen_name, profile_image_url from user_sql")

    # retweeted_status에서 뽑아올 정보
    retweet_df = df.select("retweeted_status.*")
    retweet_df.createOrReplaceTempView("ret_sql")
    retweet_sql = spark.sql("select id from ret_sql")
    # id 행 이미 있으니까 RT_id 라고 행 이름 변경
    retweet = retweet_sql.withColumn("RT_id", expr("id"))
    retweet = retweet.drop('id')

    DF1 = depth0.withColumn("row_id", monotonically_increasing_id())
    DF2 = user.withColumn("row_id", monotonically_increasing_id())
    dep_usr = DF1.join(DF2, ("row_id")).drop("row_id")

    DF1_tmp = dep_usr.withColumn("row_id", monotonically_increasing_id())
    window = Window.orderBy(col('row_id'))
    DF1 = DF1_tmp.withColumn('row_id', row_number().over(window))

    DF2_tmp = retweet.withColumn("row_id", monotonically_increasing_id())
    DF2 = DF2_tmp.withColumn('row_id', row_number().over(window))
    result = DF1.join(DF2, ("row_id")).drop("row_id")



    # Start running the query that prints the running counts to the console
    query = result\
        .writeStream\
        .outputMode('complete')\
        .format('console')\
        .start()
