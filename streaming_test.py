import sys
import json

from pyspark.sql import SparkSession

from pyspark.sql.functions import expr, monotonically_increasing_id, col, row_number, explode
from pyspark.sql.window import Window
from pyspark.sql.types import StructType, IntegerType, StringType, MapType, from_json, structureData


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
        .builder \
        .config("spark.sql.streaming.schemaInference", True) \
        .appName("StructuredStreamingDataTest")\
        .getOrCreate()

    spark.sparkContext.setLogLevel("ERROR")
    '''
    "devices": {
        "cameras": {
            "device_id": "awJo6rH...",
                "last_event": {
                "has_sound": true,
                "has_motion": true,
                "has_person": true,
                "start_time": "2016-12-29T00:00:00.000Z",
                "end_time": "2016-12-29T18:42:00.000Z"
                }
        }
    }
    '''
    # Expected Schema for JSON data
    schema = StructType() \
        .add("devices", StructType() \
        .add("cameras", MapType(StringType(), StructType().add(...))))

    # Parse the Raw JSON
    nestTimestampFormat = "yyyy-MM-dd'T'HH:mm:ss.sss'Z'"
    jsonOptions = { "timestampFormat": nestTimestampFormat }

    # Create DataSet representing the stream of input lines from kafka
    # Kafka 클러스터 위치, 데이터 읽으려는 토픽 지정
    # spark.readStream () ; DataStreamReader 인터페이스 리턴해주면서 Streaming DataFrames 생성함
    df = spark\
        .readStream\
        .format("kafka")\
        .option("kafka.bootstrap.servers", bootstrapServers)\
        .option(subscribeType, topics)\
        .load() \
        .select(from_json(col("value").cast("string"), schema, jsonOptions).alias("parsed_value"))

    #  $topics 에서 subscribe 해오는 스트리밍 DataFrame임, DataFrameReader에 옵션을 제공해서 configuration 설정
    #  kafka.bootstrap.servers, 토픽 설정 필수, startingOffsets 옵션 따로 지정 안해줌-default) "latest" 
    df.printSchema()

    # Project Relevant Columns
    camera = df \
    .select(explode("parsed_value.devices.cameras")) \
    .select("value.*")

    sightings = camera \
    .select("device_id", "last_event.has_person", "last_event.start_time") \
    .where(col("has_person") == True)



    schema = StructType().add("a", IntegerType()).add("b", StringType())
    df.select( \
        col("key").cast("string"),
        from_json(col("value").cast("string"), schema))

    schemaFromJson = StructType.fromJson(json.loads(schema.json))
    df3 = spark.createDataFrame(
            spark.sparkContext.parallelize(structureData),schemaFromJson)
    df3.printSchema()

    depth0 = df.select("id","created_at", "text", "retweet_count", "favorite_count")
    # user 에서 뽑아올 정보
    user_df = df.select("user.*")
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

    query.awaitTermination()