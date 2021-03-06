import re
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pytz import timezone
import datetime
import argparse


def check_time(from_t, to_t):
    re_day = re.compile("^[0-9]{4}-[0-9]{2}-[0-9]{2}$")
    re_hour = re.compile("^[0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9]{2}$")
    if re_hour.match(from_t):
        return from_t, to_t
    elif re_day.match(from_t):
        return f"{from_t}T00", f"{to_t}T23"
    else:
        return from_t, to_t


def drop_origin_columns(df):
    return df.drop(
        "book_name", "price", "artist", "genre"
    )


spark = SparkSession \
    .builder \
    .enableHiveSupport() \
    .appName("client_log_v1_to_v2") \
    .config("spark.sql.session.timeZone", "Asia/Seoul") \
    .getOrCreate()

client_log_v1_schema = StructType([
    StructField("log_id", StringType(), False),
    StructField("created", TimestampType(), False),
    StructField("timestamp", TimestampType(), False),
    StructField("timezone", StringType(), True),
    StructField("version", StringType(), True),
    StructField("event", StringType(), True),
    StructField("name", StringType(), True),
    StructField("type", StringType(), True),
    StructField("book_name", StringType(), True),
    StructField("artist", StringType(), True),
    StructField("price", IntegerType(), True),
    StructField("genre", StringType(), True),
])


def main(argv):
    # 기본 설장 1시간 전 데이터 처리
    now = datetime.datetime.utcnow()
    hour_ago = now - datetime.timedelta(hours=1)
    from_t = to_t = hour_ago.strftime('%Y-%m-%dT%H')
    # 변수가 있다면
    if argv.s:
        """
        from_t = 2021-02-02T00
        to_t = 2021-02-02T00
        """
        from_t = args.s
        to_t = args.e if args.e else from_t
        from_t, to_t = check_time(from_t, to_t)

    start = datetime.datetime.strptime(from_t, '%Y-%m-%dT%H')
    end = datetime.datetime.strptime(to_t, '%Y-%m-%dT%H')

    while start <= end:
        year = str(start.year)
        month = str(start.month).zfill(2)
        day = str(start.day).zfill(2)
        hour = str(start.hour).zfill(2)
        try:
            client_log = spark.read.format('json').schema(client_log_v1_schema).load(
                f"s3://waisyghost/bronze/client_log/year={year}/month={month}/day={day}/hour={hour}/")
        except Exception as e:
            print(e)
            start += datetime.timedelta(hours=1)
            continue

        # common_log 처리
        after_common_df = client_log \
            .withColumnRenamed("app_version", "version") \
            .withColumnRenamed("timestamp", "created_ts") \
            .drop("created")

        ### event 통합
        # event_name= null -> regist
        new_event = after_common_df.withColumn("event", when(col("event").isNull(), lit("regist")).otherwise(col("event")))

        # property 처리
        add_property = new_event \
            .withColumn("property", to_json(struct(
            col("book_name").alias("book_name"),
            col("price").alias("price"),
            col("artist").alias("artist"),
            col("genre").alias("genre"),
        )))

        # property 속성내 컬럼 삭
        client_log_v2 = drop_origin_columns(add_property)

        # add partition p_ymd
        # time UTC -> KST
        parquet_ymd = start.astimezone(timezone("Asia/Seoul")).strftime("%Y%m%d")

        client_log_v2 = client_log_v2.withColumn("p_ymd", lit(parquet_ymd).cast(StringType()))

        # 파일 1개로 분리
        client_log_v2 = client_log_v2.repartition(1)

        # parquet 저장
        client_log_v2.write.partitionBy("p_ymd") \
            .format("parquet") \
            .option("compression", "snappy") \
            .mode("append") \
            .save("s3://waisyghost/silver/client_log/")

        start += datetime.timedelta(hours=1)

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument('--s', required=False, help='start date')
    parser.add_argument('--e', required=False, help='end date')
    args = parser.parse_args()
    main(args)
