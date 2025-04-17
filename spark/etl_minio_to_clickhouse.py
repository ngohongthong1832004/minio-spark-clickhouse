print("🔥 ETL STARTED")

from pyspark.sql import SparkSession
from pyspark.sql.functions import col

# 👇 Tạo bảng ClickHouse nếu chưa có
from clickhouse_connect import get_client

print("🛠️ Kiểm tra hoặc tạo bảng ClickHouse...")
client = get_client(
    host='clickhouse',
    port=8123,
    username='default',
    password='thong123'
)

client.command('''
CREATE TABLE IF NOT EXISTS users_summary (
  country String,
  avg_age Float32
) ENGINE = MergeTree
ORDER BY country;
''')
print("✅ Bảng ClickHouse đã sẵn sàng!")

# 🔥 Bắt đầu ETL với Spark
spark = SparkSession.builder \
    .appName("MinIO to ClickHouse") \
    .getOrCreate()

# 👇 Cấu hình để Spark đọc từ MinIO
hadoop_conf = spark._jsc.hadoopConfiguration()
hadoop_conf.set("fs.s3a.access.key", "minioadmin")
hadoop_conf.set("fs.s3a.secret.key", "minioadmin123")
hadoop_conf.set("fs.s3a.endpoint", "http://minio:9000")
hadoop_conf.set("fs.s3a.path.style.access", "true")
hadoop_conf.set("fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")

# ✅ Đọc file từ MinIO
df = spark.read.option("header", True).csv("s3a://elt-data/users.csv")
df = df.withColumn("age", col("age").cast("int"))

# ✅ Tính trung bình tuổi theo quốc gia
agg = df.groupBy("country").avg("age").withColumnRenamed("avg(age)", "avg_age")
agg.printSchema()
agg.show()

# ✅ Ghi vào ClickHouse
try:
    agg.write.format("jdbc").options(
        url="jdbc:clickhouse://clickhouse:8123/default",
        dbtable="users_summary",
        user="default",
        password="thong123",
        driver="com.clickhouse.jdbc.ClickHouseDriver"
    ).mode("append").save()
    print("✅ Ghi vào ClickHouse thành công!")
except Exception as e:
    import traceback
    print("❌ Lỗi khi ghi vào ClickHouse:")
    traceback.print_exc()

print("✅ ETL xong từ MinIO → Spark → ClickHouse!")