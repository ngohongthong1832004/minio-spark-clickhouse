Dưới đây là bản **`README.md`** đầy đủ, tổng hợp toàn bộ quá trình bạn đã thực hiện để setup hệ thống:

---

## 📦 MinIO + Spark + ClickHouse + Airflow ETL Pipeline

> Một hệ thống ETL hiện đại sử dụng Docker Compose để:
>
> - ✅ Sinh dữ liệu giả (fake) và upload lên **MinIO**
> - ✅ Đọc và xử lý dữ liệu bằng **Spark**
> - ✅ Ghi kết quả phân tích vào **ClickHouse**
> - ✅ Orchestrate toàn bộ pipeline bằng **Apache Airflow**

---

## 🧱 Cấu trúc hệ thống

```bash
MinIO-spark-clickhouse/
│
├── airflow/
│   └── dags/
│       └── spark_etl_dag.py           # DAG chạy toàn bộ pipeline
│
├── spark/
│   ├── app/
│   │   ├── fake_to_minio.py           # Tạo dữ liệu giả và upload lên MinIO
│   │   └── etl_minio_to_clickhouse.py # Spark đọc MinIO và ghi vào ClickHouse
│   └── jars/
│       └── clickhouse-jdbc-all.jar    # JDBC driver cho ClickHouse
│   └── Dockerfile                     # Spark container có sẵn Python, boto3,...
│
├── docker-compose.yml
└── run.sh                             # Chạy hệ thống đầy đủ (build + init + start)
└── run.bat                            # Phiên bản chạy cho Windows
```

---

## 🚀 Cách chạy hệ thống

### ✅ Bước 1: Build & Start toàn bộ

```bash
./run.sh     # hoặc run.bat nếu dùng Windows
```

Tự động:
- 🧱 `docker-compose build`
- ⚙️ Init Airflow DB + tạo user admin
- 🚀 Khởi động toàn bộ hệ thống

> Truy cập Airflow tại: [http://localhost:8080](http://localhost:8080)  
> `Username: admin` – `Password: admin`

---

## 🧪 Các bước chạy trong DAG `spark_etl_minio_to_clickhouse`

| Task | Mô tả |
|------|-------|
| `run_test` | Kiểm tra `docker exec` hoạt động được |
| `generate_fake_data` | Chạy `fake_to_minio.py` để upload dữ liệu CSV lên MinIO |
| `run_spark_job` | Chạy Spark job đọc file từ MinIO → tính `avg(age)` → ghi vào ClickHouse |

---

## 🔧 Các lưu ý kỹ thuật

### 🗂️ MinIO

- Truy cập MinIO tại: [http://localhost:9001](http://localhost:9001)
- Username: `minioadmin` – Password: `minioadmin123`
- Dữ liệu được ghi vào bucket: `elt-data`

### ⚙️ Spark-submit

Spark container chạy:

```bash
spark-submit \
  --jars /app/jars/clickhouse-jdbc-all.jar \
  --driver-class-path /app/jars/clickhouse-jdbc-all.jar \
  --conf spark.executor.extraClassPath=/app/jars/clickhouse-jdbc-all.jar \
  /app/etl_minio_to_clickhouse.py
```

> JDBC Driver: `com.clickhouse.jdbc.ClickHouseDriver`

### 🛡️ Airflow logging (quan trọng)

Để log từ Python hiện trên giao diện Airflow:
- Không redirect log sang file nếu không cần
- Hoặc dùng `tee` để log vừa vào file vừa ra stdout

```bash
python3 /app/fake_to_minio.py 2>&1 | tee -a /tmp/fake.log
```

---

## ✅ Ghi chú thêm

- Mạng: tất cả container nằm trong **mạng mặc định của docker-compose**
- `docker exec` cần mount `/var/run/docker.sock` vào Airflow nếu muốn gọi lệnh từ DAG
- Sử dụng `bash -c 'set -e && ...'` giúp `BashOperator` fail đúng cách nếu có lỗi

---

## ✨ Tương lai nâng cấp

- [ ] Chuyển sang dùng `DockerOperator` hoặc `PythonOperator`
- [ ] Lưu log pipeline vào ClickHouse
- [ ] Hiển thị dashboard kết quả ETL bằng Superset/Grafana
- [ ] Add email/slack alert khi task fail

---