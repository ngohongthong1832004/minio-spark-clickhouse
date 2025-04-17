import pandas as pd
from faker import Faker
import random
import boto3
from io import BytesIO

fake = Faker()
data = []

for i in range(1, 1001):
    data.append({
        "id": i,
        "name": fake.name(),
        "email": fake.email(),
        "age": random.randint(18, 60),
        "country": fake.country()
    })

df = pd.DataFrame(data)

# Upload to MinIO
s3 = boto3.client(
    's3',
    endpoint_url='http://minio:9000',
    aws_access_key_id='minioadmin',
    aws_secret_access_key='minioadmin123'
)

bucket = 'elt-data'
key = 'users.csv'
csv_buf = BytesIO()
df.to_csv(csv_buf, index=False)

# Create bucket if needed
try:
    s3.create_bucket(Bucket=bucket)
except:
    pass

s3.put_object(Bucket=bucket, Key=key, Body=csv_buf.getvalue())
print("✅ Uploaded to MinIO.")
