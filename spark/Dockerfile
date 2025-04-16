# spark/Dockerfile
FROM bitnami/spark:latest

# Cài pip requirements
COPY requirements.txt /tmp/requirements.txt
RUN pip install --no-cache-dir -r /tmp/requirements.txt

