FROM apache/airflow:2.8.1-python3.10

USER airflow

# Copy requirements into container
COPY requirements.txt /requirements.txt

# Install everything
RUN pip install --no-cache-dir -r /requirements.txt