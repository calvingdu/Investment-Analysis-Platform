FROM apache/airflow:2.9.1
COPY . .
RUN pip install apache-airflow==${AIRFLOW_VERSION} -r requirements.txt
