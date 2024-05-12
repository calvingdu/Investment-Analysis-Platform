FROM python:3.11

ENV AIRFLOW_VERSION=2.9.1
ENV PYTHON_VERSION=3.11
ENV CONSTRAINT_URL="https://raw.githubusercontent.com/apache/airflow/constraints-${AIRFLOW_VERSION}/constraints-${PYTHON_VERSION}.txt"
ENV AIRFLOW_UID=501

RUN pip install "apache-airflow==${AIRFLOW_VERSION}" --constraint "${CONSTRAINT_URL}"
COPY . .
RUN pip install -r requirements.txt
