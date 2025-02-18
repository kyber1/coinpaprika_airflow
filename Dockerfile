FROM apache/airflow:2.10.1

ENV AIRFLOW__CORE__LOAD_EXAMPLES=False

RUN pip install google-cloud-storage gcsfs pandas

