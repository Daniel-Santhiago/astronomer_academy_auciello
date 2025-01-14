FROM quay.io/astronomer/astro-runtime:11.3.0

ENV AIRFLOW__CORE__DAGBAG_IMPORT_TIMEOUT=300
ENV AIRFLOW__CORE__DAG_DIR_LIST_INTERVAL=20
ENV AIRFLOW__CORE__MIN_FILE_PROCESS_INTERVAL=5
ENV AIRFLOW__SCHEDULER__TASK_QUEUED_TIMEOUT=12000

ENV AIRFLOW__CORE__DAG_DISCOVERY_SAFE_MODE=False
ENV AIRFLOW__CORE__DAG_FILE_PROCESSOR_TIMEOUT=20
ENV AIRFLOW__WEBSERVER__ALLOW_RAW_HTML_DESCRIPTIONS=True

ENV AIRFLOW__SMTP__SMTP_HOST="smtp.gmail.com" \
    AIRFLOW__SMTP__SMTP_PORT="587" \
    AIRFLOW__SMTP__SMTP_SSL="false" \
    AIRFLOW__SMTP__SMTP_STARTTLS="true" \
    AIRFLOW__SMTP__SMTP_MAIL_FROM="Airflow" \
    AIRFLOW__SMTP__SMTP_USER="danieldeveloper01@gmail.com" \
    AIRFLOW__SMTP__SMTP_PASSWORD="ewqbhootahadcjah"

ENV AIRFLOW__CORE__ZOMBIE_MAX_AGE=3600