FROM python:3.10-slim

RUN mkdir -p /opt/dagster/dagster_home

ENV DAGSTER_HOME=/opt/dagster/dagster_home/

WORKDIR /opt/dagster/app

RUN pip install \ 
        dagster \
        dagster-webserver \
        dagster-duckdb \
        dagster-dbt \
        dbt-duckdb \
        duckdb \
        beautifulsoup4 \
        s3fs \
        sqlescapy \
        pandas 

# Copy your code and workspace to /opt/dagster/app
COPY ../pipeline /opt/dagster/app


# Copy dagster instance YAML to $DAGSTER_HOME
COPY ../pipeline/dagster.yaml $DAGSTER_HOME


EXPOSE 4000

ENTRYPOINT ["dagster-webserver", "-h", "0.0.0.0", "-p", "4000"]
