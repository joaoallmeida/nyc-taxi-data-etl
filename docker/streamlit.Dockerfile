FROM python:3.10-slim

WORKDIR /app

RUN pip3 install \
        streamlit \
        plotly \
        duckdb \
        --no-cache-dir

COPY ../dashboard /app

EXPOSE 8501

ENTRYPOINT ["streamlit", "run", "app.py", "--server.port=8501", "--server.address=0.0.0.0"]
