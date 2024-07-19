FROM python:3.10-slim

WORKDIR /app

# RUN apt-get update && apt-get install -y \
#     build-essential \
#     curl \
#     software-properties-common \
#     && rm -rf /var/lib/apt/lists/*

RUN pip3 install \
        streamlit \
        plotly \
        duckdb \
        --no-cache-dir

COPY ../dashboard /app

EXPOSE 8501

# HEALTHCHECK CMD curl --fail http://localhost:8501/_stcore/health

ENTRYPOINT ["streamlit", "run", "app.py", "--server.port=8501", "--server.address=0.0.0.0"]