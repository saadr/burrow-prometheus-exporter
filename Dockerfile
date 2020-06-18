FROM python:3.7-slim


WORKDIR /app

COPY burrow_prometheus_exporter/ ./burrow_prometheus_exporter
COPY config.yaml requirements.txt setup.py README.md ./

RUN pip install -e .
RUN pip install -r ./requirements.txt


EXPOSE 8001

ENTRYPOINT ["burrow_prometheus_exporter"]
