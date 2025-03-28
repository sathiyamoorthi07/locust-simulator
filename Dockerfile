FROM python:3.9-slim

WORKDIR /app

COPY . /app

RUN pip install locust pandas openpyxl

ENTRYPOINT ["locust"]
