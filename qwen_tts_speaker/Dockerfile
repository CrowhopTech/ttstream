# FROM nvcr.io/nvidia/pytorch:24.04-py3
FROM python:3.12-slim

WORKDIR /app

RUN apt-get update -y
RUN apt-get install -y sox

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY . .

CMD ["python", "-u", "-m", "main"]
