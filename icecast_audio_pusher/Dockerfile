FROM python:3.12-slim

WORKDIR /app

RUN apt-get -y update
RUN apt-get install -y ffmpeg
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY . .

CMD ["python", "-m", "main"]
