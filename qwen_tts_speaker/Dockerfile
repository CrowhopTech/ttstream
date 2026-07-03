FROM nvcr.io/nvidia/pytorch:24.04-py3

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY . .

CMD ["python", "-m", "qwen_tts_speaker.main"]
