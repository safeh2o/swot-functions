FROM python:3.8

WORKDIR /app
RUN ["mkdir", "ann"]

COPY requirements.txt .
COPY ann/requirements.txt ./ann

RUN ["pip", "install", "-r", "ann/requirements.txt"]

COPY . .

ENV PYTHONPATH="/app"
CMD ["python", "/app/ann/upload.py"]
