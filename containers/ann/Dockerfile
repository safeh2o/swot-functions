FROM python:3.8

WORKDIR /app

COPY ann/requirements.txt /app

RUN ["pip", "install", "-r", "requirements.txt"]

COPY ann/ /app

COPY utils.py /app
COPY standalone_html.py /app

ENTRYPOINT ["python", "upload.py"]
