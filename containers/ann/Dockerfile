FROM python:3.8

WORKDIR /app
RUN ["mkdir", "ann"]

ADD https://api.github.com/repos/safeh2o/swot-python-analysis/git/refs/heads/master version.json
COPY ./containers/requirements.txt .
COPY ./containers/ann/requirements.txt ./ann

RUN ["pip", "install", "-r", "ann/requirements.txt"]

COPY ./utils ./utils
COPY ./containers .

ENV PYTHONPATH="/app"
CMD ["python", "/app/ann/runann.py"]
