FROM ubuntu:20.04

ENV TZ=America/Toronto \
    DEBIAN_FRONTEND=noninteractive
RUN ln -snf /usr/share/zoneinfo/$TZ /etc/localtime && echo $TZ > /etc/timezone

WORKDIR /app

RUN apt update -y && apt install -y \
    python3 \
    python3-pip \
    octave \
    octave-statistics \
    git \
    zip

ADD https://api.github.com/repos/safeh2o/swot-octave-analysis/git/refs/heads/master version.json
RUN git clone https://github.com/safeh2o/swot-octave-analysis eo/swot-octave-analysis

COPY requirements.txt .
COPY eo/requirements.txt ./eo

RUN ["pip3", "install", "-r", "eo/requirements.txt"]

RUN mkdir -p eo/output/downloads \
    eo/output/octave

COPY . .

WORKDIR /app/eo/swot-octave-analysis/EngineeringOptimizationModel
ENV PYTHONPATH="/app"
CMD ["python3", "/app/eo/upload.py"]
