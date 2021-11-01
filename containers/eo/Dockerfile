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
RUN git clone https://github.com/safeh2o/swot-octave-analysis

COPY eo/requirements.txt .

RUN ["pip3", "install", "-r", "requirements.txt"]

RUN mkdir -p output/downloads \
    output/octave

COPY eo/ /app

COPY utils.py /app

WORKDIR /app/swot-octave-analysis/EngineeringOptimizationModel

ENTRYPOINT ["python3", "/app/upload.py"]
