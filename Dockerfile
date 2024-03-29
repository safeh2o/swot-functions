# To enable ssh & remote debugging on app service change the base image to the one below
FROM mcr.microsoft.com/azure-functions/python:4-python3.10


ENV AzureWebJobsScriptRoot=/home/site/wwwroot \
    AzureFunctionsJobHost__Logging__Console__IsEnabled=true

RUN apt update -y --allow-releaseinfo-change && apt install -y git

COPY requirements.txt /
RUN pip install -r /requirements.txt

COPY . /home/site/wwwroot
