from io import TextIOWrapper
import logging

import azure.functions as func
from pymongo import MongoClient
import os
from bson.objectid import ObjectId
from utils.standardize import extract
from azure.storage.blob import BlobServiceClient, BlobClient, ContainerClient
from uuid import uuid4
import certifi
import tempfile

import logging
import socket
from logging.handlers import SysLogHandler

import openpyxl, csv

PAPERTRAIL_ADDRESS = os.getenv("PAPERTRAIL_ADDRESS")
PAPERTRAIL_PORT = int(os.getenv("PAPERTRAIL_PORT", 0))


class ContextFilter(logging.Filter):
    hostname = socket.gethostname()

    def filter(self, record):
        record.hostname = ContextFilter.hostname
        return True


syslog = SysLogHandler(address=(PAPERTRAIL_ADDRESS, PAPERTRAIL_PORT))
syslog.addFilter(ContextFilter())
format = "%(asctime)s %(hostname)s SWOT-FUNCTIONS-UPLOAD: %(message)s"
formatter = logging.Formatter(format, datefmt="%b %d %H:%M:%S")
syslog.setFormatter(formatter)
logger = logging.getLogger()
logger.addHandler(syslog)
logger.setLevel(logging.INFO)


def generate_random_filename(extension="csv"):
    return str(uuid4()) + f".{extension}"


def convert_xlsx_blob_to_csv(blob_client: BlobClient, fp: TextIOWrapper):
    xlsx_fp = tempfile.NamedTemporaryFile(suffix=".xlsx", delete=False)
    xlsx_fp.write(blob_client.download_blob().readall())
    xlsx_fp.close()

    wb = openpyxl.load_workbook(xlsx_fp.name, read_only=True)
    sh = wb.active
    wr = csv.writer(fp, quoting=csv.QUOTE_MINIMAL)

    for row in sh.rows:
        rowvalues = []
        for cell in row:
            rowvalues.append(cell.value)
        wr.writerow(rowvalues)

    fp.flush()
    wb.close()
    os.remove(xlsx_fp.name)


def main(msg: func.QueueMessage) -> None:
    ca = certifi.where()
    msg_json = msg.get_json()
    upload_id = msg_json["uploadId"]
    logging.info(
        "Python queue trigger function processed a queue item: %s",
        upload_id,
    )

    MONGODB_CONNECTION_STRING = os.getenv("MONGODB_CONNECTION_STRING")
    AZURE_STORAGE_CONNECTION_STRING = os.getenv("AzureWebJobsStorage")
    COLLECTION_NAME = os.getenv("COLLECTION_NAME")

    db = MongoClient(MONGODB_CONNECTION_STRING, tlsCAFile=ca).get_database()
    col = db.get_collection(COLLECTION_NAME)
    upl = col.find_one({"_id": ObjectId(upload_id)})
    col.update_one({"_id": ObjectId(upload_id)}, {"$set": {"status": "processing"}})

    is_overwriting = upl["overwriting"]
    in_container_name = upl["containerName"]

    blob_sc = BlobServiceClient.from_connection_string(AZURE_STORAGE_CONNECTION_STRING)
    blob_cc = blob_sc.get_container_client(in_container_name)

    blobs = blob_cc.list_blobs(name_starts_with=upload_id)

    for blob in blobs:
        # generate temp file
        # download file to it
        # standardize it, returning list of DataPoint objects
        # add overwriting flag
        bc = blob_cc.get_blob_client(blob)
        fp = tempfile.NamedTemporaryFile(suffix=".csv", mode="w", delete=False)
        tmpname = fp.name
        # handle blob by extension
        ext = blob.name.split(".")[-1]
        if ext == "xlsx":
            # convert xlsx to csv and write to fp
            convert_xlsx_blob_to_csv(bc, fp)
        elif ext == "csv":
            fp.write(bc.download_blob().content_as_text())
            fp.flush()
        else:
            raise TypeError(f"Invalid file extension {ext}")

        fp.close()
        datapoints = extract(tmpname)
        for datapoint in datapoints:
            datapoint_collection = db.get_collection("datapoints")
            datapoint_collection.insert_one(
                datapoint.to_document(
                    upload=ObjectId(upload_id),
                    fieldsite=upl["fieldsite"],
                    dateUploaded=upl[
                        "dateUploaded"
                    ],  # can be referenced by aggregation, but doing this for simplicity
                    overwriting=is_overwriting,  # can be referenced by aggregation, but doing this for simplicity
                )
            )
        os.remove(tmpname)

    col.update_one({"_id": ObjectId(upload_id)}, {"$set": {"status": "ready"}})
