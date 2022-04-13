import csv
import logging
import os
import tempfile
from io import TextIOWrapper
from uuid import uuid4

import azure.functions as func
import certifi
import openpyxl
from azure.storage.blob import BlobClient, ContainerClient
from bson.objectid import ObjectId
from pymongo import MongoClient
from utils.logging import set_logger
from utils.mailing import send_mail
from utils.standardize import extract

PAPERTRAIL_ADDRESS = os.getenv("PAPERTRAIL_ADDRESS")
PAPERTRAIL_PORT = int(os.getenv("PAPERTRAIL_PORT", 0))


def generate_random_filename(extension="csv"):
    return str(uuid4()) + f".{extension}"


def convert_xlsx_blob_to_csv(blob_client: BlobClient, fp: TextIOWrapper):
    xlsx_fp = tempfile.NamedTemporaryFile(suffix=".xlsx", delete=False)
    xlsx_fp.write(blob_client.download_blob().readall())
    xlsx_fp.close()

    wb = openpyxl.load_workbook(xlsx_fp.name, read_only=True, data_only=True)
    sh = wb.active
    wr = csv.writer(fp, quoting=csv.QUOTE_MINIMAL, lineterminator="\n")

    for row in sh.rows:
        rowvalues = []
        for cell in row:
            rowvalues.append(cell.value)
        wr.writerow(rowvalues)

    fp.flush()
    wb.close()
    os.remove(xlsx_fp.name)


def main(msg: func.QueueMessage) -> None:
    set_logger("SWOT-FUNCTIONS-UPLOAD")

    ca = certifi.where()
    msg_json = msg.get_json()
    upload_id = msg_json["uploadId"]
    uploader_email = msg_json["uploaderEmail"]
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

    blob_cc = ContainerClient.from_connection_string(
        AZURE_STORAGE_CONNECTION_STRING, in_container_name
    )

    blobs = blob_cc.list_blobs(name_starts_with=upload_id)
    errors = []

    for blob in blobs:
        # generate temp file
        # download file to it
        # standardize it, returning list of DataPoint objects
        # add overwriting flag
        bc = blob_cc.get_blob_client(blob)
        fp = tempfile.NamedTemporaryFile(
            suffix=".csv", mode="w", newline="", delete=False
        )
        tmpname = fp.name
        # handle blob by extension
        ext = blob.name.split(".")[-1]
        if ext == "xlsx":
            # convert xlsx to csv and write to fp
            convert_xlsx_blob_to_csv(bc, fp)
        elif ext == "csv":
            fp.write(bc.download_blob().content_as_text(encoding="utf-8-sig"))
            fp.flush()
        else:
            raise TypeError(f"Invalid file extension {ext}")

        fp.close()
        datapoint_collection = db.get_collection("datapoints")
        datapoints, errors_in_file = extract(tmpname)
        if errors_in_file:
            filename_as_uploaded = "_".join(blob.name.split("_")[1:])
            errors.append(
                {"filename": filename_as_uploaded, "errors_in_file": errors_in_file}
            )

        for datapoint in datapoints:
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
    send_mail(uploader_email, errors)
