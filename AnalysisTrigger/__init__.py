import json
import logging
import os

import azure.functions as func
import certifi
from bson import ObjectId
from pymongo import MongoClient
from utils.logging import set_logger
from utils.standardize import Datapoint

PAPERTRAIL_ADDRESS = os.getenv("PAPERTRAIL_ADDRESS")
PAPERTRAIL_PORT = int(os.getenv("PAPERTRAIL_PORT", 0))
AZURE_STORAGE_KEY = os.getenv("AzureWebJobsStorage")
ANALYSIS_CONTAINER_NAME = os.getenv("ANALYSIS_CONTAINER_NAME")
RESULTS_CONTAINER_NAME = os.getenv("RESULTS_CONTAINER_NAME")
TENANT_ID = os.getenv("TENANT_ID")
CLIENT_ID = os.getenv("CLIENT_ID")
CLIENT_SECRET = os.getenv("CLIENT_SECRET")
SUBSCRIPTION_ID = os.getenv("SUBSCRIPTION_ID")
REGISTRY_NAME = os.getenv("REGISTRY_NAME")
RG_LOCATION = os.getenv("RG_LOCATION")
RG_NAME = os.getenv("RG_NAME")
WEBURL = os.getenv("WEBURL").rstrip("/")
SENDGRID_API_KEY = os.getenv("SENDGRID_API_KEY")
SENDGRID_ANALYSIS_COMPLETION_TEMPLATE_ID = os.getenv(
    "SENDGRID_ANALYSIS_COMPLETION_TEMPLATE_ID"
)
MONGODB_CONNECTION_STRING = os.getenv("MONGODB_CONNECTION_STRING")
ANN_CONTAINER_NAME = "serverann"
EO_CONTAINER_NAME = "servereo"

if not WEBURL.startswith("http"):
    WEBURL = f"https://{WEBURL}"


def datapoint_eq(datapoint1, datapoint2):
    return (
        datapoint1["tsDate"] == datapoint2["tsDate"]
        and datapoint1["hhDate"] == datapoint2["hhDate"]
    )


def remove_duplicates(datapoints: list[dict]) -> list[Datapoint]:
    resolved_datapoints = []
    for datapoint in datapoints:
        latest = datapoint
        duplicates = filter(lambda x: datapoint_eq(datapoint, x), datapoints)
        for d in duplicates:
            if (
                d["dateUploaded"] > latest["dateUploaded"]
                and d["overwriting"]
                or not datapoint["overwriting"]
            ):
                latest = d
        resolved_datapoints.append(Datapoint.from_document(latest))

    return resolved_datapoints


def main(
    msg: func.QueueMessage,
    output: func.Out[bytes],
    anntrigger: func.Out[str],
    eotrigger: func.Out[str],
) -> None:
    # set_logger("SWOT-FUNCTIONS-ANALYSIS")

    logging.info(
        "Python queue trigger function processed a queue item: %s",
        msg.get_body().decode("utf-8"),
    )

    ca = certifi.where()
    msg_json = msg.get_json()
    dataset_id = msg_json["datasetId"]

    db = MongoClient(MONGODB_CONNECTION_STRING, tlsCAFile=ca).get_database()
    dataset_collection = db.get_collection("datasets")
    datapoint_collection = db.get_collection("datapoints")
    dataset = dataset_collection.find_one({"_id": ObjectId(dataset_id)})
    (start_date, end_date) = (dataset["startDate"], dataset["endDate"])

    date_filter = {"$lt": end_date}
    if start_date:
        date_filter["$gt"] = start_date

    datapoint_documents = list(
        datapoint_collection.find(
            {
                "tsDate": date_filter,
                "overwriting": {"$ne": None},
                "dateUploaded": {"$ne": None},
                "fieldsite": dataset["fieldsite"],
            }
        ).sort("tsDate", 1)
    )

    resolved_datapoints = remove_duplicates(datapoint_documents)
    dataset_collection.update_one(
        {"_id": ObjectId(dataset_id)},
        {
            "$set": {
                "firstSample": resolved_datapoints[0].ts_date,
                "lastSample": resolved_datapoints[-1].ts_date,
                "nSamples": len(resolved_datapoints),
            }
        },
    )
    Datapoint.add_timezones(resolved_datapoints)
    lines = Datapoint.get_csv_lines(resolved_datapoints)

    output.set("\n".join(lines))

    analysis_parameters = {
        "AZURE_STORAGE_KEY": AZURE_STORAGE_KEY,
        "MONGODB_CONNECTION_STRING": MONGODB_CONNECTION_STRING,
        "BLOB_NAME": f"{dataset_id}.csv",
        "DATASET_ID": dataset_id,
        "SRC_CONTAINER_NAME": ANALYSIS_CONTAINER_NAME,
        "DEST_CONTAINER_NAME": RESULTS_CONTAINER_NAME,
        "CONFIDENCE_LEVEL": dataset["confidenceLevel"],
        "MAX_DURATION": dataset["maxDuration"],
        "SENDGRID_API_KEY": SENDGRID_API_KEY,
        "SENDGRID_ANALYSIS_COMPLETION_TEMPLATE_ID": SENDGRID_ANALYSIS_COMPLETION_TEMPLATE_ID,
        "WEBURL": WEBURL,
        "PAPERTRAIL_ADDRESS": PAPERTRAIL_ADDRESS,
        "PAPERTRAIL_PORT": PAPERTRAIL_PORT,
        "NETWORK_COUNT": os.getenv("NETWORK_COUNT"),
        "EPOCHS": os.getenv("EPOCHS"),
        "RG_NAME": RG_NAME
    }

    anntrigger.set(json.dumps(analysis_parameters))
    eotrigger.set(json.dumps(analysis_parameters))
