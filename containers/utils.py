import logging
import mimetypes
import os
import socket
from enum import Enum
from logging.handlers import SysLogHandler
from tempfile import NamedTemporaryFile

from azure.identity import ClientSecretCredential
from azure.mgmt.containerinstance import ContainerInstanceManagementClient
from azure.storage.blob import BlobServiceClient, ContentSettings
from bson import ObjectId
from pymongo import MongoClient
from pymongo.collection import Collection

from postprocessing import postprocess


class Status(Enum):
    FAIL = 0
    SUCCESS = 1


class AnalysisMethod(Enum):
    ANN = "ann"
    EO = "eo"


class ContextFilter(logging.Filter):
    hostname = socket.gethostname()

    def filter(self, record):
        record.hostname = ContextFilter.hostname
        return True


PAPERTRAIL_ADDRESS = os.getenv("PAPERTRAIL_ADDRESS")
PAPERTRAIL_PORT = int(os.getenv("PAPERTRAIL_PORT", 0))
AZURE_STORAGE_KEY = os.getenv("AZURE_STORAGE_KEY")
MONGODB_CONNECTION_STRING = os.getenv("MONGODB_CONNECTION_STRING")
DATASET_ID = os.getenv("DATASET_ID")
TENANT_ID = os.getenv("TENANT_ID")
CLIENT_ID = os.getenv("CLIENT_ID")
CLIENT_SECRET = os.getenv("CLIENT_SECRET")
SUBSCRIPTION_ID = os.getenv("SUBSCRIPTION_ID")
RG_NAME = os.getenv("RG_NAME")
SENDGRID_ANALYSIS_COMPLETION_TEMPLATE_ID = os.getenv(
    "SENDGRID_ANALYSIS_COMPLETION_TEMPLATE_ID"
)
SENDGRID_API_KEY = os.getenv("SENDGRID_API_KEY")
WEBURL = os.getenv("WEBURL")
DEST_CONTAINER_NAME = os.getenv("DEST_CONTAINER_NAME")
SRC_CONTAINER_NAME = os.getenv("SRC_CONTAINER_NAME")
INPUT_FILENAME = os.getenv("BLOB_NAME", "")

blob_service_client = BlobServiceClient.from_connection_string(AZURE_STORAGE_KEY)
blob_result_client = blob_service_client.get_container_client(DEST_CONTAINER_NAME)
blob_input_client = blob_service_client.get_container_client(SRC_CONTAINER_NAME)


def set_logger(prefix) -> logging.Logger:
    syslog = SysLogHandler(address=(PAPERTRAIL_ADDRESS, PAPERTRAIL_PORT))
    syslog.addFilter(ContextFilter())
    format = f"%(asctime)s %(hostname)s {prefix}: %(message)s"
    formatter = logging.Formatter(format, datefmt="%b %d %H:%M:%S")
    syslog.setFormatter(formatter)
    logger = logging.getLogger()
    logger.addHandler(syslog)
    logger.setLevel(logging.INFO)

    return logger


def upload_files(file_paths):
    container_client = blob_service_client.get_container_client(DEST_CONTAINER_NAME)

    for out_file in file_paths:
        with open(out_file, "rb") as out_fp:
            (content_type, content_encoding) = mimetypes.guess_type(out_file)
            content_settings = ContentSettings(content_type, content_encoding)
            container_client.upload_blob(
                out_file, data=out_fp, overwrite=True, content_settings=content_settings
            )
        logging.info(f"uploaded file: {out_file}")


def download_src_blob() -> str:
    blob_client = blob_service_client.get_blob_client(
        SRC_CONTAINER_NAME, INPUT_FILENAME
    )

    if not blob_client.exists():
        logging.error("No blobs in the queue to process...")
        return

    # download blob and save
    with open(INPUT_FILENAME, "wb") as downloaded_file:
        downloaded_file.write(blob_client.download_blob().readall())

    return INPUT_FILENAME


def update_dataset(extra_data: dict):
    dataset_collection = get_dataset_collection()
    update_operation = {"$set": extra_data}
    dataset_collection.update_one({"_id": ObjectId(DATASET_ID)}, update_operation)


def get_dataset_collection() -> Collection:
    db = MongoClient(MONGODB_CONNECTION_STRING).get_database()
    return db.get_collection("datasets")


def get_user(userId):
    db = MongoClient(MONGODB_CONNECTION_STRING).get_database()
    user_collection = db.get_collection("users")
    return user_collection.find_one({"_id": userId})


def is_all_analysis_complete() -> bool:
    dataset = get_dataset()
    return all(
        [
            "status" in dataset and analysis_method.value in dataset["status"]
            for analysis_method in AnalysisMethod
        ]
    )


def get_dataset():
    dataset_collection = get_dataset_collection()
    return dataset_collection.find_one({"_id": ObjectId(DATASET_ID)})


def remove_container_group():
    sp = ClientSecretCredential(
        client_id=CLIENT_ID, client_secret=CLIENT_SECRET, tenant_id=TENANT_ID
    )
    ci_client = ContainerInstanceManagementClient(sp, subscription_id=SUBSCRIPTION_ID)
    logging.info(f"Deleting container group {DATASET_ID}")
    ci_client.container_groups.begin_delete(RG_NAME, DATASET_ID)


def send_analysis_confirmation_email():
    from sendgrid import SendGridAPIClient
    from sendgrid.helpers.mail import Mail

    dataset = get_dataset()
    user = get_user(dataset["user"])
    results_url = f"{WEBURL}/results/{DATASET_ID}"

    message = Mail(from_email="no-reply@safeh2o.app", to_emails=user["email"])
    message.template_id = SENDGRID_ANALYSIS_COMPLETION_TEMPLATE_ID
    message.dynamic_template_data = {"resultsUrl": results_url}
    try:
        sg = SendGridAPIClient(SENDGRID_API_KEY)
        sg.send(message)
    except Exception as e:
        print(e)


def update_status(analysis_method: AnalysisMethod, success: bool, message: str):
    update_dataset(
        {
            f"status.{analysis_method.value}": {
                "success": success,
                "message": message,
            }
        }
    )

    if is_all_analysis_complete():
        dataset = get_dataset()
        frc_target = dataset["eo"]["reco"]
        case_blobpaths = []
        for case in ["worst", "average"]:
            for timing in ["am", "pm"]:
                case_blobpaths.append(
                    f"{DATASET_ID}/{DATASET_ID}_{case}_case_{timing}.csv"
                )
        case_filepaths = []
        for case_blob in case_blobpaths:
            fp = NamedTemporaryFile(suffix=".csv", delete=False)
            fp.write(
                blob_result_client.get_blob_client(case_blob).download_blob().readall()
            )
            fp.flush()
            case_filepaths.append(fp.name)

        input_filepath = download_src_blob()

        water_safety = postprocess(
            frc_target=frc_target,
            case_filepaths=case_filepaths,
            input_file=input_filepath,
        )
        update_dataset(
            {
                "safety_range": water_safety["safety_range"],
                "safe_percent": water_safety["safe_percent"],
            }
        )
        logging.info(f"Sending analysis completion email for dataset {DATASET_ID}")
        send_analysis_confirmation_email()
        update_dataset({"isComplete": True})

        try:
            remove_container_group()
        except:
            logging.error(f"Error while trying to remove container group {DATASET_ID}")
