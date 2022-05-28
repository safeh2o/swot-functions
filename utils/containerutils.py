import logging
import mimetypes
import os
import socket
from datetime import datetime
from enum import Enum
from tempfile import NamedTemporaryFile

from azure.storage.blob import BlobServiceClient, ContentSettings
from bson import ObjectId
from pymongo import MongoClient

from .postprocessing import postprocess


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


class ContainerUtils:
    def __init__(
        self,
        azure_storage_key: str,
        mongodb_connection_str: str,
        dataset_id: str,
        sg_template_id: str,
        sg_api_key: str,
        weburl: str,
        dest_container: str,
        src_container: str,
        blob_name: str,
        max_duration: int,
        confidence_level: str,
    ):
        self.azure_storage_key = azure_storage_key
        self.mongodb_connection_str = mongodb_connection_str
        self.dataset_id = dataset_id
        self.sg_template_id = sg_template_id
        self.sg_api_key = sg_api_key
        self.weburl = weburl
        self.dest_container = dest_container
        self.src_container = src_container
        self.blob_name = blob_name

        self.blob_service_client = BlobServiceClient.from_connection_string(
            self.azure_storage_key
        )
        self.blob_result_cc = self.blob_service_client.get_container_client(
            self.dest_container
        )
        self.blob_input_cc = self.blob_service_client.get_container_client(
            self.src_container
        )
        self.mongo_client = MongoClient(self.mongodb_connection_str)
        self.db = self.mongo_client.get_database()
        self.dataset_collection = self.db.get_collection("datasets")
        self.max_duration = max_duration
        self.confidence_level = confidence_level

    def upload_files(self, directory_name: str, file_paths: list[str]):
        for out_file in file_paths:
            with open(out_file, "rb") as out_fp:
                basename = os.path.basename(out_file)
                filepath = os.path.join(directory_name, basename)
                (content_type, content_encoding) = mimetypes.guess_type(out_file)
                content_settings = ContentSettings(content_type, content_encoding)
                self.blob_result_cc.upload_blob(
                    filepath,
                    data=out_fp,
                    overwrite=True,
                    content_settings=content_settings,
                )
            logging.info(f"uploaded file: {out_file}")

    def download_src_blob(self) -> str:
        blob_client = self.blob_input_cc.get_blob_client(self.blob_name)
        fp = NamedTemporaryFile(suffix=".csv", delete=False)

        if not blob_client.exists():
            logging.error("No blobs in the queue to process...")
            return

        # download blob and save
        with fp as downloaded_file:
            downloaded_file.write(blob_client.download_blob().readall())

        return os.path.realpath(fp.name)

    def update_dataset(self, extra_data: dict):
        update_operation = {"$set": extra_data}
        self.dataset_collection.update_one(
            {"_id": ObjectId(self.dataset_id)}, update_operation
        )

    def get_user(self, user_id: str):
        db = self.mongo_client.get_database()
        user_collection = db.get_collection("users")
        return user_collection.find_one({"_id": user_id})

    def is_all_analysis_complete(self) -> bool:
        dataset = self.get_dataset()
        return all(
            [
                "status" in dataset and analysis_method.value in dataset["status"]
                for analysis_method in AnalysisMethod
            ]
        )

    def get_dataset(self):
        return self.dataset_collection.find_one({"_id": ObjectId(self.dataset_id)})

    def send_analysis_confirmation_email(self):
        from sendgrid import SendGridAPIClient
        from sendgrid.helpers.mail import Mail

        dataset = self.get_dataset()
        user = self.get_user(dataset["user"])
        results_url = f"{self.weburl}/results/{self.dataset_id}"

        message = Mail(from_email="no-reply@safeh2o.app", to_emails=user["email"])
        message.template_id = self.sg_template_id
        message.dynamic_template_data = {"resultsUrl": results_url}
        try:
            sg = SendGridAPIClient(self.sg_api_key)
            sg.send(message)
        except Exception as e:
            logging.error(e)

    def update_status(
        self,
        analysis_method: AnalysisMethod,
        success: bool,
        message: str,
    ):
        self.update_dataset(
            {
                f"status.{analysis_method.value}": {
                    "success": success,
                    "last_updated": datetime.now(),
                },
                f"{analysis_method.value}_message": message,
            },
        )

        if self.is_all_analysis_complete():
            dataset = self.get_dataset()
            frc_target = dataset["eo"]["reco"]
            case_blobpaths = []
            for case in ["worst", "average"]:
                for timing in ["am", "pm"]:
                    case_blobpaths.append(
                        f"{self.dataset_id}/{self.dataset_id}_{case}_case_{timing}.csv"
                    )
            case_filepaths = []
            for case_blob in case_blobpaths:
                fp = NamedTemporaryFile(suffix=".csv", delete=False)
                fp.write(
                    self.blob_result_cc.get_blob_client(case_blob)
                    .download_blob()
                    .readall()
                )
                fp.flush()
                case_filepaths.append(fp.name)

            input_filepath = self.download_src_blob()

            water_safety = postprocess(
                frc_target=frc_target,
                case_filepaths=case_filepaths,
                input_file=input_filepath,
            )
            self.update_dataset(
                {
                    "safety_range": water_safety["safety_range"],
                    "safe_percent": water_safety["safe_percent"],
                }
            )
            logging.info(
                f"Sending analysis completion email for dataset {self.dataset_id}"
            )
            self.send_analysis_confirmation_email()
            self.update_dataset({"isComplete": True})
