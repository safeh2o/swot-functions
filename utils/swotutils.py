from __future__ import annotations

import json
import logging
import mimetypes
import os
from datetime import datetime
from enum import Enum
from tempfile import NamedTemporaryFile
from typing import Any, Dict, TypedDict

import requests
from azure.storage.blob import BlobServiceClient, ContentSettings
from bson import ObjectId
from pymongo import MongoClient
from pymongo.database import Database
from sendgrid import SendGridAPIClient
from sendgrid.helpers.mail import Content, Mail

from .postprocessing import get_water_safety


class Status(Enum):
    FAIL = 0
    SUCCESS = 1


class AnalysisMethod(Enum):
    ANN = "ann"
    EO = "eo"


class AnalysisUtils:
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
        rg_name: str,
        error_recepient: str,
    ):
        self.azure_storage_key = azure_storage_key
        self.mongodb_connection_str = mongodb_connection_str
        self.dataset_id = dataset_id
        self.sg_template_id = sg_template_id
        self.sg_api_key = sg_api_key
        self.sg_client = SendGridAPIClient(self.sg_api_key)
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
        self.rg_name = rg_name
        self.error_recepient = error_recepient
        self.locations = get_locations_from_fieldsite_id(
            self.get_fieldsite_id(), self.db
        )

    def upload_files(self, directory_name: str, file_paths: list[str]):
        for out_file in file_paths:
            with open(out_file, "rb") as out_fp:
                basename = os.path.basename(out_file)
                filepath = os.path.join(directory_name, basename)
                (content_type, content_encoding) = mimetypes.guess_type(out_file)
                content_settings = ContentSettings(content_type, content_encoding)
                if not self.blob_result_cc.exists():
                    self.blob_result_cc.create_container()
                self.blob_result_cc.upload_blob(
                    filepath,
                    data=out_fp,
                    overwrite=True,
                    content_settings=content_settings,
                )
            logging.info("uploaded file: %s", out_file)

    def download_src_blob(self) -> str:
        blob_client = self.blob_input_cc.get_blob_client(self.blob_name)
        tmp_fp = NamedTemporaryFile(suffix=".csv", delete=False)

        if not blob_client.exists():
            logging.error("No blobs in the queue to process...")
            return ""

        # download blob and save
        with tmp_fp as downloaded_file:
            downloaded_file.write(blob_client.download_blob().readall())

        return os.path.realpath(tmp_fp.name)

    def update_dataset(self, extra_data: dict):
        update_operation = {"$set": extra_data}
        self.dataset_collection.update_one(
            {"_id": ObjectId(self.dataset_id)}, update_operation
        )

    def get_user(self):
        dataset = self.get_dataset()
        user_id = dataset["user"]
        user_collection = self.db.get_collection("users")
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
        user = self.get_user()
        results_url = f"{self.weburl}/results/{self.dataset_id}"
        fieldsite_name = self.locations["fieldsite"]

        message = Mail(from_email="no-reply@safeh2o.app", to_emails=user["email"])
        message.template_id = self.sg_template_id
        message.dynamic_template_data = {
            "resultsUrl": results_url,
            "fieldsiteName": fieldsite_name,
        }
        try:
            self.sg_client.send(message)
        except Exception as ex:
            logging.error(ex)

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

    def postprocess(self):
        dataset = self.get_dataset()
        completion_status = "failed"
        ann_passed = False
        eo_passed = False
        water_safety = {}
        try:
            ann_passed = dataset["status"][AnalysisMethod.ANN.value]["success"] or False
            eo_passed = dataset["status"][AnalysisMethod.EO.value]["success"] or False
            frc_target = dataset["eo"]["reco"]
        except KeyError:
            frc_target = None

        if ann_passed and eo_passed:
            completion_status = "complete"
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

            water_safety = get_water_safety(
                frc_target=frc_target,
                case_filepaths=case_filepaths,
                input_file=input_filepath,
            )

        self.update_dataset(
            {
                "safety_range": water_safety.get("safety_range"),
                "safe_percent": water_safety.get("safe_percent"),
                "completionStatus": completion_status,
            }
        )
        logging.info(
            "Sending analysis completion email for dataset %s", self.dataset_id
        )
        self.send_analysis_confirmation_email()

    def get_error_message(self, message: str, analysis_method: AnalysisMethod):
        web_url = os.getenv("WEBURL")
        country_name = self.locations["country"]
        area_name = self.locations["area"]
        fieldsite_name = self.locations["fieldsite"]
        user = self.get_user()
        user_fullname = f'{user["name"]["first"]} {user["name"]["last"]}'
        user_email = user["email"]
        dataset = self.get_dataset()
        date_of_analysis: datetime = dataset["dateCreated"]
        first_sample = dataset["firstSample"]
        last_sample = dataset["lastSample"]
        error_message = f"""An error occurred during analysis.
                        Analysis type: {analysis_method.value}
                        Dataset ID: {self.dataset_id}
                        User name: {user_fullname}
                        User email: {user_email}
                        Fieldsite: {fieldsite_name}
                        Area: {area_name}
                        Country: {country_name}
                        Date of analysis: {date_of_analysis.isoformat()}
                        First sample: {first_sample.isoformat()}
                        Last sample: {last_sample.isoformat()}
                        Environment: {web_url}

                        Stack trace:
                        {message}
                        """

        return error_message

    def send_error_email(self, analysis_method: AnalysisMethod, error_message: str):
        email = Mail(
            from_email="no-reply@safeh2o.app",
            to_emails=self.error_recepient,
        )
        email.subject = f"Error in {self.rg_name} {analysis_method.value}"
        email.add_content(
            Content(
                "text/plain",
                error_message,
            )
        )
        self.sg_client.send(email)

    def send_slack_message(self, message: str):
        webhook_url = os.getenv("SLACK_WEBHOOK_URL")
        if not webhook_url:
            logging.error("Slack webhook URL not set. Not sending Slack message")
            return

        headers = {"Content-Type": "application/json"}
        body = {"text": message}

        requests.post(webhook_url, data=json.dumps(body), headers=headers)

    def handle_error(self, analysis_method: AnalysisMethod, message: str):
        error_message = self.get_error_message(message, analysis_method)

        self.send_error_email(analysis_method, error_message)
        if int(os.getenv("ENABLE_SLACK_NOTIFICATIONS", '0')):
            self.send_slack_message(error_message)

    def get_fieldsite_id(self):
        dataset = self.get_dataset()
        return dataset["fieldsite"]


class LocationInfo(TypedDict):
    country: str
    area: str
    fieldsite: str


def get_locations_from_fieldsite_id(
    fieldsite_id: ObjectId, db: Database[Dict[str, Any]]
) -> LocationInfo:
    fieldsite_object = db.get_collection("fieldsites").find_one({"_id": fieldsite_id})
    fieldsite_name = fieldsite_object["name"]
    area_object = db.get_collection("areas").find_one({"fieldsites": fieldsite_id})
    area_id = area_object["_id"]
    area_name = area_object["name"]
    country_object = db.get_collection("countries").find_one({"areas": area_id})
    country_name = country_object["name"]
    return {"country": country_name, "area": area_name, "fieldsite": fieldsite_name}
