import json
import logging
import os
import tempfile
import traceback

import azure.functions as func
from sendgrid import SendGridAPIClient
from sendgrid.helpers.mail import Content, Mail
from swoteo.EO_ens_SWOT import EO_Ensemble
from utils import containerutils
from utils.containerutils import ContainerUtils
from utils.logging import set_logger
from utils.standalone_html import make_html_images_inline

ANALYSIS_METHOD = containerutils.AnalysisMethod.EO


def main(msg: func.QueueMessage) -> None:
    decoded_msg = msg.get_body().decode("utf-8")
    analysis_parameters = json.loads(decoded_msg)

    logging.info(
        "Python queue trigger function processed a queue item: %s",
        decoded_msg,
    )

    controller = ContainerUtils(
        analysis_parameters["AZURE_STORAGE_KEY"],
        analysis_parameters["MONGODB_CONNECTION_STRING"],
        analysis_parameters["DATASET_ID"],
        analysis_parameters["SENDGRID_ANALYSIS_COMPLETION_TEMPLATE_ID"],
        analysis_parameters["SENDGRID_API_KEY"],
        analysis_parameters["WEBURL"],
        analysis_parameters["DEST_CONTAINER_NAME"],
        analysis_parameters["SRC_CONTAINER_NAME"],
        analysis_parameters["BLOB_NAME"],
        analysis_parameters["MAX_DURATION"],
        analysis_parameters["CONFIDENCE_LEVEL"],
    )

    try:
        process_queue(controller)
        message = "OK"
        success = True
    except Exception as ex:
        message = "".join(
            traceback.format_exception(etype=type(ex), value=ex, tb=ex.__traceback__)
        )
        success = False
        logging.error(message)
        sg = SendGridAPIClient(os.environ.get("SENDGRID_API_KEY"))
        RG_NAME = os.getenv("RG_NAME")
        email = Mail(
            from_email="no-reply@safeh2o.app", to_emails=f"errors+{RG_NAME}@safeh2o.app"
        )
        email.subject = f"Error in {RG_NAME} EO"
        email.add_content(
            Content(
                "text/plain",
                f"An error occurred during EO analysis for dataset ID {controller.dataset_id}\n{message}",
            )
        )
        sg.send(email)
    finally:
        controller.update_status(ANALYSIS_METHOD, success, message)


def process_queue(controller: ContainerUtils):
    dataset_id = controller.dataset_id
    # set_logger(f"{dataset_id}-{ANALYSIS_METHOD}")

    input_filepath = controller.download_src_blob()
    base_output_filename = f"{dataset_id}.csv"

    tmpdir = tempfile.mkdtemp()
    output_dirname = os.path.join(tmpdir, dataset_id)
    os.makedirs(output_dirname)

    eo = EO_Ensemble(
        controller.max_duration,
        output_dirname,
        input_filepath,
        controller.confidence_level,
    )

    # results filename will be the same as the input filename, but that's OK because they'll live in different directories
    metadata = eo.run_EO()
    frc = metadata["frc"]
    controller.update_dataset({"eo": {"reco": frc}})

    output_files = [
        os.path.join(output_dirname, file) for file in os.listdir(output_dirname)
    ]

    directory_name = os.path.join(dataset_id, "eo")
    controller.upload_files(directory_name, output_files)
