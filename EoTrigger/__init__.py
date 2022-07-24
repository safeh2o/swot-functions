import json
import logging
import os
import tempfile
import traceback

import azure.functions as func
import matplotlib as mpl
from matplotlib import pyplot as plt
from swoteo.EO_ens_SWOT import EO_Ensemble
from utils.standalone_html import make_html_images_inline
from utils.swotutils import AnalysisMethod, AnalysisUtils

mpl.use("agg")
plt.ioff()

ANALYSIS_METHOD = AnalysisMethod.EO


def main(msg: func.QueueMessage) -> None:
    decoded_msg = msg.get_body().decode("utf-8")
    analysis_parameters = json.loads(decoded_msg)
    dataset_id = analysis_parameters["DATASET_ID"]

    logging.info(
        "Python queue trigger function processed a queue item: %s",
        decoded_msg,
    )

    controller = AnalysisUtils(
        analysis_parameters["AZURE_STORAGE_KEY"],
        analysis_parameters["MONGODB_CONNECTION_STRING"],
        dataset_id,
        analysis_parameters["SENDGRID_ANALYSIS_COMPLETION_TEMPLATE_ID"],
        analysis_parameters["SENDGRID_API_KEY"],
        analysis_parameters["WEBURL"],
        analysis_parameters["DEST_CONTAINER_NAME"],
        analysis_parameters["SRC_CONTAINER_NAME"],
        analysis_parameters["BLOB_NAME"],
        analysis_parameters["MAX_DURATION"],
        analysis_parameters["CONFIDENCE_LEVEL"],
        analysis_parameters["RG_NAME"],
        analysis_parameters["ERROR_RECEPIENT_EMAIL"],
    )

    success = True
    try:
        process_queue(controller)
        message = "OK"
    except Exception as ex:
        message = "".join(
            traceback.format_exception(etype=type(ex), value=ex, tb=ex.__traceback__)
        )
        success = False
        logging.error(message)

        controller.send_error_email(ANALYSIS_METHOD, message)
    finally:
        controller.update_status(ANALYSIS_METHOD, success, message)


def process_queue(controller: AnalysisUtils):
    dataset_id = controller.dataset_id

    input_filepath = controller.download_src_blob()

    tmpdir = tempfile.mkdtemp()

    eo = EO_Ensemble(
        controller.max_duration,
        tmpdir,
        input_filepath,
        controller.confidence_level,
    )

    # results filename will be the same as the input filename, but that's OK because they'll live in different directories
    metadata = eo.run_EO()
    frc = metadata["frc"]
    controller.update_dataset({"eo": {"reco": frc}})

    output_files = [
        os.path.realpath(os.path.join(tmpdir, file)) for file in os.listdir(tmpdir)
    ]

    directory_name = os.path.join(dataset_id, "eo")
    controller.upload_files(directory_name, output_files)
