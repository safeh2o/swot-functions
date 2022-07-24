import json
import logging
import os
import tempfile
import traceback

import azure.functions as func
import matplotlib as mpl
from matplotlib import pyplot as plt
from swotann.nnetwork import NNetwork
from utils import swotutils
from utils.standalone_html import make_html_images_inline
from utils.swotutils import AnalysisMethod, AnalysisUtils

mpl.use("agg")
plt.ioff()

ANALYSIS_METHOD = swotutils.AnalysisMethod.ANN


def main(msg: func.QueueMessage) -> None:
    decoded_msg = msg.get_body().decode("utf-8")
    analysis_parameters = json.loads(decoded_msg)
    network_count = analysis_parameters.get("NETWORK_COUNT")
    epochs = analysis_parameters.get("EPOCHS")
    dataset_id = analysis_parameters.get("DATASET_ID")

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
        process_queue(controller, network_count, epochs)
        message = "OK"
    except Exception as ex:
        message = "".join(
            traceback.format_exception(
                etype=type(ex), value=ex, tb=ex.__traceback__
            )
        )
        success = False
        logging.error(message)

        controller.send_error_email(AnalysisMethod.ANN, message)
    finally:
        controller.update_status(ANALYSIS_METHOD, success, message)


def process_queue(controller: AnalysisUtils, network_count: int, epochs: int):
    dataset_id = controller.dataset_id
    input_filepath = controller.download_src_blob()
    base_output_filename = f"{dataset_id}.csv"

    with tempfile.TemporaryDirectory() as tmpdir:
        # tmpdir = tempfile.mkdtemp()
        output_dirname = os.path.join(tmpdir, dataset_id)
        os.makedirs(output_dirname)

        # run swot analysis on downloaded blob
        if network_count and epochs:
            ann = NNetwork(int(network_count), int(epochs))
        elif network_count:
            ann = NNetwork(network_count=int(network_count))
        elif epochs:
            ann = NNetwork(epochs=int(epochs))
        else:
            ann = NNetwork()

        # results filename will be the same as the input filename, but that's OK because they'll live in different directories
        results_filepath = os.path.join(output_dirname, base_output_filename)
        report_filepath = results_filepath.replace(".csv", ".html")
        metadata = ann.run_swot(
            input_filepath,
            results_filepath,
            report_filepath,
            controller.max_duration,
        )
        controller.update_dataset({"ann": metadata})

        # make report file standalone (convert all images to base64)
        report_file_standalone = report_filepath.replace(".html", "-standalone.html")
        make_html_images_inline(report_filepath, report_file_standalone)

        output_files = [
            os.path.join(output_dirname, file) for file in os.listdir(output_dirname)
        ]

        directory_name = dataset_id
        controller.upload_files(directory_name, output_files)
