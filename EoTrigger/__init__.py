import logging
import os
import tempfile
import traceback

import matplotlib as mpl
from matplotlib import pyplot as plt
from swoteo.EO_ens_SWOT import EO_Ensemble
from utils.standalone_html import make_html_images_inline
from utils.swotutils import AnalysisMethod, AnalysisUtils

mpl.use("agg")
plt.ioff()

ANALYSIS_METHOD = AnalysisMethod.EO


def main(msg: dict) -> None:
    dataset_id = msg["DATASET_ID"]

    logging.info(
        "In EO Trigger: %s",
        msg,
    )

    controller = AnalysisUtils(
        msg["AZURE_STORAGE_KEY"],
        msg["MONGODB_CONNECTION_STRING"],
        dataset_id,
        msg["SENDGRID_ANALYSIS_COMPLETION_TEMPLATE_ID"],
        msg["SENDGRID_API_KEY"],
        msg["WEBURL"],
        msg["DEST_CONTAINER_NAME"],
        msg["SRC_CONTAINER_NAME"],
        msg["BLOB_NAME"],
        msg["MAX_DURATION"],
        msg["CONFIDENCE_LEVEL"],
        msg["RG_NAME"],
        msg["ERROR_RECEPIENT_EMAIL"],
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

    return "Done EO"


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
