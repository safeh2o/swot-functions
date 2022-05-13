import logging
import os
import traceback

import containerutils
from standalone_html import make_html_images_inline
from swotann.nnetwork import NNetwork
from utils.logging import set_logger

ANALYSIS_METHOD = containerutils.AnalysisMethod.ANN


def process_queue():
    input_filename = containerutils.download_src_blob()
    storage_target = os.getenv("MAX_DURATION", 3)
    network_count = os.getenv("NETWORK_COUNT", None)
    epochs = os.getenv("EPOCHS", None)
    dataset_id = os.getenv("DATASET_ID", None)
    set_logger(f"{dataset_id}-{ANALYSIS_METHOD}")

    output_dirname = dataset_id
    os.mkdir(output_dirname)

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
    results_file = os.path.join(output_dirname, input_filename)
    report_file = results_file.replace(".csv", ".html")
    metadata = ann.run_swot(input_filename, results_file, report_file, storage_target)
    containerutils.update_dataset({"ann": metadata})

    # make report file standalone (convert all images to base64)
    report_file_standalone = report_file.replace(".html", "-standalone.html")
    make_html_images_inline(report_file, report_file_standalone)

    output_files = [
        os.path.join(output_dirname, file) for file in os.listdir(output_dirname)
    ]

    containerutils.upload_files(output_files)


if __name__ == "__main__":
    try:
        process_queue()
        message = "OK"
        success = True
    except Exception as ex:
        message = "".join(
            traceback.format_exception(etype=type(ex), value=ex, tb=ex.__traceback__)
        )
        success = False
        logging.error(message)
    finally:
        containerutils.update_status(ANALYSIS_METHOD, success, message)