import glob
import json
import logging
import os
import subprocess
import traceback

import containerutils
from utils.logging import set_logger

ANALYSIS_METHOD = containerutils.AnalysisMethod.EO


def process_queue():
    input_filename = containerutils.download_src_blob()
    confidence_level = os.getenv("CONFIDENCE_LEVEL", "optimumDecay")
    max_duration = os.getenv("MAX_DURATION", 3)
    dataset_id = os.getenv("DATASET_ID", None)
    set_logger(f"{dataset_id}-{ANALYSIS_METHOD}")

    # run swot analysis on downloaded blob
    out_dir = dataset_id
    os.mkdir(out_dir)

    subprocess.run(
        [
            "octave-cli",
            "--eval",
            f"engmodel {input_filename} {out_dir} {confidence_level} {max_duration}",
        ]
    )

    result_files = os.listdir(out_dir)

    output_files = [os.path.join(out_dir, x) for x in result_files]

    result_dict = {}

    # get all json files and store in dict
    for json_file in glob.glob(out_dir + os.path.sep + "*.json"):
        with open(json_file, "r") as json_fp:
            result_dict.update(json.load(json_fp))

    containerutils.update_dataset({"eo": result_dict})
    containerutils.upload_files(output_files)


if __name__ == "__main__":
    try:
        message = "OK"
        success = True
        process_queue()
    except Exception as ex:
        message = "".join(
            traceback.format_exception(etype=type(ex), value=ex, tb=ex.__traceback__)
        )
        logging.error(message)
        success = False
    finally:
        containerutils.update_status(ANALYSIS_METHOD, success, message)
