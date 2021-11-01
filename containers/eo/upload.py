import os, glob, subprocess, json
import utils
import traceback

ANALYSIS_METHOD = utils.AnalysisMethod.EO

utils.set_logger(ANALYSIS_METHOD)


def process_queue():
    input_filename = utils.download_src_blob()
    confidence_level = os.getenv("CONFIDENCE_LEVEL", "optimumDecay")
    max_duration = os.getenv("MAX_DURATION", 3)
    dataset_id = os.getenv("DATASET_ID", None)

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

    utils.update_dataset({"eo": result_dict})
    utils.upload_files(output_files)


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
    finally:
        utils.update_status(ANALYSIS_METHOD, success, message)
