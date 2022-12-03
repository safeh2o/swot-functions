from ..utils.swotutils import AnalysisUtils


def main(msg: dict) -> str:
    controller = AnalysisUtils(
        msg["AZURE_STORAGE_KEY"],
        msg["MONGODB_CONNECTION_STRING"],
        msg["DATASET_ID"],
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
    controller.postprocess()

    return "Done postprocessing"
