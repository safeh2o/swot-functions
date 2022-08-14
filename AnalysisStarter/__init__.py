import logging

import azure.functions as func
import azure.durable_functions as df


async def main(msg: func.QueueMessage, starter: str) -> func.HttpResponse:
    client = df.DurableOrchestrationClient(starter)
    msg_json = msg.get_json()
    instance_id = await client.start_new("AnalysisOrchestrator", None, msg_json)

    logging.info(f"Started orchestration with ID = '{instance_id}'.")
