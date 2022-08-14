import azure.durable_functions as df


def orchestrator_function(context: df.DurableOrchestrationContext):
    msg = context.get_input()
    analysis_parameters = yield context.call_activity("AnalysisPrep", msg)

    analysis_tasks = [
        context.call_activity("AnnTrigger", analysis_parameters),
        context.call_activity("EoTrigger", analysis_parameters),
    ]

    yield context.task_all(analysis_tasks)

    yield context.call_activity("AnalysisPostprocess", analysis_parameters)
    return "Done both functions!"


main = df.Orchestrator.create(orchestrator_function)
