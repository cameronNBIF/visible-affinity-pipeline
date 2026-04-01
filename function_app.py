import logging
import azure.functions as func
from main import main

app = func.FunctionApp()

@app.timer_trigger(schedule="0 0 8 * * *", arg_name="myTimer", run_on_startup=False,
              use_monitor=False) 
def visible_affinity_pipeline(myTimer: func.TimerRequest) -> None:
    if myTimer.past_due:
        logging.info('The timer is past due!')

    try:
        main()
    except Exception as e:
        logging.error(f"An error occurred during pipeline execution: {e}")

    logging.info('Python timer trigger function executed.')