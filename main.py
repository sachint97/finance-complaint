from finance_complaint.exception import FinanceException
from finance_complaint.logger import logger
from finance_complaint.pipeline.training_pipeline import TraningPipeline
import sys,argparse

def start_training(start=False):
    try:
        if start:
            TraningPipeline().start()
    except Exception as e:
        raise FinanceException(e, sys)

def start_prediction(start=False):
    pass

def main(training_status,prediction_status):
    try:
        start_training(training_status)
        start_prediction(prediction_status)
    except Exception as e:
        raise FinanceException(e, sys)

if __name__ == "__main__":
    try:
        parser = argparse.ArgumentParser()
        parser.add_argument("--t", default=0, type=int, help="If provided training process will start else not.")
        parser.add_argument("--p", default=0, type=int, help="If provided training process will start else not.")

        args = parser.parse_args()

        main(training_status=args.t,prediction_status=args.p)
    except Exception as e:
        logger.exception(e)