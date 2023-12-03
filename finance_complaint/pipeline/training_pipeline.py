from finance_complaint.configs.training_pipeline_config import FinanceConfig
from finance_complaint.exception import FinanceException
from finance_complaint.logger import logger
import sys

class TraningPipeline:
    def __init__(self, finance_config):
        self.finance_config : FinanceConfig = finance_config

    def start_data_ingestion(self) :
        try:
            data_ingestion_config = self.finance_config.get_data_ingestion_config()

        except Exception as e:
            raise FinanceException(e, sys)

    def start_data_validation(self):
        pass

    def start_data_transformation(self):
        pass

    def start_model_trainer():
        pass

    def start_model_evaluvation():
        pass

    def start_model_pusher():
        pass

    def start():
        pass