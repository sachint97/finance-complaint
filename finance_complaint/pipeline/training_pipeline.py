from finance_complaint.configs.training_pipeline_config import FinanceConfig
from finance_complaint.exception import FinanceException
from finance_complaint.logger import logger
from finance_complaint.components.training_components.data_ingestion import DataIngestion
import sys

class TrainingPipeline:
    def __init__(self, finance_config):
        self.finance_config : FinanceConfig = finance_config

    def start_data_ingestion(self):
        try:
            data_ingestion_config = self.finance_config.get_data_ingestion_config(to_date="2012-06-01")
            data_ingestion = DataIngestion(data_ingestion_config=data_ingestion_config)
            data_ingestion_artifact = data_ingestion.initiate_data_ingestion()
            return data_ingestion_artifact

        except Exception as e:
            raise FinanceException(e, sys)

    def start_data_validation(self):
        pass

    def start_data_transformation(self):
        pass

    def start_model_trainer(self):
        pass

    def start_model_evaluvation(self):
        pass

    def start_model_pusher(self):
        pass

    def start(self):
        data_ingestion_artifact = self.start_data_ingestion()
        logger.info(f"Data Ingestion completed, artifact generated: {data_ingestion_artifact}")
