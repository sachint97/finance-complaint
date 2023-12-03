import os

PIPELINE_NAME = "finance-complaint"
PIPELINE_ARTIFACT_DIR = os.path.join(os.getcwd(), "finance_artifact")

from finance_complaint.constants.training_pipeline_constants.data_ingestion_constants import *
