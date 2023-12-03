import os, sys
from finance_complaint.exception import FinanceException
from dataclasses import dataclass
from finance_complaint.utils import write_yaml_file, read_yaml_file
from finance_complaint.logger import logger

@dataclass
class DataIngestionMetaDataInfo:
    from_date:str
    to_date:str
    data_file_path:str

class DataIngestionMetadata:

    def __init__(self, metadata_file_path):
        self.metadata_file_path = metadata_file_path

    @property
    def is_metadata_file_present(self):
        return os.path.exists(self.metadata_file_path)

    def write_metadata_info(self, from_date:str, to_date:str, data_file_path:str):
        try:
            metadata_info = DataIngestionMetaDataInfo(
                from_date=from_date,
                to_date=to_date,
                data_file_path=data_file_path
            )

            write_yaml_file(file_path=self.metadata_file_path,
                            data = metadata_info.__dict__)
        except Exception as e:
            raise FinanceException(e, sys)

    def get_metadata_info(self) -> DataIngestionMetaDataInfo:
        try:
            if self.is_metadata_file_present:
                metadata = read_yaml_file(self.metadata_file_path)
                metadata_info = DataIngestionMetaDataInfo(**(metadata))
                logger.info(metadata)
                return metadata_info
            else:
                raise Exception("No meta data file available")
        except Exception as e:
            raise FinanceException(e, sys)