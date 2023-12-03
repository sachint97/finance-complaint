import yaml,os,sys
from finance_complaint.exception import FinanceException


def write_yaml_file(file_path: str, data: dict = None):
    """
    Creates a yaml file and writes data into it
    """
    try:
        os.makedirs(os.path.dirname(file_path), exist_ok=True)
        with open(file_path, "w") as yaml_file:
            if data is not None:
                yaml.dump(data,yaml_file)
    except Exception as e:
        raise FinanceException(e, sys)

def read_yaml_file(file_path:str) -> dict:
    """
    Reads a YAML file and returns the content as dictionary.
    file_path:str
    """
    try:
        with open(file_path, "r") as yaml_file:
            return yaml.safe_load(yaml_file)
    except Exception as e:
        raise FinanceException(e, sys)