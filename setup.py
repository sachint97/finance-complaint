from setuptools import setup,find_packages
from typing import List
import chardet # encoding detector


def get_packages_list(path)->List[str]:
    """This function returns the required packages in list format."""
    requirements_list:List[str]=[]
    # detect encoding of file
    with open(path,mode="rb") as file:
        file_type=chardet.detect(file.read())

    # read file and append it to list
    with open(path, mode="r",encoding=file_type["encoding"]) as requirements:
        for requirement in requirements.readlines():
            requirements_list.append(requirement.split('\n')[0])
        requirements_list.remove("-e .")
    return requirements_list

setup(name="finance-complaint",
      version="0.0.1",
      description="A ML project to classify finance complaints",
      author="sachin",
      author_email="cssachinshoba@gmail.com",
      packages=find_packages(),
      install_requires=get_packages_list(path="requirements.txt")
      )
