#
#Copyright (c) 2022 Cloudera, Inc. All rights reserved.
#

!pip3 install -r requirements.txt

# Create the directories and upload data
from cmlbootstrap import CMLBootstrap
from IPython.display import Javascript, HTML
import os
import time
import json
import requests
import xml.etree.ElementTree as ET
import datetime
import subprocess

run_time_suffix = datetime.datetime.now()
run_time_suffix = run_time_suffix.strftime("%d%m%Y%H%M%S")

# Set the setup variables needed by CMLBootstrap
HOST = os.getenv("CDSW_API_URL").split(":")[0] + "://" + os.getenv("CDSW_DOMAIN")
USERNAME = os.getenv("CDSW_PROJECT_URL").split("/")[6]  # args.username  # "vdibia"
API_KEY = os.getenv("CDSW_API_KEY")
PROJECT_NAME = os.getenv("CDSW_PROJECT")

# Instantiate API Wrapper
cml = CMLBootstrap(HOST, USERNAME, API_KEY, PROJECT_NAME)

# Set the STORAGE environment variable
try:
    storage = os.environ["STORAGE"]
except:
    if os.path.exists("/etc/hadoop/conf/hive-site.xml"):
        tree = ET.parse("/etc/hadoop/conf/hive-site.xml")
        root = tree.getroot()
        for prop in root.findall("property"):
            if prop.find("name").text == "hive.metastore.warehouse.dir":
                storage = (
                    prop.find("value").text.split("/")[0]
                    + "//"
                    + prop.find("value").text.split("/")[2]
                )
    else:
        storage = "/user/" + os.getenv("HADOOP_USER_NAME")
    storage_environment_params = {"STORAGE": storage}
    storage_environment = cml.create_environment_variable(storage_environment_params)
    os.environ["STORAGE"] = storage

### Loading Data to Cloud Storage

!hdfs dfs -mkdir -p $STORAGE/datalake/model_factory
!hdfs dfs -copyFromLocal /home/cdsw/data/LoanStats_2015_subset_091322.csv $STORAGE/datalake/model_factory/LoanStats_2015_subset_091322.csv
!hdfs dfs -ls $STORAGE/datalake/model_factory
