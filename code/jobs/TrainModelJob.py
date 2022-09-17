# ###########################################################################
#
#  CLOUDERA APPLIED MACHINE LEARNING PROTOTYPE (AMP)
#  (C) Cloudera, Inc. 2022
#  All rights reserved.
#
#  Applicable Open Source License: Apache 2.0
#
#  NOTE: Cloudera open source products are modular software products
#  made up of hundreds of individual components, each of which was
#  individually copyrighted.  Each Cloudera open source product is a
#  collective work under U.S. Copyright Law. Your license to use the
#  collective work is as provided in your written agreement with
#  Cloudera.  Used apart from the collective work, this file is
#  licensed for your use pursuant to the open source license
#  identified above.
#
#  This code is provided to you pursuant a written agreement with
#  (i) Cloudera, Inc. or (ii) a third-party authorized to distribute
#  this code. If you do not have a written agreement with Cloudera nor
#  with an authorized and properly licensed third party, you do not
#  have any rights to access nor to use this code.
#
#  Absent a written agreement with Cloudera, Inc. (“Cloudera”) to the
#  contrary, A) CLOUDERA PROVIDES THIS CODE TO YOU WITHOUT WARRANTIES OF ANY
#  KIND; (B) CLOUDERA DISCLAIMS ANY AND ALL EXPRESS AND IMPLIED
#  WARRANTIES WITH RESPECT TO THIS CODE, INCLUDING BUT NOT LIMITED TO
#  IMPLIED WARRANTIES OF TITLE, NON-INFRINGEMENT, MERCHANTABILITY AND
#  FITNESS FOR A PARTICULAR PURPOSE; (C) CLOUDERA IS NOT LIABLE TO YOU,
#  AND WILL NOT DEFEND, INDEMNIFY, NOR HOLD YOU HARMLESS FOR ANY CLAIMS
#  ARISING FROM OR RELATED TO THE CODE; AND (D)WITH RESPECT TO YOUR EXERCISE
#  OF ANY RIGHTS GRANTED TO YOU FOR THE CODE, CLOUDERA IS NOT LIABLE FOR ANY
#  DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, PUNITIVE OR
#  CONSEQUENTIAL DAMAGES INCLUDING, BUT NOT LIMITED TO, DAMAGES
#  RELATED TO LOST REVENUE, LOST PROFITS, LOSS OF INCOME, LOSS OF
#  BUSINESS ADVANTAGE OR UNAVAILABILITY, OR LOSS OR CORRUPTION OF
#  DATA.
#
# ###########################################################################

!pip3 install -r requirements.txt

import seaborn as sns
import pandas as pd
import numpy as np
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
import matplotlib.pyplot as plt
from pyspark.ml.pipeline import PipelineModel
from sklearn.metrics import classification_report, confusion_matrix

spark = SparkSession.builder\
  .appName("1.1 - Retrain Model") \
  .config("spark.kerberos.access.hadoopFileSystems", os.environ["STORAGE"])\
  .getOrCreate()

#Explore putting GE here
df = spark.sql("SELECT * FROM default.batch_load_table") #mlops_batch_load_table

#Put open source model registry call here
mPath = os.environ["STORAGE"]+"/datalake/pdefusco/pipeline"
persistedModel = PipelineModel.load(mPath)

###Data Pipeline###

#Target Feature
df = df.withColumn("label", when((df["loan_status"] == "Charged Off")|(df["loan_status"] == "Default"), 1).otherwise(0))

#Data Transformations:

df = df.select(['acc_now_delinq', 'acc_open_past_24mths', 'addr_state', 'annual_inc', 'avg_cur_bal', 'funded_amnt', 'label'])

df = df.na.drop(subset=["addr_state"])

li=["TX","CA","FL", "CO"]
df = df.filter(df.addr_state.isin(li))

df = df.fillna(0)

#Checking Nulls
from pyspark.sql.functions import col,isnan, when, count
df.select([count(when(isnan(c) | col(c).isNull(), c)).alias(c) for c in df.columns]).show()

#Data Type Transformations
df = df.withColumn("acc_now_delinq", df["acc_now_delinq"].cast(FloatType()))
df = df.withColumn("acc_open_past_24mths", df["acc_open_past_24mths"].cast(FloatType()))
df = df.withColumn("annual_inc", df["annual_inc"].cast(FloatType()))
df = df.withColumn("avg_cur_bal", df["avg_cur_bal"].cast(FloatType()))
df = df.withColumn("funded_amnt", df["funded_amnt"].cast(FloatType()))
df = df.withColumn("label", df["label"].cast(FloatType()))

###Machine Learning Pipeline###
df_model = persistedModel.transform(df)

#Model Evaluation
trainingSummary = pipelineModel.stages[-1].summary

accuracy = trainingSummary.accuracy
falsePositiveRate = trainingSummary.weightedFalsePositiveRate
truePositiveRate = trainingSummary.weightedTruePositiveRate
fMeasure = trainingSummary.weightedFMeasure()
precision = trainingSummary.weightedPrecision
recall = trainingSummary.weightedRecall
print("Accuracy: %s\nFPR: %s\nTPR: %s\nF-measure: %s\nPrecision: %s\nRecall: %s"
      % (accuracy, falsePositiveRate, truePositiveRate, fMeasure, precision, recall))

get_confusion_matrix(df_model)

#try: 
#    df_model.writeTo("spark_catalog.default.mlops_staging_scores_table").create()
#    df_model.writeTo("spark_catalog.default.mlops_scores_table").create()
#except:
#    spark.sql("INSERT INTO spark_catalog.default.mlops_scores_table SELECT * FROM spark_catalog.default.mlops_staging_scores_table").show()
#else:
#    spark.sql("INSERT INTO spark_catalog.default.mlops_scores_table SELECT * FROM spark_catalog.default.mlops_staging_scores_table").show()

#spark.sql("DROP TABLE IF EXISTS spark_catalog.default.mlops_staging_scores_table")