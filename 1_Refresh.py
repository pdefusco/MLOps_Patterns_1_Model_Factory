### Creating Base Table
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import *

#create version without iceberg extension options for CDE
spark = SparkSession.builder\
  .appName("0.2 - Batch Load") \
  .config("spark.kerberos.access.hadoopFileSystems", os.environ["STORAGE"])\
  .getOrCreate()
  
print(os.environ["STORAGE"])

#Explore putting GE here to unit test column types
try:
    # Load and parse the data file, converting it to a DataFrame.
    df = spark.read.csv(os.environ["STORAGE"]+'/datalake/model_factory/LoanStats_2015_subset_091322.csv',   
        header=True,
        sep=',',
        nullValue='NA')
    
    df = df.limit(2000)
    
    #Creating table for batch load if not present
    df.writeTo("default.mlops_batch_load_table").create()
    
    
except:
    sparkDF = spark.sql("SELECT * FROM default.batch_load_table")

else:
    sparkDF = spark.sql("SELECT * FROM default.batch_load_table")
    
print("Total row count in the target table before batch load")
sparkDF.count()

newBatchDF = sparkDF.sample(withReplacement=True, fraction=0.5)

newBatchDF.count()

#spark.sql("DROP TABLE IF EXISTS spark_catalog.default.mlops_staging_table")

#Explore putting GE here to unit test column types
try:
    newBatchDF.writeTo("default.batch_load_table").create()
except:
    spark.sql("INSERT INTO batch_load_table SELECT * FROM default.batch_load_table").show()
else:
    spark.sql("INSERT INTO batch_load_table SELECT * FROM default.batch_load_table").show()
