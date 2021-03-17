#!/usr/bin/env python
# coding: utf-8

# In[1]:


#Import library 
import pyspark
import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
import urllib.request
get_ipython().run_line_magic('matplotlib', 'inline')
from pyspark import SparkContext
from pyspark.sql import SQLContext
from pyspark.sql import Row
from pyspark import SparkFiles
from pyspark.sql.types import *
from pyspark.sql import SparkSession

#Establish connexions 
try:
    sc = SparkContext('local', 'Pyspark test1')
except ValueError:
    print('SparkContext already exists!')
try:
    spark = SparkSession.builder.appName('Customers Scoring').getOrCreate()
except ValueError:
    print('SparkSession already exists!')
    


# In[31]:


#sc.stop()


# In[56]:


# Load CSV file,rename & format fields

filePath=(r'C:\Users\yaitali\Desktop\score\Scoring\import\client_10.csv')

schema = StructType()       .add("SIREN",StringType(),True)       .add("SIRET",StringType(),True)       .add("Acc_type_groupe",StringType(),True)       .add("Ind_level_2",StringType(),True)       .add("Customer_size",StringType(),True)       .add("ID_customer_size",StringType(),True)       .add("Organisation_type",StringType(),True)       .add("ID_CUSTOMER",StringType(),True)       .add("Name_CUSTOMER",StringType(),True)       .add("STREET",StringType(),True)       .add("Code_postal",IntegerType(),True)       .add("City",StringType(),True)       .add("Departement",StringType(),True)       .add("Code_dep",DoubleType(),True)       .add("Region",StringType(),True)       .add("Code_reg",IntegerType(),True)       .add("Flag_active",StringType(),True)       .add("Sales_channel",StringType(),True)       .add("Date_comd",StringType(),True)       .add("Sales_Orders_ID",IntegerType(),True)      .add("GMC_1_ID",StringType(),True)       .add("GMC_1_Desc",StringType(),True)       .add("GMC_2_ID",StringType(),True)       .add("GMC_2_Desc",StringType(),True)       .add("GMC_4_ID",StringType(),True)       .add("GMC_4_Desc",StringType(),True)       .add("Raison_reclamation",StringType(),True)       .add("Nb_reclamation",IntegerType(),True)       .add("Montant_net",DoubleType(),True)       .add("Nb_products",IntegerType(),True) 

df = spark.read.options(header=True,multiLine=True,ignoreLeadingWhiteSpace=True,ignoreTrailingWhiteSpace=True,encoding="UTF-8",sep=';',inferSchema=True)       .schema(schema)       .csv(filePath)
df.printSchema()

#df = spark.read.options(header=True,multiLine=True,ignoreLeadingWhiteSpace=True,ignoreTrailingWhiteSpace=True,encoding="UTF-8",sep=';',inferSchema=True)\
#.csv(r'C:\Users\yaitali\Desktop\score\Scoring\import\client_10.csv')
#df.printSchema()



# In[57]:


#Format Date Commande (String to DateTimes)
df.withColumn("Date_comd",df['Date_comd'].cast(TimestampType()))
df.select('Date_comd').show(100)


# In[65]:


# Handle duplicates data

# Method 1: Distinct()
distinctDF1 = df.distinct()
print("Distinct count: "+str(distinctDF1.count()))
distinctDF1.show(truncate=False)

# Method 2: DropDuplicate()
#distinctDF2 = df.dropDuplicates()
#print("Distinct count: "+str(distinctDF2.count()))
#distinctDF2.show(truncate=False)


# In[73]:


data= distinctDF1.select('Sales_Orders_ID','GMC_4_ID','SIREN','Date_comd','SIRET','Montant_net','Customer_size').show(10)


# In[69]:


#Replace NULL Values with Zero (0)
distinctDF1.na.fill(value=0,subset=["Montant_net","Nb_reclamation","Nb_products"]).show()

#Replace Null Values with Empty String
distinctDF1.na.fill({"Customer_size": "Unknown"})     .show()

#distinctDF1.na.fill("").show()


# In[71]:


#Replace Null Values with Empty String
distinctDF1.na.fill({"Customer_size": "Unknown"})     .show()


# In[ ]:



# using SQLContext to read parquet file/PySpark

from pyspark.sql import SQLContext

sqlContext = SQLContext(sc)

# to read parquet file

df = sqlContext.read.parquet('path-to-file/commentClusters.parquet')


# In[ ]:


# Write & Read Text file from HDFS/ Scala
(/RDD)
val rddFromFile = spark.sparkContext.textFile("hdfs://nn1home:8020/text01.txt")
val rddWhole = spark.sparkContext.wholeTextFiles("hdfs://nn1home:8020/text01.txt")
(/DS)
val df:DataFrame = spark.read.text("hdfs://nn1home:8020/text01.txt")
val ds:Dataset[String] = spark.read.textFile("hdfs://nn1home:8020/text01.txt")


# In[ ]:


# Write & Read CSV & TSV file from HDFS/ Scala
spark.read.csv("hdfs://nn1home:8020/file.csv")
df2.write.option("header","true").csv("hdfs://nn1home:8020/csvfile")


# In[35]:


# Write & Read Parquet file from HDFS/ Scala
val parqDF = spark.read.parquet("hdfs://nn1home:8020/people.parquet")
df.write.parquet("hdfs://nn1home:8020/parquetFile")


# In[ ]:





# In[ ]:





# In[ ]:





# In[ ]:





# In[ ]:




