# Databricks notebook source
from pyspark.sql.functions import explode, split, col
data = [
    ("John", "python, sql"),
    ("Aravind", "Java,SQL,HTML"),
    ("Sridevi", "Python,sql,pyspark")
]
raw_df = spark.createDataFrame(data,["Name","Skills"])
df = raw_df.withColumn("Skills",explode(split(col("Skills"),",")))
df.display()

# COMMAND ----------

from pyspark.sql.functions import concat, coalesce, col, lit
from pyspark.sql.types import StructType, StringType, StructField
schema = StructType([
    StructField("Name1", StringType(), True),
    StructField("Name2", StringType(), True),
    StructField("Name3", StringType(), True)
])
data = [("Aravind", None, None), ("John", None, None), (None, "Sridevi", None)]
raw_df = spark.createDataFrame(data,schema = schema)

df = raw_df.select(
        coalesce(col("Name1"),col("Name2"),col("Name3")).alias("Names"))
df.display()

# COMMAND ----------

from pyspark.sql.functions import avg,when

data1 = [ (101, "Aravind"), (102, "John"), (103, "Sridevi") ]
data2 = [ ("pyspark", 90, 101), ("sql", 70, 101), ("pyspark", 70, 102), ("sql", 60, 102), ("sql", 30, 103), ("pyspark", 20, 103) ]

df1 = spark.createDataFrame(data1,["student_id","student_name"])
df2 = spark.createDataFrame(data2,["course_name","Marks","student_id"])

res_df2 = df2.groupBy("student_id").agg(avg("Marks").alias("percentage"))
res_df = res_df2.withColumn("Result",when(res_df2.percentage >= 70,'Distinction')
                              .when(res_df2.percentage >= 60,'First Class')
                              .when(res_df2.percentage >= 50,'Second Class')
                              .when(res_df2.percentage >= 40,'Third Class')
                              .when(res_df2.percentage <= 39,'Fail'))

joined_df = df1.join(res_df,df1.student_id == res_df.student_id,how='inner').drop(res_df.student_id)
joined_df.display()
                            

# COMMAND ----------


