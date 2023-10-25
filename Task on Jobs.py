# Databricks notebook source
input_file_path_csv="dbfs:/mnt/sanly/input/task/csv"
input_file_path_json="dbfs:/mnt/sanly/input/task/json"

# COMMAND ----------

output_file_path="dbfs:/mnt/sanly/input/taskouput"

# COMMAND ----------

# MAGIC %sql
# MAGIC create schema if not exists task;
# MAGIC use task;

# COMMAND ----------

(spark.readStream
.format("cloudFiles")
.option("cloudFiles.format", "csv")
.option("cloudFiles.schemaLocation",f"{input_file_path_csv}Manan/logs")
.option("cloudFiles.inferColumnTypes","True")
.load(f"{input_file_path_csv}")
.createOrReplaceTempView("empdata"))

# COMMAND ----------

# MAGIC %sql
# MAGIC create or replace temp view emp_final as
# MAGIC select *, current_timestamp() as ingestionTime, input_file_name() as fileName from empdata

# COMMAND ----------

(spark.table("emp_final")
.writeStream
.option("checkpointLocation",f"{output_file_path}/Manan/logs/checkpoint/csv/")
.option("mergeSchema",True)
.trigger(once=True)
.table("task.emp_bronze")
)

# COMMAND ----------


