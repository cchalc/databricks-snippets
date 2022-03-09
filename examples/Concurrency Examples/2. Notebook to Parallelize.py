# Databricks notebook source
import time

dbutils.widgets.text('tagName', '')

try:
  # Some Spark Code
  df = spark.range(10)
  df.show()
  
  # Get Tag Name from parameter
  tag_name = dbutils.widgets.get('tagName')
  print(f"This is the tag name: {tag_name}")
  
  # Sleep for 30 seconds
  #time.sleep(30)

  dbutils.notebook.exit("Success")
except Exception as e: 
  print(e) 

# COMMAND ----------


