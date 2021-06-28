# Databricks notebook source
dbutils.widgets.text("team_name", "");

# COMMAND ----------

# import to enable removal on non-alphanumeric characters (re stands for regular expressions)
import re

# Get the email address entered by the user on the calling notebook
team_name = dbutils.widgets.get("team_name")
print(f"Data entered in team_name field: {team_name}")

# Strip out special characters and make it lower case
clean_team_name = re.sub('[^A-Za-z0-9]+', '', team_name).lower();
print(f"Team Name with special characters removed: {clean_team_name}");

# Construct the unique path to be used to store files on the local file system
local_data_path = f"{clean_team_name}/"
print(f"Path to be used for Local Files: {local_data_path}")

# Construct the unique path to be used to store files on the DBFS file system
dbfs_data_path = f"{clean_team_name}/"
print(f"Path to be used for DBFS Files: {dbfs_data_path}")

# Construct the unique database name
database_name = f"{clean_team_name}"
print(f"Database Name: {database_name}")

# COMMAND ----------

def useDatabase(databaseName):
# databaseName = "cchalc"
  spark.sql(f"DROP DATABASE IF EXISTS {databaseName} CASCADE")
  spark.sql(f"CREATE DATABASE {databaseName}")
  spark.sql(f"USE {databaseName}")
  
useDatabase(f"{database_name}")

# COMMAND ----------

# Enables running shell commands from Python
# I need to do this, rather than just the %sh magic command, because I want to pass in python variables

import subprocess

# COMMAND ----------

# Delete local directories that may be present from a previous run

process = subprocess.Popen(['rm', '-f', '-r', clean_team_name],
                     stdout=subprocess.PIPE, 
                     stderr=subprocess.PIPE)
stdout, stderr = process.communicate()

stdout.decode('utf-8'), stderr.decode('utf-8')

# COMMAND ----------

# Create local directories used in the workshop

process = subprocess.Popen(['mkdir', '-p', clean_team_name],
                     stdout=subprocess.PIPE, 
                     stderr=subprocess.PIPE)
stdout, stderr = process.communicate()

stdout.decode('utf-8'), stderr.decode('utf-8')

# COMMAND ----------

