# Databricks notebook source
from pyspark.sql.functions import *
from pyspark.sql.types import *
 
# Convenience function for turning JSON strings into DataFrames.
def jsonToDataFrame(json, schema=None):
  # SparkSessions are available with Spark 2.0+
  reader = spark.read
  if schema:
    reader.schema(schema)
  return reader.json(sc.parallelize([json]))

# COMMAND ----------

cluster_policy_string = """
{
  "num_workers": {
    "type": "fixed",
    "value": 1
  },
  "autotermination_minutes": {
    "type": "range",
    "maxValue": 120,
    "defaultValue": 60
  },
  "node_type_id": {
    "type": "allowlist",
    "values": [
      "Standard_DS3_v2",
      "Standard_DS4_v2"
    ],
    "defaultValue": "Standard_DS3_v2"
  },
  "driver_node_type_id": {
    "type": "allowlist",
    "values": [
      "Standard_DS3_v2",
      "Standard_DS4_v2"
    ]
  },
  "spark_version": {
    "type": "fixed",
    "value": "9.1.x-scala2.12",
    "hidden": true
  },
  "custom_tags.team": {
    "type": "fixed",
    "value": "product"
  }
}
"""

# COMMAND ----------

df = jsonToDataFrame(cluster_policy_string)

# COMMAND ----------

display(df)

# COMMAND ----------

display(df.select(col("`custom_tags.team`").getItem("value")))

# COMMAND ----------

new_cols=(column.replace('.', '_') for column in df.columns)
df2 = df.toDF(*new_cols)
print(df2.columns)

# COMMAND ----------

df = (spark
     .createDataFrame([(1, cluster_policy_string)],["id","json_data"])
     )

# COMMAND ----------

# df.show(truncate=False)
display(df)

# COMMAND ----------

df.createTempView("policy_check")

# COMMAND ----------
