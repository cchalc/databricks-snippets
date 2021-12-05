dbs = spark.sql("show databases like 'christopher_chalcraft*'")

dbs_list = (dbs.select('databaseName').
            rdd.flatMap(lambda x: x).collect()
           )

for db in dbs_list:
    spark.sql(f"DROP DATABASE IF EXISTS {db} CASCADE;")
