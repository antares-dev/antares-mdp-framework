# Databricks notebook source
# MAGIC %run ../../Common/common-workspace

# COMMAND ----------

def RunNotebookViews():
    for i in ListCurrentNotebooks():
        if i == CurrentNotebookPath():
            continue
        dbutils.notebook.run(i, 0, {})

# COMMAND ----------

def RowCounts():
    sql = ' UNION '.join(f"SELECT '{i.tableName}' TableName, COUNT(*) Counts FROM curated.{i.tableName}" for i in spark.sql("SHOW TABLES FROM curated LIKE 'd_*'").rdd.collect())
    display(spark.sql(x))

# COMMAND ----------

def UnionFactViews():
    list = spark.sql("SHOW DATABASES LIKE 'history_20*'").rdd.collect()
    for i in spark.sql("SHOW TABLES FROM curated LIKE 'f_*' ").rdd.collect():
        tableName = i.tableName

        if(tableName in ["f_factlesssalesperioddate", "f_organisationcontractassociated", "f_organisationpayment", "f_payments", "f_test"] ):
            continue
        
        if(#'organisation' in tableName or 
           'manual' in tableName):
            continue
        columns = ",".join(spark.table(f"curated.{tableName}").columns[2:])
        whereClause = "WHERE LEFT(DateKey, 4) = 2022" if "DateKey" in columns else ""
        whereClause = ""
        
        sql = f"CREATE OR REPLACE VIEW curated.v{tableName} AS SELECT {columns} FROM curated.{tableName} {whereClause} UNION " + " UNION ".join([f"SELECT {columns} FROM {i.databaseName}.{tableName} {whereClause}" for i in list])
        #MOVING ONE OF THE SNAPSHOTS
        sql = sql.replace("_Created,_Ended,_Current FROM history_20211226.f_revenue", "CAST('2022-04-25 00:00:00' AS TIMESTAMP) _Created, CAST('2022-05-01 00:00:00' AS TIMESTAMP) _Ended,_Current FROM history_20211226.f_revenue")

        print(sql)
        #spark.sql(sql)
        #break
UnionFactViews()

# COMMAND ----------

def UnionDimensionViews():
    list = spark.sql("SHOW DATABASES LIKE 'history*'").rdd.collect()
    for i in spark.sql("SHOW TABLES FROM curated LIKE 'd_*' ").rdd.collect():
        tableName = i.tableName
        if (tableName in ["d_organisation", "d_fulldatecalendar", "d_asatdate", "d_exchangerate", "d_date", "f_factlesssalesperioddate", "d_factbatch", "d_rebate"]):
            continue
            
        sql = f"CREATE OR REPLACE VIEW curated.v{tableName} AS SELECT * FROM curated.{tableName} UNION " + " UNION ".join([f"SELECT * FROM {i.databaseName}.{tableName}" for i in list])
        print(sql)
        #spark.sql(sql)
        #break
#UnionDimensionViews()

# COMMAND ----------

def UpdateDates():
    for i in spark.sql("SHOW TABLES FROM history LIKE 'f_*' ").rdd.collect():
        tableName = i.tableName
        sql = f"UPDATE history.{tableName} SET _Created = '2021-12-19', _Ended = '2021-12-26'"
        
        try:
            ended = spark.sql(f"SELECT _Ended FROM history.{tableName} LIMIT 1").select("_Ended").rdd.collect()[0]._Ended
        except:
            pass
        if (str(ended) == '2021-12-26 00:00:00'):
            continue
        
        try:
            spark.sql(sql)
            print(sql)
        except:
            pass

# COMMAND ----------

def DistinctSCD():
    db = "history2"
    sql = ' UNION '.join(f"SELECT '{i.tableName}' TableName, _Created, COUNT(*) Counts FROM {db}.{i.tableName} GROUP BY _Created" for i in spark.sql(f"SHOW TABLES FROM {db}").rdd.collect())
    #print(sql)
    display(spark.sql(sql))

# COMMAND ----------

