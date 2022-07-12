# Databricks notebook source
# MAGIC %run ../../Common/common-transform

# COMMAND ----------

BATCH_END_CODE = "000000000000"
DATE_FORMAT = "yyMMddHHmmss"

# COMMAND ----------

# DBTITLE 1,Revised Methods
def _InjectSK(dataFrame):
    skName = f"{_.Name}_SK"
    df = dataFrame
    df = df.withColumn(skName, expr(f"XXHASH64(CONCAT(date_format(now(), '{DATE_FORMAT}'), '-', {_.Name}_BK), 512)"))
    #df = df.withColumn(skName, expr(f"XXHASH64({_.Name}_BK, 512) || DATE_FORMAT(NOW(), '{DATE_FORMAT}')"))
    #df = df.withColumn(skName, expr(f"CAST((XXHASH64({_.Name}_BK, 512) || DATE_FORMAT(NOW(), '{DATE_FORMAT}')) AS BIGINT)"))
    #df = df.withColumn(skName, expr(f"CAST(LEFT({_.Name}_SK, 32) AS BIGINT)"))
    columns = [c for c in dataFrame.columns]
    columns.insert(0, skName)
    df = df.select(columns)

    return df

# COMMAND ----------

def _AddSCD(dataFrame):
    df = dataFrame
    df = df.withColumn("_Created", expr(f"CAST({DEFAULT_START_DATE} AS TIMESTAMP)"))
    df = df.withColumn("_Ended", expr("CAST(NULL AS TIMESTAMP)" if DEFAULT_END_DATE == "NULL" else f"CAST(({DEFAULT_END_DATE} + INTERVAL 1 DAY) - INTERVAL 1 SECOND AS TIMESTAMP)"))
    df = df.withColumn("_Current", expr("CAST(1 AS INT)"))
    df = df.withColumn("_Batch_SK", expr(f"DATE_FORMAT(_Created, '{DATE_FORMAT}') || COALESCE(DATE_FORMAT(_Ended, '{DATE_FORMAT}'), '{BATCH_END_CODE}') || _Current"))
    #THIS IS LARGER THAN BIGINT 
    df = df.withColumn("_Batch_SK", expr("CAST(_Batch_SK AS DECIMAL(25, 0))"))

    return df

# COMMAND ----------

def Save(sourceDataFrame):
    targetTableFqn = f"{_.Destination}"
    print(f"Saving {targetTableFqn}...")
    sourceDataFrame = _WrapSystemColumns(sourceDataFrame) if sourceDataFrame is not None else None

    if (not(TableExists(targetTableFqn))):
        print(f"Creating {targetTableFqn}...")
        CreateDeltaTable(sourceDataFrame, targetTableFqn, _.DataLakePath)  
        #EndNotebook(sourceDataFrame)
        return

    targetTable = spark.table(targetTableFqn)
    _exclude = {f"{_.Name}_SK", f"{_.Name}_BK", '_Created', '_Ended', '_Current', "_Batch_SK"}
    changeColumns = " OR ".join([f"s.{c} <> t.{c}" for c in targetTable.columns if c not in _exclude])
    bkList = "','".join([str(c.Test_BK) for c in spark.table("curated.f_test").select("Test_BK").rdd.collect()])

    newRecords = sourceDataFrame.where(f"{_.Name}_BK NOT IN ('{bkList}')")
    if newRecords.count() > 0:
        newRecords.write.insertInto(tableName=targetTableFqn)

    sourceDataFrame = sourceDataFrame.where(f"{_.Name}_BK IN ('{bkList}')")
    
    changeRecords = sourceDataFrame.alias("s") \
        .join(targetTable.alias("t"), f"{_.Name}_BK") \
        .where(f"t._Current = 1 AND ({changeColumns})") 

    stagedUpdates = changeRecords.selectExpr("NULL BK", "s.*") \
        .union( \
          sourceDataFrame.selectExpr(f"{_.Name}_BK BK", "*") \
        )

    insertValues = {
        f"{_.Name}_SK": f"{_.Name}_SK", 
        f"{_.Name}_BK": f"COALESCE(s.BK, s.{_.Name}_BK)",
        "_Created": "s._Created",
        "_Ended": DEFAULT_END_DATE,
        "_Current": "1",
        "_Batch_SK": expr(f"DATE_FORMAT(s._Created, 'yyMMddHHmmss') || COALESCE(DATE_FORMAT({DEFAULT_END_DATE}, '{DATE_FORMAT}'), '{BATCH_END_CODE}') || 1")
    }
    for c in [i for i in targetTable.columns if i not in _exclude]:
        insertValues[f"{c}"] = f"s.{c}"

    DeltaTable.forName(spark, targetTableFqn).alias("t").merge(stagedUpdates.alias("s"), f"t.{_.Name}_BK = BK") \
        .whenMatchedUpdate(
          condition = f"t._Current = 1 AND ({changeColumns})", 
          set = {
            "_Ended": expr(f"{DEFAULT_START_DATE} - INTERVAL 1 SECOND"),
            "_Current": "0",
            "_Batch_SK": expr(f"DATE_FORMAT(s._Created, '{DATE_FORMAT}') || COALESCE(DATE_FORMAT({DEFAULT_START_DATE} - INTERVAL 1 SECOND, '{DATE_FORMAT}'), '{BATCH_END_CODE}') || 0") 
          }
        ) \
        .whenNotMatchedInsert(
          values = insertValues
        ).execute()
    
    
    #EndNotebook(sourceDataFrame)
    return 

# COMMAND ----------

# DBTITLE 1,Cleanup
CleanSelf()

# COMMAND ----------

Save(spark.sql("SELECT 1 Test_BK, 'Version_1' TestingName \
UNION SELECT 2 Test_BK, 'Version_1' TestingName \
"))
display(spark.table(_.Destination))

# COMMAND ----------

Save(spark.sql("SELECT 1 Test_BK, 'Version_1' TestingName"))
display(spark.table(_.Destination))

# COMMAND ----------

Save(spark.sql("SELECT 1 Test_BK, 'Version_2' TestingName"))
display(spark.table(_.Destination))

# COMMAND ----------

Save(spark.sql("SELECT 2 Test_BK, 'Version_2' TestingName"))
display(spark.table(_.Destination))

# COMMAND ----------

Save(spark.sql("SELECT 2 Test_BK, 'Version_3' TestingName"))
display(spark.table(_.Destination))

# COMMAND ----------

Save(spark.sql("SELECT 3 Test_BK, 'Version_1' TestingName"))
display(spark.table(_.Destination))

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT Test_BK, TestingName, _Batch_SK FROM curated.F_Test

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT DISTINCT _Batch_SK, 
# MAGIC _Created,
# MAGIC _Ended
# MAGIC FROM curated.F_Test

# COMMAND ----------


