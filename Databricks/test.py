# Databricks notebook source
# MAGIC %run /ELT-Framework/Common/common-jdbc

# COMMAND ----------

CreateSqlTableFromSource(sourceTable="dbo.ExtractLoadManifest", targetTable="default.ExtractLoadManifest", targetJdbcKVSecretName="JDBC-SQL-ControlDB")


# COMMAND ----------

def GetSqlDataFrameTable(sql, targetJdbcKVSecretName):
    jdbc = GetJdbc(targetJdbcKVSecretName)
    return spark.read.option("driver", "com.microsoft.sqlserver.jdbc.SQLServerDriver").jdbc(url=jdbc, table=f"({sql}) T")

# COMMAND ----------

df = GetSqlDataFrameTable("SELECT * FROM information_schema.tables WHERE TABLE_TYPE = 'BASE TABLE'", "JDBC-SQL-ControlDB")
display(df)

# COMMAND ----------

jdbc = GetJdbc("JDBC-SQL-ControlDB")
df = spark.read.jdbc(jdbc, "information_schema.columns")
display(df)

def CreateControlDBViews():
    jdbc = JdbcConnectionFromSqlConnectionString(dbutils.secrets.get(scope = "ADS", key = "daf-sql-controldb-connectionstring"))

    sql = """
    SELECT SCHEMA_NAME(sOBJ.schema_id) [SchemaName]
        ,sOBJ.name AS [TableName]
          , SUM(sPTN.Rows) AS [RowCount]
    FROM  sys.objects AS sOBJ
    INNER JOIN sys.partitions AS sPTN
                ON sOBJ.object_id = sPTN.object_id
    WHERE
          sOBJ.type = 'U'
          AND sOBJ.is_ms_shipped = 0
          AND index_id < 2
    GROUP BY
          sOBJ.schema_id
          , sOBJ.name
          HAVING SUM(sPTN.Rows) > 0"""
    df = spark.read.option("driver", "com.microsoft.sqlserver.jdbc.SQLServerDriver").jdbc(url=jdbc, table=f"({sql}) T")

    for i in df.rdd.collect():
        schema = i.SchemaName
        table = i.TableName

        if(table.lower() in ["__refactorlog", "sysdiagrams"]):
            continue
        
        #df = spark.read.option("driver", "com.microsoft.sqlserver.jdbc.SQLServerDriver").jdbc(url=jdbc, table=f"{schema}.{table}")
        #df.createOrReplaceTempView(f"controldb_{schema}_{table}")
        
        try:
            spark.sql(f"DROP TABLE controldb.{schema}_{table}")
        except:
            pass
        sql = f"""CREATE TABLE controldb.{schema}_{table}
                USING org.apache.spark.sql.jdbc
                OPTIONS (
                  url \"{jdbc}\",
                  dbtable \"{schema}.{table}\"
                )"""
        #print(sql)
        spark.sql(sql)
        #break
        
        #spark.sql(f"DROP VIEW controldb_{schema}_{table}")

        #break
CreateControlDBViews()

# COMMAND ----------


