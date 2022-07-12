# Databricks notebook source
# MAGIC %run ../../Common/common-transform

# COMMAND ----------

def Transform():
    # ------------- TABLES ----------------- #
    audf = spark.sql(f"SELECT concat(1001,uid_booking_type) BookingType_BK, BookingTypeKey, Case when BookingTypeCode = '' then '(~empty~)' when BookingTypeCode is null then '(~null~)' else BookingTypeCode end BookingTypeCode, Case when BookingTypeName = '' then '(~empty~)' when BookingTypeName is null then '(~null~)' else BookingTypeName end BookingTypeName, 1 as CountryCode FROM {DEFAULT_SOURCE}.dbo_dimbookingtype")
    nzdf = spark.sql(f"SELECT concat(2002,uid_booking_type) BookingType_BK, BookingTypeKey, Case when BookingTypeCode = '' then '(~empty~)' when BookingTypeCode is null then '(~null~)' else BookingTypeCode end BookingTypeCode, Case when BookingTypeName = '' then '(~empty~)' when BookingTypeName is null then '(~null~)' else BookingTypeName end BookingTypeName, 2 as CountryCode FROM {DEFAULT_SOURCE}.dbo_nz_dimbookingtype")
    df = audf.union(nzdf)
    # ------------- JOINS ------------------ #

    # ------------- TRANSFORMS ------------- #
    _.Transforms = [
        "*"
    ]
    df = df.selectExpr(
        _.Transforms
    )

    # ------------- CLAUSES ---------------- #

    # ------------- SAVE ------------------- #
    #display(df)
    Save(df)
pass

Transform()