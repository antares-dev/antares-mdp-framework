# Databricks notebook source
# MAGIC %run ../../Common/common-transform

# COMMAND ----------

def Transform():
    # ------------- TABLES ----------------- #
    adfsales = spark.sql(f"select concat(1001,uid_factbookings) Bookings_BK, PanelKey, SiteKey, DateKey, TimeKey, ShareOfTime, to_date(cast(LastUpdateDateKey as char(8)), 'yyyyMMdd') LastUpdateDate, 1 as CountryCd from {DEFAULT_SOURCE}.dbo_factbookings")
    ndfsales = spark.sql(f"select concat(2002,uid_factbookings) Bookings_BK, PanelKey, SiteKey, DateKey, TimeKey, ShareOfTime, to_date(cast(LastUpdateDateKey as char(8)), 'yyyyMMdd') LastUpdateDate, 2 as CountryCd from {DEFAULT_SOURCE}.dbo_nz_factbookings")
    bkdf = adfsales.union(ndfsales)
    spark.sql(f"drop view if exists temp_f_bookings")     
    bkdf.createOrReplaceTempView("temp_f_bookings")
    dfbookings = spark.table("temp_f_bookings")    
    dfsite = spark.table("{DEFAULT_TARGET}.d_site")   
    dfpanel = spark.table("{DEFAULT_TARGET}.d_panel")     
  
    # ------------- JOINS ------------------ #
    df = dfbookings.join(dfsite, (dfbookings.SiteKey == dfsite.SiteKey) & (dfbookings.CountryCd == dfsite.CountryCode))
    df = df.join(dfpanel, (df.PanelKey == dfpanel.PanelKey) & (df.CountryCd == dfpanel.CountryCode))    
        
    # ------------- TRANSFORMS ------------- #
    _.Transforms = [
        "temp_f_bookings.Bookings_BK" 
        ,"temp_f_bookings.PanelKey"
        ,"temp_f_bookings.SiteKey" 
        ,"temp_f_bookings.DateKey" 
        ,"temp_f_bookings.TimeKey"
        ,"temp_f_bookings.ShareOfTime" 
        ,"temp_f_bookings.LastUpdateDate" 
        ,"temp_f_bookings.CountryCd CountryCode" 
        ,"d_site.Site_SK"
        ,"d_panel.Panel_SK"               
    ]
    df = df.selectExpr(
        _.Transforms
    )

    # ------------- CLAUSES ---------------- #
#     df = df.where("temp_f_sales.SiteKey IN (2318)")
    # ------------- SAVE ------------------- #
#     display(df)
    Save(df)
pass

Transform()

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC -- drop table {DEFAULT_TARGET}.f_bookings;