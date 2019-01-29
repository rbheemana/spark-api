# Spark API-EndPoint Connector Library

A library for constructing dataframes by downloading files from api end point

-- This project is still in construction phase

## Requirements

This library requires Spark 2.x.

### Scala API
```scala

// Construct Spark dataframe using file in FTP server
val df = spark.read.
            format("com.savy3.spark.api").
            option("host", "API_URL").
            option("username", "API_USER").
            option("password", "****").
            option("fileType", "xml").
            load("path")
