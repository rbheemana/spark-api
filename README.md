# Spark API-EndPoint Connector Library

A library for constructing dataframes by downloading files from api end point

-- This project is still in construction phase

## Requirements

This library requires Spark 2.x.

### Scala API
```scala

// Construct Spark dataframe using file in FTP server
val df = spark.read.
            format("com.apache.spark.api").
            option("host", "HOST_DOMAIN_NAME").
            option("auth_type", "AUTHENTICATION_TYPE").
            option("client_id", "CLIENT_ID").
            option("client_key", "CLIENT_KEY").
            option("token", "TOKEN").
            option("refrsh_token", "REFRESH_TOKEN").            
            option("username", "API_USER").
            option("password", "****").
            option("fileType", "application/json").
            option("retry_count", "RETRY_COUNT").
            option("retry_frquencey", "RETRY_FREQUENCY").
            load("rest_endpoint")
