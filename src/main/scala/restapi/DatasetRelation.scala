package restapi

import com.databricks.spark.avro._
import org.apache.log4j.Logger
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.sources.{BaseRelation, TableScan}
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{DataFrame, Row, SQLContext}

/**
 * Abstract relation class for reading data from file
 */
case class DatasetRelation(
    fileLocation: String,  //Downloaded file path, can be HDFS/S3
    fileType: String,
    inferSchema: String,
    header: String,
    delimiter: String,
    rowTag: String,   //Used for reading XML content
    customSchema: StructType,
    sqlContext: SQLContext) extends BaseRelation with TableScan {

    private val logger = Logger.getLogger(classOf[DatasetRelation])

    val df = read()

    private def read(): DataFrame = {
      var dataframeReader = sqlContext.read
      if (customSchema != null) {
        dataframeReader = dataframeReader.schema(customSchema)
      }

      var df: DataFrame = null
      if (fileType.equals("json")) {
        df = dataframeReader.json(fileLocation)
      } else if (fileType.equals("parquet")) {
        df = dataframeReader.parquet(fileLocation)
      } else if (fileType.equals("txt")) {
        df = dataframeReader.text(fileLocation)
      } else if (fileType.equals("xml")) {
        df = dataframeReader.format(constants.xmlClass)
          .option(constants.xmlRowTag, rowTag)
          .load(fileLocation)
      }
      else if (fileType.equals("csv")) {
        df = dataframeReader.
          option("header", header).
          option("delimiter", delimiter).
          option("inferSchema", inferSchema).
          csv(fileLocation)
      } else if (fileType.equals("avro")) {
        df = dataframeReader.avro(fileLocation)
      }

      df
    }

    override def schema: StructType = {
      df.schema
    }

    override def buildScan(): RDD[Row] = {
      df.rdd
    }

}
