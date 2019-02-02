/*
 * Copyright 2015 springml
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package restapi

import java.io.File
import java.util.UUID

import com.savy3.spark.restapi.util._
import com.springml.sftp.client.SFTPClient
import org.apache.commons.io.FilenameUtils
import org.apache.hadoop.fs.Path
import org.apache.log4j.Logger
import org.apache.spark.sql.sources.{BaseRelation, CreatableRelationProvider, RelationProvider, SchemaRelationProvider}
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{DataFrame, SQLContext, SaveMode}

/**
 * Datasource to construct dataframe from a sftp url
 */
class DefaultSource extends RelationProvider with SchemaRelationProvider with CreatableRelationProvider  {
  @transient val logger = Logger.getLogger(classOf[DefaultSource])
  
  val ioHelper = new IOHelper()

  /**
   * Copy the file from SFTP to local location and then create dataframe using local file
   */
  override def createRelation(sqlContext: SQLContext, parameters: Map[String, String]) = {
    createRelation(sqlContext, parameters, null)
  }

   override def createRelation(sqlContext: SQLContext, parameters: Map[String, String], schema: StructType) = {
    val username = parameters.get("username")
    val password = parameters.get("password")
    val pemFileLocation = parameters.get("pem")
    val pemPassphrase = parameters.get("pemPassphrase")
    val host = parameters.getOrElse("host", sys.error("SFTP Host has to be provided using 'host' option"))
    val port = parameters.get("port")
    val path = parameters.getOrElse("path", sys.error("'path' must be specified"))
    val fileType = parameters.getOrElse("fileType", sys.error("File type has to be provided using 'fileType' option"))
    val inferSchema = parameters.get("inferSchema")
    val header = parameters.getOrElse("header", "true")
    val delimiter = parameters.getOrElse("delimiter", ",")
    val createDF = parameters.getOrElse("createDF", "true")
    val copyLatest = parameters.getOrElse("copyLatest", "false")
    val tempFolder = parameters.getOrElse("tempLocation", System.getProperty("java.io.tmpdir"))
    val hdfsTemp = parameters.getOrElse("hdfsTempLocation", tempFolder)
    val cryptoKey = parameters.getOrElse("cryptoKey", null)
    val cryptoAlgorithm = parameters.getOrElse("cryptoAlgorithm", "AES")
    val rowTag = parameters.getOrElse(constants.xmlRowTag, null)

    val supportedFileTypes = List("csv", "json", "avro", "parquet", "txt", "xml")
    if (!supportedFileTypes.contains(fileType)) {
      sys.error("fileType " + fileType + " not supported. Supported file types are " + supportedFileTypes)
    }

    val inferSchemaFlag = if (inferSchema != null && inferSchema.isDefined) {
      inferSchema.get
    } else {
      "false"
    }

    val sftpClient = getSFTPClient(username, password, pemFileLocation, pemPassphrase, host, port, cryptoKey, cryptoAlgorithm)

    //TODO - 1) Copy to local for creating dataframe 2) Convert the response to JSON, string to dataframe and persist in Spark memory
    //download the file content to local file from ResponseBody
    val copiedFileLocation = ioHelper.copy(sftpClient, path, tempFolder, copyLatest.toBoolean)

    val fileLocation = ioHelper.copyToHdfs(sqlContext, copiedFileLocation, hdfsTemp)

    if (!createDF.toBoolean) {
      logger.info("Returning an empty dataframe after copying files...")
      createReturnRelation(sqlContext, schema)
    } else {
      DatasetRelation(fileLocation, fileType, inferSchemaFlag, header, delimiter, rowTag, schema, sqlContext)
    }
  }

  /**
   * Copy the file from SFTP to local location and then create dataframe using local file
   */
  override def createRelation(sqlContext: SQLContext, parameters: Map[String, String], schema: StructType) = {
    val username = parameters.get("username")
    val password = parameters.get("password")
    val pemFileLocation = parameters.get("pem")
    val pemPassphrase = parameters.get("pemPassphrase")
    val host = parameters.getOrElse("host", sys.error("SFTP Host has to be provided using 'host' option"))
    val port = parameters.get("port")
    val path = parameters.getOrElse("path", sys.error("'path' must be specified"))
    val fileType = parameters.getOrElse("fileType", sys.error("File type has to be provided using 'fileType' option"))
    val inferSchema = parameters.get("inferSchema")
    val header = parameters.getOrElse("header", "true")
    val delimiter = parameters.getOrElse("delimiter", ",")
    val createDF = parameters.getOrElse("createDF", "true")
    val copyLatest = parameters.getOrElse("copyLatest", "false")
    val tempFolder = parameters.getOrElse("tempLocation", System.getProperty("java.io.tmpdir"))
    val hdfsTemp = parameters.getOrElse("hdfsTempLocation", tempFolder)
    val cryptoKey = parameters.getOrElse("cryptoKey", null)
    val cryptoAlgorithm = parameters.getOrElse("cryptoAlgorithm", "AES")
    val rowTag = parameters.getOrElse(constants.xmlRowTag, null)

    val supportedFileTypes = List("csv", "json", "avro", "parquet", "txt", "xml")
    if (!supportedFileTypes.contains(fileType)) {
      sys.error("fileType " + fileType + " not supported. Supported file types are " + supportedFileTypes)
    }

    val inferSchemaFlag = if (inferSchema != null && inferSchema.isDefined) {
      inferSchema.get
    } else {
      "false"
    }

    val sftpClient = getSFTPClient(username, password, pemFileLocation, pemPassphrase, host, port,
      cryptoKey, cryptoAlgorithm)
    val copiedFileLocation = copy(sftpClient, path, tempFolder, copyLatest.toBoolean)
    val fileLocation = ioHelper.copyToHdfs(sqlContext, copiedFileLocation, hdfsTemp)

    if (!createDF.toBoolean) {
      logger.info("Returning an empty dataframe after copying files...")
      createReturnRelation(sqlContext, schema)
    } else {
      DatasetRelation(fileLocation, fileType, inferSchemaFlag, header, delimiter, rowTag, schema,
        sqlContext)
    }
  }

  /**
    * Write to SFTP location.
    * @param sqlContext
    * @param mode
    * @param parameters
    * @param data
    * @return
    */
  override def createRelation(
      sqlContext: SQLContext,
      mode: SaveMode,
      parameters: Map[String, String],
      data: DataFrame): BaseRelation = {

    val username = parameters.get("username")
    val password = parameters.get("password")
    val pemFileLocation = parameters.get("pem")
    val pemPassphrase = parameters.get("pemPassphrase")
    val host = parameters.getOrElse("host", sys.error("SFTP Host has to be provided using 'host' option"))
    val port = parameters.get("port")
    val path = parameters.getOrElse("path", sys.error("'path' must be specified"))
    val fileType = parameters.getOrElse("fileType", sys.error("File type has to be provided using 'fileType' option"))
    val header = parameters.getOrElse("header", "true")
    val copyLatest = parameters.getOrElse("copyLatest", "false")
    val tmpFolder = parameters.getOrElse("tempLocation", System.getProperty("java.io.tmpdir"))
    val hdfsTemp = parameters.getOrElse("hdfsTempLocation", tmpFolder)
    val cryptoKey = parameters.getOrElse("cryptoKey", null)
    val cryptoAlgorithm = parameters.getOrElse("cryptoAlgorithm", "AES")
    val delimiter = parameters.getOrElse("delimiter", ",")
    val codec = parameters.getOrElse("codec", null)
    val rowTag = parameters.getOrElse(constants.xmlRowTag, null)
    val rootTag = parameters.getOrElse(constants.xmlRootTag, null)

    val supportedFileTypes = List("csv", "json", "avro", "parquet", "txt", "xml")
    if (!supportedFileTypes.contains(fileType)) {
      sys.error("fileType " + fileType + " not supported. Supported file types are " + supportedFileTypes)
    }

    val sftpClient = getSFTPClient(username, password, pemFileLocation, pemPassphrase, host, port, cryptoKey, cryptoAlgorithm)

    val tempFile = writeToTemp(sqlContext, data, hdfsTemp, tmpFolder, fileType, header, delimiter, codec, rowTag, rootTag)

    upload(tempFile, path, sftpClient)

    return createReturnRelation(data)
  }



  private def createReturnRelation(data: DataFrame): BaseRelation = {
    createReturnRelation(data.sqlContext, data.schema)
  }

  private def createReturnRelation(sqlContextVar: SQLContext, schemaVar: StructType): BaseRelation = {
    new BaseRelation {
      override def sqlContext: SQLContext = sqlContextVar
      override def schema: StructType = schemaVar
    }
  }



  
    

}
