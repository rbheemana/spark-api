package restapi

import java.io.{File, FileOutputStream}
import java.util.UUID

import com.savy3.spark.restapi.util._
import com.springml.sftp.client.SFTPClient
import org.apache.commons.io.FilenameUtils
import org.apache.hadoop.fs.Path
import org.apache.log4j.Logger
import org.apache.spark.sql.{DataFrame, SQLContext}
import scalaj.http.HttpResponse

class IOHelper {
@transient val logger = Logger.getLogger(classOf[DefaultSource])
  def addShutdownHook(tempLocation: String) {
    logger.debug("Adding hook for file " + tempLocation)
    val hook = new DeleteTempFileShutdownHook(tempLocation)
    Runtime.getRuntime.addShutdownHook(hook)
  }



  /**
    * HDFS helper method
    * @param sqlContext
    * @param hdfsTemp
    * @param fileLocation
    * @return
    */
  def copyFromHdfs(sqlContext: SQLContext, hdfsTemp : String,
                           fileLocation : String): String  = {
    val hadoopConf = sqlContext.sparkContext.hadoopConfiguration
    val hdfsPath = new Path(hdfsTemp)
    val fs = hdfsPath.getFileSystem(hadoopConf)
    if ("hdfs".equalsIgnoreCase(fs.getScheme)) {
      fs.copyToLocalFile(new Path(hdfsTemp), new Path(fileLocation))
      fs.deleteOnExit(new Path(hdfsTemp))
      return fileLocation
    } else {
      return hdfsTemp
    }
  }



    private def writeToTemp(sqlContext: SQLContext,
                            df: DataFrame,
                            hdfsTemp: String,
                            tempFolder: String,
                            fileType: String,
                            header: String,
                            delimiter: String,
                            codec: String,
                            rowTag: String,
                            rootTag: String
    ) : String = {
    val randomSuffix = "spark_sftp_connection_temp_" + UUID.randomUUID
    val hdfsTempLocation = hdfsTemp + File.separator + randomSuffix
    val localTempLocation = tempFolder + File.separator + randomSuffix

    addShutdownHook(localTempLocation)

    if (fileType.equals("json")) {
      df.coalesce(1).write.json(hdfsTempLocation)
    } else if (fileType.equals("txt")) {
      df.coalesce(1).write.text(hdfsTempLocation)
    } else if (fileType.equals("xml")) {
      df.coalesce(1).write.format(constants.xmlClass)
        .option(constants.xmlRowTag, rowTag)
        .option(constants.xmlRootTag, rootTag).save(hdfsTempLocation)
    }
    else if (fileType.equals("parquet")) {
      df.coalesce(1).write.parquet(hdfsTempLocation)
      return copiedParquetFile(hdfsTempLocation)
    } else if (fileType.equals("csv")) {
      df.coalesce(1).
        write.
        option("header", header).
        option("delimiter", delimiter).
        optionNoNull("codec", Option(codec)).
        csv(hdfsTempLocation)
    } else if (fileType.equals("avro")) {
      df.coalesce(1).write.format("com.databricks.spark.avro").save(hdfsTempLocation)
    }
    //copy written HDFS file to local
    copyFromHdfs(sqlContext, hdfsTempLocation, localTempLocation)
    copiedFile(localTempLocation)
  }



  private def copiedParquetFile(tempFileLocation: String) : String = {
    val baseTemp = new File(tempFileLocation)
    val files = baseTemp.listFiles().filter { x =>
      (!x.isDirectory()
          && x.getName.endsWith("parquet")
          && !x.isHidden())}
    files(0).getAbsolutePath
  }

  //get the written file in local file system. Exclude un wanted files.
  private def copiedFile(tempFileLocation: String) : String = {
    val baseTemp = new File(tempFileLocation)
    val files = baseTemp.listFiles().filter { x =>
      (!x.isDirectory()
        && !x.getName.contains("SUCCESS")
        && !x.isHidden()
        && !x.getName.contains(".crc"))}
    files(0).getAbsolutePath
  }


  private def upload(source: String, target: String, sftpClient: SFTPClient) {
    logger.info("Copying " + source + " to " + target)
    sftpClient.copyToFTP(source, target)
  }

  /**
    * Sftp Cient Copy action - download to local file system. Later will be moved to HDFS path.
    * @param sftpClient
    * @param source
    * @param tempFolder
    * @param latest
    * @return
    */
  def copy(sftpClient: SFTPClient, source: String, tempFolder: String, latest: Boolean): String = {
    var copiedFilePath: String = null
    try {
      val target = tempFolder + File.separator + FilenameUtils.getName(source)
      copiedFilePath = target
      if (latest) {
        copiedFilePath = sftpClient.copyLatest(source, tempFolder)
      } else {
        logger.info("Copying " + source + " to " + target)
        copiedFilePath = sftpClient.copy(source, target)
      }

      copiedFilePath
    } finally {
      addShutdownHook(copiedFilePath)
    }
  }

  /**
    * Read Array of Bytes and store in local filesystem file.
    * Need to delete the file after copying to HDFS to save local space.
    * @param fileBytesResponse
    * @param localFilePath - File path and filename.
    * @return - Return the local temp location file path with file name
    */
  def copyFromHttpResponse(fileBytesResponse: Array[Byte], localTempPath: String, localFilePath: String, fileName: String ): String = {
    var pathWithFileName: String = s"${localTempPath}${localFilePath}${fileName}"
   try {
     (new FileOutputStream(pathWithFileName)).write(fileBytesResponse)
     logger.info(s"Response saved to ${pathWithFileName}")
   } catch {
     case e : Any => logger.error(s"copyFromHttpResponse -> Error while saving to ${pathWithFileName}" + e.toString)
     pathWithFileName = ""
   } finally {
      if(!pathWithFileName.isEmpty) addShutdownHook(pathWithFileName)
    }
   pathWithFileName
  }

  /**
    * HDFS helper method - Copy from local to HDFS path
    * @param sqlContext
    * @param fromLocalFilePath
    * @param hdfsTemp
    * @return
    */
  def copyToHdfs(sqlContext: SQLContext, fromLocalFile : String, toHdfsTemp : String): String  = {
    val hadoopConf = sqlContext.sparkContext.hadoopConfiguration
    val fromLocalFilePath = new Path(fromLocalFile)
    val toHdfsPath = new Path(toHdfsTemp)
    var toHdfsFile: String = ""
    val fs = fromLocalFilePath.getFileSystem(hadoopConf)
    if ("hdfs".equalsIgnoreCase(fs.getScheme)) {
      try{
        fs.copyFromLocalFile(fromLocalFilePath, toHdfsPath)
        toHdfsFile = toHdfsTemp + "/" + fromLocalFilePath.getName
        return toHdfsFile
      }
      catch {
        case e : Any => {
          logger.error(s"copyToHdfs -> Error while saving to ${toHdfsFile}" + e.toString)
          //TODO - Decide on what to return
          fromLocalFile
        }
      }
      finally {
        fs.deleteOnExit(new Path(toHdfsFile))
      }

    } else {
      return fromLocalFile
    }
  }


}
