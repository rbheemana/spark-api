package restapi

import com.savy3.spark.restapi.util.Util1
import com.springml.sftp.client.SFTPClient
import org.apache.log4j.Logger
import scalaj.http.{Http, HttpRequest, HttpResponse}
import com.google.gson.{Gson, JsonObject}

class RestHelper {
@transient val logger = Logger.getLogger(classOf[DefaultSource])

  /**
    * Prepare Http client with URL and headers
    * @param requestHeadersMap
    * @return
    */
  def restClientWithHeaders( restEndPoint: String, requestHeadersMap: Option[Map[String, String]]): HttpRequest = {
    var httpRequest: HttpRequest = null

    if(!restEndPoint.isEmpty){
      httpRequest  = Http(restEndPoint)
      if(requestHeadersMap.isDefined){
        //.headers("Content-Type" -> "application/json", "key1" -> "val1")
        httpRequest = httpRequest.headers(requestHeadersMap.get)
      }
     }
    httpRequest
  }

  /**
    * Prepare HttpRequest with body detials
    * @param httpRequest
    * @param requestBody
    * @return
    */
   def addPostRequestBody( httpRequest: HttpRequest, requestBody: String ): HttpRequest = {
    if(httpRequest.isInstanceOf[HttpRequest] && !requestBody.isEmpty){
      return httpRequest.postData(requestBody)
    }
    return httpRequest
  }

  /**
    * The socket connection and read timeouts in milliseconds. Defaults are 1000 and 5000 respectively
    * @param httpRequest
    * @param proxyHostIP
    * @param proxyHostPort
    * @param connTimeoutMs
    * @param readTimeoutMs
    */
  def addRequestConfig(httpRequest: HttpRequest, proxyHostIP: String, proxyHostPort: Int, connTimeoutMs: Int = 1000, readTimeoutMs: Int = 5000): HttpRequest = {
    var localHttpRequest: HttpRequest = httpRequest
    if(httpRequest.isInstanceOf[HttpRequest]){
      if(!proxyHostIP.isEmpty && proxyHostPort.isValidInt){
        localHttpRequest = localHttpRequest.proxy(host = proxyHostIP, port = proxyHostPort)
      }

      if(connTimeoutMs.isValidInt && readTimeoutMs.isValidInt){
        localHttpRequest = localHttpRequest.timeout(connTimeoutMs = connTimeoutMs, readTimeoutMs = readTimeoutMs)
      }
    }
    localHttpRequest
  }

  def getHttpRequestResponse(responseType: String, httpRequest: HttpRequest): Any = {

    var stringResponse: String = null
    var bytesResponse: Array[Byte] = null
    try {
      if ("string".equalsIgnoreCase(responseType)) {
        stringResponse = httpRequest.asString.throwError.body
      } else if ("bytes".equalsIgnoreCase(responseType)) {
        bytesResponse = httpRequest.asBytes.throwError.body
      }
    } catch {
      case e: Any => logger.error(s"getHttpRequestResponse => Http request response error" + e.printStackTrace())
    }

  }

  /**
    *
    * @param objectType
    * @param responseString
    * @param objectClass - classOf[CaseClassName/PojoName]
    * @tparam T
    * @return
    */
  def httpResponseToObject[T](objectType: String, responseString: String, objectClass: Class[T]): Class[T] ={
    var convertedObject: Class[T] = null
    convertedObject =  objectType.toLowerCase match {
      case "json" => (new Gson()).fromJson(responseString, objectClass)
      //TODO - Add other object deserializers - xml, txt, html
      case _ => null
    }
    convertedObject
  }

  def getSFTPClient(
      username: Option[String],
      password: Option[String],
      pemFileLocation: Option[String],
      pemPassphrase: Option[String],
      host: String,
      port: Option[String],
      cryptoKey : String,
      cryptoAlgorithm : String) : SFTPClient = {

    val sftpPort = if (port != null && port.isDefined) {
      port.get.toInt
    } else {
      22
    }

    val cryptoEnabled = cryptoKey != null

    if (cryptoEnabled) {
      new SFTPClient(Util1.getValue(pemFileLocation), Util1.getValue(pemPassphrase), Util1.getValue(username),
        Util1.getValue(password),
          host, sftpPort, cryptoEnabled, cryptoKey, cryptoAlgorithm)
    } else {
      new SFTPClient(Util1.getValue(pemFileLocation), Util1.getValue(pemPassphrase), Util1.getValue(username),
        Util1.getValue(password), host, sftpPort)
    }
  }



}


//TODO - Review Scala Either Left Right
