package com.savy3.spark.restapi.util

object Util1 {
def getValue(param: Option[String]): String = {
    if (param != null && param.isDefined) {
      param.get
    } else {
      null
    }
  }

}
