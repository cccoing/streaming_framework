package com.ys.streaming.util

import net.liftweb.json._
import net.liftweb.json.JsonDSL._
/**
  * Created by yangshuo on 2017/11/14.
  */
object JsonUtil {

  def getValue(key: String, json: JValue): String = {
    val value = (json \ key \\ classOf[JString])
    if (value == JNothing || value.size == 0)
      ""
    else
      value(0)
  }

  def getIntValue(key : String,json : JValue): String ={
    val value = (json \ key \\ classOf[JInt])
    if (value == JNothing || value.size == 0)
      ""
    else
      value(0).toString()
  }

  def getValue(key : String,json : String): String ={
    val value = (parse(json) \ key \\ classOf[JString])
    if (value == JNothing || value.size == 0)
      ""
    else
      value(0)
  }

  def getIntValue(key : String,json : String): String ={
    val value = (parse(json) \ key \\ classOf[JInt])
    if (value == JNothing || value.size == 0)
      ""
    else
      value(0).toString()
  }

  def getValue(key1 : String , key2 : String , json : String): String ={
    val value = (parse(json) \ key1 \ key2 \\ classOf[JString])
    if (value == JNothing || value.size == 0)
      ""
    else
      value(0)
  }

  /*
  *   key : json key
  *   json : json String
  *   return JArray[Any]
  *   example : {"name":["mikcle","jack"]}
   */
  def getValues(key : String , json : String): JArray#Values ={
    (parse(json) \ key \\ classOf[JArray])(0)
  }

  def contains(key : String ,json : String): Boolean ={
    (parse(json) \ key \\ classOf[JString]).size != 0
  }

  def parser(json : String): JValue = {
    parse(json)
  }

}
