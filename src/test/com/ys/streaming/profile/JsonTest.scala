package com.ys.streaming.profile

import org.json4s._
import org.json4s.JsonDSL._
import org.json4s.jackson.JsonMethods._

/**
  * Created by hadoop on 2017/11/13.
  */
object JsonTest {
  case class Click(lang: String, user_seq_id: String, article_seq_id: String)
  def main(args: Array[String]): Unit = {

    val json = "{\"lang\":\"hi\",\"custom_categories\":[\"local\"],\"user_seq_id\":21632294,\"article_seq_id\":177763120,\"ts\":1510546721,\"article_type\":\"article\",\"source\":\"main\",\"article_cate\":\"health\",\"app_v\":\"2.4.7\"}"
//    val json = "{\"lang\":\"hi\",\"user_seq_id\":21632294,\"article_seq_id\":177763120}"

    println(json)
    val jsonFormat = parse(json).values.asInstanceOf[Map[String,_]]

    val lang = JsonUtils.parseJsonFromJsonStr[String](json, "lang")
    val custom_categories = JsonUtils.parseJsonFromJsonStr[List[String]](json, "custom_categories")

    println(jsonFormat.get("lang"))
    println(lang)
    println(custom_categories)
  }
}
