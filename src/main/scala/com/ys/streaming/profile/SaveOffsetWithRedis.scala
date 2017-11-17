package com.ys.streaming.profile

import com.tmrwh.dmp.framework.StreamingFramework
import com.tmrwh.dmp.util._
import com.ys.streaming.framework.StreamingFramework
import com.ys.streaming.util.{DateUtil, JsonUtil, RedisManager, RedisUtil}
import net.liftweb.json.parse
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.streaming.kafka.{HasOffsetRanges, OffsetRange}

/**
  * Created by yangshuo on 2017/11/15.
  * Save click log to redis, keep most five click log
  * Expire date is five Days
  */
object SaveOffsetWithRedis {

  private var topic  = ""
  private var broker = ""
  private var group = ""

  def main(args: Array[String]): Unit = {

    object App extends StreamingFramework(name = "click-articles",
      batchDuration = Seconds(60)) {
      /*
        * userdefine function
        */
      override def userdefined(streamingContext: StreamingContext): Unit = {

        val kafkaParams = Map[String, String]("metadata.broker.list" -> broker)
        //  Get stream from redis offset
        val stream = RedisManager.getDStream(topic, group, streamingContext, kafkaParams)

        // Hold a reference to the current offset ranges, so it can be used downstream
        var offsetRanges = Array.empty[OffsetRange]
        stream.transform { rdd =>
          offsetRanges = rdd.asInstanceOf[HasOffsetRanges].offsetRanges
          rdd
        }.map(record => {
          //  println(s"key = ${record._1},value = ${record._2}")
          record._2
        })
          // Get (name, age) from json string
          .map(extractFieldFromJsonStr)
          .foreachRDD(rdd => {
            // Save offset to redis
            RedisManager.saveOffsetToRedis(topic, group, offsetRanges)

            val aggregationedRDD = rdd.reduceByKey(_ + "," + _)
            // Save rdd to redis
            aggregationedRDD
                .foreachPartition(insertRedisFuncPartition)
            // Save rdd to hdfs, hdfs path is "/user/yangshuo/click/$DATE/$TS"
            aggregationedRDD.saveAsTextFile("/user/yangshuo/click/" + DateUtil.getNowDate() + "/" + System.currentTimeMillis())
          })
      }

      def extractFieldFromJsonStr(row: String): (String, String) = {
        try {
          val jvalue = parse(row)
          val age = JsonUtil.getIntValue("age", jvalue)
          val myname = JsonUtil.getValue("name", jvalue)
          (myname, age)
        } catch {
          case _ => ("", "")
        }
      }

      def insertRedisFuncPartition(numbers :
                                   Iterator[(String, String)]): Unit  = {
        try {
          RedisUtil.setParam("redis_host", 6379)
          val jedis = RedisUtil.getResource
          numbers.foreach {
            case (name , ages) =>
              ages.split(",").foreach(age => {
                jedis.lpush(name, age)
              })
              // Redis list only need five elements
              jedis.ltrim(name, 0, 4)
              // Keep most 5 days
              jedis.expire(name, 60*60*24*5)
          }
          jedis.close()
        } catch {
          case _ =>
        }
      }

      override def usage[T](t: T): Unit = {
        if (args.size != 3) {
          println("Usage: ClickProfile need 3 params,[broker, topics, group]")
          System.exit(-1)
        }
        broker = args(0)
        topic = args(1)
        group = args(2)
      }
    }

    App.run(args)
  }
}
