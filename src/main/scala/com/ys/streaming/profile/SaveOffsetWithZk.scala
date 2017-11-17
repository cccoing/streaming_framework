package com.ys.streaming.profile

import com.tmrwh.dmp.framework.StreamingFramework
import com.tmrwh.dmp.util.{JsonUtil, KafkaManager, RedisUtil}
import com.ys.streaming.framework.StreamingFramework
import com.ys.streaming.util.{JsonUtil, KafkaManager}
import org.apache.spark.streaming.kafka.{HasOffsetRanges, OffsetRange}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import net.liftweb.json._

/**
  * Created by yangshuo on 2017/11/11.
  * save offset to zk
  */
object SaveOffsetWithZk {

  private var topic  = ""
  private var broker = ""
  private var group = ""

  def main(args: Array[String]): Unit = {

    object App extends StreamingFramework(name = "test",
      batchDuration = Seconds(20)) {
      /*
        * userdefine function
        */
      override def userdefined(streamingContext: StreamingContext): Unit = {

        val kafkaParams = Map[String, String]("metadata.broker.list" -> broker)
        val stream = KafkaManager.getDStream(topic, group, streamingContext, kafkaParams)

        // Hold a reference to the current offset ranges, so it can be used downstream
        var offsetRanges = Array.empty[OffsetRange]
        stream.transform { rdd =>
          offsetRanges = rdd.asInstanceOf[HasOffsetRanges].offsetRanges
          rdd
        }.map(record => {
//          println(s"key = ${record._1},value = ${record._2}")
          record._2
        }).map(extractFieldFromJsonStr)
          .filter(!_._1.equals(""))
            .foreachRDD(rdd => {
              // save rdd to hdfs
              rdd.reduceByKey(_ + "," + _)
                .saveAsTextFile("/user/yangshuo/click")

              // save offset to zk
              KafkaManager.saveOffsetToZk(topic, group, offsetRanges)
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

      override def usage[T](t: T): Unit = {
        if (args.size != 3) {
          println("Usage: SaveOffsetWithZk need 3 params,[broker, topics, group]")
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
