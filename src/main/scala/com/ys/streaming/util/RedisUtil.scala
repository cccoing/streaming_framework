package com.ys.streaming.util

import kafka.common.TopicAndPartition
import kafka.message.MessageAndMetadata
import kafka.serializer.StringDecoder
import org.apache.commons.pool2.impl.GenericObjectPoolConfig
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.dstream.InputDStream
import org.apache.spark.streaming.kafka.{KafkaUtils, OffsetRange}
import redis.clients.jedis.{Jedis, JedisPool}

import scala.collection.JavaConverters._

/**
  * Created by yangshuo on 2017/11/15.
  */
object RedisUtil extends Serializable {

  private var host = ""
  private var port = 6379

  def setParam(host :String, port :Int): Unit = {
    this.host = host
    this.port = port
  }

  def getResource: Jedis = pool.getResource

  private val redisTimeout = 30000
  private lazy val pool = new JedisPool(new GenericObjectPoolConfig(), host, port, redisTimeout)

  /**
    *  If use shutdown hook, it will occur error when shutdown spark streaming
    */
  /*private lazy val hook = new Thread {
    override def run() = {
      pool.destroy()
    }
  }
  sys.addShutdownHook(hook.run())*/
}

object RedisManager {

  val KAFKA_OFFSET_PREFIX = "kafka-offsets"
  val REDIS_HOST = "reds_host"
  val SEPARATOR = ":"

  def getDStream(topic: String,group: String,ssc: StreamingContext,
                 kafkaParams : Map[String, String]): InputDStream[(String, String)] = {

    RedisUtil.setParam(REDIS_HOST, 6379)
    val jedis = RedisUtil.getResource

    val topics : Set[String]  = Set(topic)

    // Get offset from redis
    val children = jedis.hgetAll(KAFKA_OFFSET_PREFIX + SEPARATOR + group + SEPARATOR + topic).asScala

    /**
      * used for collect Offset
      */
    var fromOffsets: Map[TopicAndPartition, Long] = Map()

    //
    val stream = if (children.size > 0) {
      children.foreach {
        case (partitionId, offset) =>
          val tp = new TopicAndPartition(topic, partitionId.toInt)
          fromOffsets += (tp -> offset.toLong)
      }
      println(s"get offset from redis now,fromOffsets length ${fromOffsets.size}")
      val messageHandler = (mmd : MessageAndMetadata[String, String]) => (mmd.topic, mmd.message())
      KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder, (String, String)](
        ssc, kafkaParams, fromOffsets, messageHandler)
    } else {
      KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](
        ssc, kafkaParams, topics)
    }
    jedis.close()

    stream
  }

  /**
    * Use redis Hash store kafka offset (partitionId -> offset)
    * @param topic
    * @param groupId
    * @param offsetRanges
    */
  def saveOffsetToRedis(topic :String, groupId :String, offsetRanges :Array[OffsetRange]): Unit = {

    RedisUtil.setParam("redis_host", 6379)
    val jedis = RedisUtil.getResource

    val key = s"${KAFKA_OFFSET_PREFIX}${SEPARATOR}${groupId}${SEPARATOR}${topic}"
    val offsetMap = scala.collection.mutable.Map[String, String]()
    for (offsetRange <- offsetRanges) {
      val partition = offsetRange.partition.toString
      val offset = offsetRange.untilOffset.toString
      println(s"partition ${partition} fromOffset=${offsetRange.fromOffset} untilOffset=${offsetRange.untilOffset}")
      offsetMap += (partition -> offset)
    }

    jedis.hmset(key,offsetMap.asJava)
    jedis.close()
  }
}

