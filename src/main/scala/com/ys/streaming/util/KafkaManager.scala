package com.ys.streaming.util

import kafka.common.TopicAndPartition
import kafka.message.MessageAndMetadata
import kafka.serializer.StringDecoder
import kafka.utils.{ZKGroupTopicDirs, ZkUtils}
import org.I0Itec.zkclient.ZkClient
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.dstream.InputDStream
import org.apache.spark.streaming.kafka.{KafkaUtils, OffsetRange}

/**
  * Created by yangshuo on 2017/11/11.
  * Manage zk's offset
  */
object KafkaManager {

  def getDStream[K, V](topic: String, groupId : String, ssc: StreamingContext,
                       kafkaParams : Map[String, String]): InputDStream[(String, String)] = {

    val topics : Set[String]  = Set(topic)
    val topicDirs = new ZKGroupTopicDirs(groupId, topic)

    // get zk client
    val zkClient = new ZkClient(ZookeeperConst.ZOOKEEPER_IPS)

    /**
      * get children from given zk path
      * if zk path doesn't exist, children is 0
      */
    val children = zkClient.countChildren(s"${topicDirs.consumerOffsetDir}")

    /**
      * used for collect Offset
      */
    var fromOffsets: Map[TopicAndPartition, Long] = Map()

    /**
      * get stream
      * if children is 0,get stream from earlies|latest,configure in auto.offset.reset
      * if children is not 0,get stream from the offset spcified by zk path
      */
    val stream = if (children > 0) {
      for (i <- 0 until children) {
        val partitionOffset = zkClient.readData[String](s"${topicDirs.consumerOffsetDir}/${i}")
        val tp = new TopicAndPartition(topic, i)
        println(s"get child partition ${i} offset is ${partitionOffset}")
        fromOffsets += (tp -> partitionOffset.toLong)
      }

      val messageHandler = (mmd : MessageAndMetadata[String, String]) => (mmd.topic, mmd.message())
      KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder, (String, String)](
        ssc, kafkaParams, fromOffsets, messageHandler)
    } else {
      KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](
        ssc, kafkaParams, topics)
    }
    stream
  }

  def saveOffsetToZk(topic :String, groupId :String, offsetRanges :Array[OffsetRange]): Unit = {
    // Get zk client
    val zkClient = new ZkClient(ZookeeperConst.ZOOKEEPER_IPS)
    val topicDirs = new ZKGroupTopicDirs(groupId, topic)

    for (offset <- offsetRanges) {
      val zkPath = s"${topicDirs.consumerOffsetDir}/${offset.partition}"
      println(s"zkPath = ${zkPath}")
      // Use untilOffset or it will consume twice at the first time
      ZkUtils.updatePersistentPath(zkClient, zkPath, offset.untilOffset.toString)
    }
  }
}

