package com.ys.streaming.profile

import com.ys.streaming.util.RedisUtil

/**
  * Created by hadoop on 2017/11/15.
  */
object JedisTest {

  def main(args: Array[String]): Unit = {
//    RedisUtil("", 6379)

    RedisUtil.setParam("dmp-redis.swlldj.0001.aps1.cache.amazonaws.com", 6379)
    println("--")
    val jedis = RedisUtil.getResource

    jedis.lpush("test","test")
    jedis.close()
  }
}
