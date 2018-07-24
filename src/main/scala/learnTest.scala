import java.util

import redis.clients.jedis.Jedis

import scala.util.Random



object learnTest {


  def main(args: Array[String]): Unit = {

    val redisHost = "118.89.60.46"
    val redisPort = 6379
    val jedis = new Jedis(redisHost, redisPort)

    jedis.set("BIG_SCREEN_MONITOR2", "lsmtest1")
    jedis.get("BIG_SCREEN_MONITOR2")
    jedis.expire("BIG_SCREEN_MONITOR", 300)
    println("ceshii")
    val currentDate = (new DataFormatUtils).getNow_time
    println("ceshi"+currentDate)

  }


}


