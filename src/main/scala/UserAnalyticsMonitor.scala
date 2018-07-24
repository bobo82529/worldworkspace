import java.util

import akka.actor._
import akka.util.Timeout
import kafka.serializer.StringDecoder
import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}
import redis.clients.jedis.Jedis

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration._



object UserAnalyticsMonitor {

  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.WARN)
    val conf = new SparkConf()
            .setAppName("UserAnalyticsMonitor")
            .setMaster("local[2]")
    val ssc = new StreamingContext(conf, Seconds(3))

    //读取kafka中的数据
    //val topics = Set("new-ubi-trip-events")
    val topics = Set("flume_test_lsm")

    //测试外网kafka和zookeeper
    val brokers = "118.89.59.251:9092,118.89.60.46:9092,118.89.62.210:9092"
    val zkConnect = "118.89.59.251:2188,118.89.60.46:2188,118.89.62.210:2188"
    val kafkaParams = Map[String, String](
      "zookeeper.connect" -> zkConnect, //配置zookeeper
      "metadata.broker.list" -> brokers,
      //"group.id" -> "streaming", //设置一下group id
      "auto.offset.reset" -> kafka.api.OffsetRequest.LargestTimeString, //从该topic最新的位置开始读数
      //"client.id" -> "UserAnalytics",
      "zookeeper.connection.timeout.ms" -> "10000"
    )

    //从kafka读取数据到一个DStream
    val kafkaStream: InputDStream[(String, String)] = KafkaUtils.
                    createDirectStream[String, String, StringDecoder, StringDecoder](ssc, kafkaParams, topics)
    //val events: DStream[String] = kafkaStream.window(Seconds(3), Seconds(3)).map(line => line._2)


    val events: DStream[String] = kafkaStream.map(line => line._2)
    events.foreachRDD(rdd => {
      rdd.foreach(println)
    })

    val redisHost = "118.89.60.46"
    val redisPort = 6379

    val filteredDStream = events.filter(_.contains("\"terminalId\":\"87601\""))
    filteredDStream.foreachRDD(rdd => {
      val jedis = new Jedis(redisHost, redisPort)
      val currentDate = (new DataFormatUtils).getNow_time
      if (rdd.count > 0)
        jedis.set("BIG_SCREEN_MONITOR", currentDate.toString)
        jedis.expire("BIG_SCREEN_MONITOR", 120)
      jedis.close()
    })

    filteredDStream.foreachRDD(rdd => {
      rdd.foreach(println)
    })



    /*
           //json数据抽取
           val jsonAnalytics = new JsonAnalyticsUtils
           val userEvents = jsonAnalytics.userEventsJson(events)

           //将原来的redis的数据和kafka的数据相加，然后更新到redis
           userEvents.foreachRDD(rdd => {
             if (rdd.count > 0)
               rdd.foreachPartition(partitionOfRecords => {

                 partitionOfRecords.foreach(pair => {

                   /*实时级
                     今日里程			    BS_MILE_TODAY
                     今日驾车人次		  BS_DRIVING_USER_NUM_TODAY
                     今日三急行为		  BS_THREE_QUICKNESS_TODAY
                     今日行程评分		  BS_DRIVING_SCHEDULE_SCORE_TODAY
                   */

                   //今日里程
                   val totalMileageKey: String = "BS_MILE_TODAY"

                   //获取Redis中的里程
                   val mileage: Int = pair._1.toInt
                   val totalMileage: Int = jedis.get(totalMileageKey).toInt
                   val totalMileageNew = TotalNums.totalMileage(totalMileage, mileage)
                   jedis.set(totalMileageKey, totalMileageNew.toString)

                   //设备号的集合
                   val teterminalIdArrayBufferKey: String = "BS_TETERMINALID_TODAY"
                   //今日驾车人次
                   val totalPersonCountsKey: String = "BS_DRIVING_USER_NUM_TODAY"

                   //设备号的集合
                   val terminalId: String = pair._2
                   jedis.sadd(teterminalIdArrayBufferKey, terminalId)

                   //今日驾车人次
                   val totalPersonCountsNew = jedis.scard(teterminalIdArrayBufferKey)
                   jedis.set(totalPersonCountsKey, totalPersonCountsNew.toString)

                   //今日急加速次数
                   val totalAccelerationRapidTimesKey: String = "BS_ACCELERATION_RAPID_TIMES_DAY"
                   //今日急减速次数
                   val totalDecelerationRapidTimesKey: String = "BS_DECELERATION_RAPID_TIMES_DAY"
                   //今日急转弯次数
                   val totalSuddenTurnTimesKey: String = "BS_SUDDEN_TURN_TIMES_DAY"
                   //今日三急行为
                   val totalThreeTimesKey: String = "BS_THREE_QUICKNESS_TODAY"
                   //今日加速度>3G的数量
                   val totalThreeGKey: String = "BS_THREE_G_TODAY"

                   //今日急加速次数
                   val accelerationRapidTimes: Int = pair._3
                   val totalAccelerationRapidTimes: Int = jedis.get(totalAccelerationRapidTimesKey).toInt
                   val totalAccelerationRapidTimesNew = TotalNums.totalAccelerationRapidTimes(totalAccelerationRapidTimes, accelerationRapidTimes)
                   jedis.set(totalAccelerationRapidTimesKey, totalAccelerationRapidTimesNew.toString)

                   //今日急减速次数
                   val decelerationRapidTimes: Int = pair._5
                   val totalDecelerationRapidTimes: Int = jedis.get(totalDecelerationRapidTimesKey).toInt
                   val totalDecelerationRapidTimesNew = TotalNums.totalDecelerationRapidTimes(totalDecelerationRapidTimes, decelerationRapidTimes)
                   jedis.set(totalDecelerationRapidTimesKey, totalDecelerationRapidTimesNew.toString)

                   //今日加速度>3G的次数
                   val threeGTimes: Int = pair._11 + pair._12
                   val totalThreeGTimes: Int = jedis.get(totalThreeGKey).toInt
                   val totalThreeGTimesNew: Int = TotalNums.totalThreeGTimes(totalThreeGTimes, threeGTimes)
                   jedis.set(totalThreeGKey, totalThreeGTimesNew.toString)

                   //今日急转弯次数
                   val suddenTurnTimes: Int = pair._7
                   val totalSuddenTurnTimes: Int = jedis.get(totalSuddenTurnTimesKey).toInt
                   val totalSuddenTurnTimesNew = TotalNums.totalSuddenTurnTimes(totalSuddenTurnTimes, suddenTurnTimes)
                   jedis.set(totalSuddenTurnTimesKey, totalSuddenTurnTimesNew.toString)

                   //今日三急行为
                   val accelerationTimes: Int = jedis.get(totalAccelerationRapidTimesKey).toInt
                   val decelerationTimes: Int = jedis.get(totalDecelerationRapidTimesKey).toInt
                   val suddenTimes: Int = jedis.get(totalSuddenTurnTimesKey).toInt
                   val threeTimesNew = TotalNums.totalThreeTimes(accelerationTimes, decelerationTimes, suddenTimes)
                   jedis.set(totalThreeTimesKey, threeTimesNew.toString)




                   /*
                   //今日行程评分的分布
                   val scoreCountArrayKey: String = "BS_DRIVING_SCHEDULE_COUNT_TODAY"
                   //今日行程评分占比
                   val scorePercentArrayKey: String = "BS_DRIVING_SCHEDULE_SCORE_TODAY"

                   val scoreArray = new Array[Int](10)
                   scoreArray(0) = pair._4
                   scoreArray(1) = pair._6
                   scoreArray(2) = pair._8
                   scoreArray(3) = pair._9
                   scoreArray(4) = pair._10
                   scoreArray(5) = pair._13
                   scoreArray(6) = pair._14
                   scoreArray(7) = pair._15
                   scoreArray(8) = pair._16
                   scoreArray(9) = pair._17

                   //计算行程评分
                   val score: Double = (scoreArray(0) + scoreArray(1) + scoreArray(2) + scoreArray(3) * 2 + scoreArray(4) * 2 +
                     scoreArray(5) * 3 + scoreArray(6) * 3 + scoreArray(7) * 3 + scoreArray(8) * 3 + scoreArray(9) * 3) / 22.0

                   //今日行程评分分布
                   val listPercent: List[String] = List("<60", "60-70", "70-80", "80-90", ">90")
                   //scoreCountArray代表各评分中的行程数
                   val scoreCountArray: util.Map[String, String] = jedis.hgetAll(scoreCountArrayKey)
                   //scoreCountArrayTemp代表一个数组,存储行程数
                   val scoreCountArrayTemp = new Array[Int](5)
                   for (i <- 0 to 4) {
                     scoreCountArrayTemp(i) = scoreCountArray.get(listPercent(i)).toInt
                   }


                   //scoreCountArrayNew存储新的得分
                   val scoreCountArrayNew = TotalNums.scoreCount(score.toInt, scoreCountArrayTemp)
                   //val scoreCountArrayNew = TotalNums.scoreCount(scoreArray, scoreCountArrayTemp)
                   //今日行程评分的分布
                   for (j <- 0 to 4) {
                     jedis.hset(scoreCountArrayKey, listPercent(j), scoreCountArrayNew(j).toString)
                   }

                   //今日行程评分占比
                   val scorePercentArray: util.Map[String, String] = jedis.hgetAll(scorePercentArrayKey)
                   val scorePercentArrayTemp = new Array[String](5)
                   for (i <- 0 to 4) {
                     scorePercentArrayTemp(i) = scorePercentArray.get(listPercent(i)).toString
                   }
                   val scorePercentArrayNew = TotalNums.scorePercent(scoreCountArrayNew, scorePercentArrayTemp)
                   for (j <- 0 to 4) {
                     jedis.hset(scorePercentArrayKey, listPercent(j), scorePercentArrayNew(j))
                   }
                   */
                 })
                 jedis.close()
               })
           })

           //进行全流程监控
           userEvents.foreachRDD(rdd => {
             if (rdd.count > 0)
               rdd.foreachPartition(partitionOfRecords => {
                 val jedis = new Jedis(redisHost, redisPort)
                 partitionOfRecords.foreach(pair => {

                 })
               })
           })
           */
    ssc.start()
    ssc.awaitTermination()
  }
}