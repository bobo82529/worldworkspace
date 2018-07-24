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



//Actor处理多线程并发问题
class MyActor extends Actor {
  val a = new ActorSystemUtils()

  //根据pare的值作出反应。如果为ini,则进行初始化;如果为hourGet,则执行小时级任务
  def receive() = {
    //case pare => if (pare.equals("ini")) println("ini") else if ((pare.equals("hourGet"))) println("hourGet")
    case pare => if (pare.equals("ini")) a.ini else if (pare.equals("hourGet")) a.hourGet else if (pare.equals("minuteGet")) a.minuteGet
  }
}

object UserAnalytics {
  val system = ActorSystem("mySystem")
  val act = system.actorOf(Props[MyActor], "beginning")
  implicit val time = Timeout(5 seconds)

  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.WARN)
    val conf = new SparkConf()
      .setAppName("UserAnalytics")
      //.setMaster("local[2]")
    val ssc = new StreamingContext(conf, Seconds(3))

    //读取kafka中的数据
    val topics = Set("new-ubi-trip-events")
    //测试内网kafka和zookeeper
    //val brokers = "10.104.109.176:9092,10.104.86.238:9092,10.104.122.21:9092"
    //val zkConnect = "10.104.109.176:2188,10.104.86.238:2188,10.104.122.21:2188"
    //测试外网kafka和zookeeper
    //val brokers = "118.89.59.251:9092,118.89.60.46:9092,118.89.62.210:9092"
    //val zkConnect = "118.89.59.251:2188,118.89.60.46:2188,118.89.62.210:2188"
    //正式内网kafka和zookeeper
    val brokers = "10.135.142.234:9092,10.135.145.169:9092"
    val zkConnect = "10.135.159.207:2188,10.135.142.234:2188,10.135.145.169:2188"
    //正式外网：基本用不到
    //val brokers = "118.89.25.68:9092,123.207.242.130:9092"
    //val zkConnect = "123.207.243.124:2188,118.89.25.68:2188,123.207.242.130:2188"
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

    //Redis工具类
    //val Ini = new RedisUtils
    val data = new DataFormatUtils
    val nowTime = data.getNow_time()
    val hourTime = data.getHour_time()
    val zeroTime = data.getZero_time()
    val minuteTime = data.getMinute_time()
    //延迟时间，等于下一小时整点(如果现在17:42，那么等于18:00)减去当前的时刻
    /**************************************************************************
    lsm修改记录2
    延迟每天级别数据的清零
      **************************************************************************/
    val beginTime01: Long = (hourTime -10 + 3600) - nowTime
    //延迟时间,等于今天最后时刻(当天24:00:00)减去当前的时刻

    val beginTime02: Long = (zeroTime + 3600 * 24) - nowTime
    val minuteChange:Long = (minuteTime + 60) - nowTime

    // Redis配置

    //其他环境
    //val redisHost = "192.168.171.235"
    //测试外网Redis
    //val redisHost = "118.89.60.46"
    //测试内网
    //val redisHost = "10.104.86.238"
    //正式内网
    val redisHost = "10.104.156.96"
    val redisPort = 6379
    //json数据抽取
    val jsonAnalytics = new JsonAnalyticsUtils
    val userEvents = jsonAnalytics.userEventsJson(events)

    //新建Jedis
    val jedisTemp = new Jedis(redisHost, redisPort)
    //初始化
    //Ini.redisInitialize(jedisTemp)
    //Ini.redisHourInitialize(jedisTemp)
    //定时清零
    /**
      * AKKA还支持消息的定期重复执行，方法为schedule()。例如设置在2秒后，间隔5秒重复执行：
      * 每天24:00:00准时清零
      */
    system.scheduler.schedule(beginTime02 seconds, 3600 * 24 seconds, act, "ini")
    //小时级的定时
    system.scheduler.schedule(beginTime01 seconds, 3600 seconds, act, "hourGet")
    //分钟级别定时
    system.scheduler.schedule(minuteChange seconds, 60 seconds, act, "minuteGet")

    //关闭Jedis
    jedisTemp.close()

    //监控程序
    //监控策略是，每50s发送GPS消息，特征是terminalId为87601.
    val filteredDStream = events.filter(_.contains("\"terminalId\":\"87601\""))
    filteredDStream.foreachRDD(rdd => {
      val jedis = new Jedis(redisHost, redisPort)
      val currentDate = (new DataFormatUtils).getNow_time
      if (rdd.count > 0)
        jedis.set("BIG_SCREEN_MONITOR", currentDate.toString)
        jedis.expire("BIG_SCREEN_MONITOR", 600)
        jedis.close()
    })

    //将原来的redis的数据和kafka的数据相加，然后更新到redis
    userEvents.foreachRDD(rdd => {
      if (rdd.count > 0)
        rdd.foreachPartition(partitionOfRecords => {
          val jedis = new Jedis(redisHost, redisPort)
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


    ssc.start()
    ssc.awaitTermination()
  }
}