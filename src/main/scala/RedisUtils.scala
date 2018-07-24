import java.util

import redis.clients.jedis.Jedis

import scala.util.Random

class RedisUtils extends Serializable {

  //redis各个指标数据初始化
  def redisInitialize(jedis: Jedis): Unit = {
    /*实时级
      今日里程			    BS_MILE_TODAY
      今日驾车人次		  BS_DRIVING_USER_NUM_TODAY
      今日三急行为		  BS_THREE_QUICKNESS_TODAY
      今日行程评分		  BS_DRIVING_SCHEDULE_SCORE_TODAY
    */
    //今日里程
    val totalMileageKey: String = "BS_MILE_TODAY"
    //设备号的集合
    val teterminalIdArrayBufferKey: String = "BS_TETERMINALID_TODAY"
    //今日驾车人次
    val totalPersonCountsKey: String = "BS_DRIVING_USER_NUM_TODAY"
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

    //设置里程数的初始值
    jedis.set(totalMileageKey, "0")

    //设备号的集合
    jedis.del(teterminalIdArrayBufferKey)
    jedis.sadd(teterminalIdArrayBufferKey, "184706")

    //今日驾车人次
    jedis.set(totalPersonCountsKey, "0")

    //今日急加速次数
    jedis.set(totalAccelerationRapidTimesKey, "0")

    //今日急减速次数
    jedis.set(totalDecelerationRapidTimesKey, "0")

    //今日急转弯次数
    jedis.set(totalSuddenTurnTimesKey, "0")

    //今日三急行为
    jedis.set(totalThreeTimesKey, "0")

    //今日加速度>3G的数量
    jedis.set(totalThreeGKey, "0")

    //今日行程评分的分布
    val scoreCountArrayKey: String = "BS_DRIVING_SCHEDULE_COUNT_TODAY"

    //今日行程评分占比
    val scorePercentArrayKey: String = "BS_DRIVING_SCHEDULE_SCORE_TODAY"

    //今日行程评分分布
    /**************************************************************************
    lsm修改记录3
    初始化为5300000，3600000，500000，400000，200000
      **************************************************************************/
    val listCount: List[String] = List("<60", "60-70", "70-80", "80-90", ">90")
    val listScoreCount: List[Int] = List(0)
    for (i <- 0 to 4) {
      jedis.hset(scoreCountArrayKey, listCount(i), listScoreCount(0).toString)
    }

    //今日行程评分占比
    val listPercent: List[String] = List("<60", "60-70", "70-80", "80-90", ">90")
    val listScorePercent: List[Double] = List(0)
    for (i <- 0 to 4) {
      jedis.hset(scorePercentArrayKey, listPercent(i), listScorePercent(0).toString)
    }

    //小时级
    val totalHourKey: String = "BS_BY_HOUR"
    val listHourKey: List[String] = List("totalMileage", "totalPersonCounts",
      "totalAccelerationRapidTimes", "totalDecelerationRapidTimes",
      "totalSuddenTurnTimes", "totalThreeGTimes")
    for (i <- 0 to 5) {
      jedis.hset(totalHourKey, listHourKey(i), 0.toString)
    }
  }



  //更新小时级别的redis的数据,对应页面中的三个页面的曲线
  def redisHourGet(jedis: Jedis): Unit = {
    /*小时级
      今日里程曲线		  BS_TOTAL_MILE_TODAY_BY_HOUR
      今日驾车人次曲线	BS_DRIVING_USER_NUM_BY_HOUR
      今日三急行为曲线	BS_THREE_QUICKNESS_TODAY_BY_HOUR
    */
    //BS_BY_HOUR 表中存放的是最近整点的各小时级别的当天的累计值
    val totalHourKey: String = "BS_BY_HOUR"
    val listHourKey: List[String] = List("totalMileage", "totalPersonCounts",
      "totalAccelerationRapidTimes", "totalDecelerationRapidTimes",
      "totalSuddenTurnTimes", "totalThreeGTimes")
    val a = new DataFormatUtils
    val time = a.getHour()

    if (time.toInt != 23) {
      //今日里程(实时)
      val totalMileageKey: String = "BS_MILE_TODAY"
      //返回哈希表 totalMileageKey 的值,totalMileageN 为当前(实时)的累计值
      val totalMileageN: Int = jedis.get(totalMileageKey).toInt
      //返回哈希表 totalHourKey 中给定域 totalMileage(小时级)的值,该表每个小时变化一次
      val totalMileageB: Int = jedis.hget(totalHourKey, listHourKey(0)).toInt
      // listMileageHourN 为实时的值减去上一个小时的累计值
      val listMileageHourN: Int = totalMileageN - totalMileageB

      //每小时里程数
      // totalMileageHourKey 为当天每小时的行程数
      val totalMileageHourKey: String = "BS_TOTAL_MILE_TODAY_BY_HOUR"
      // 设置哈希表 BS_TOTAL_MILE_TODAY_BY_HOUR 中时段time的值为listMileageHourN
      jedis.hset(totalMileageHourKey, time, listMileageHourN.toString)
      // 更新到当前时段最新的累计值
      jedis.hset(totalHourKey, listHourKey(0), totalMileageN.toString)


      //今日驾车人次
      val totalPersonCountsKey: String = "BS_DRIVING_USER_NUM_TODAY"
      val totalPersonCountsN = jedis.get(totalPersonCountsKey).toLong
      val totalPersonCountsB = jedis.hget(totalHourKey, listHourKey(1)).toLong
      // listPersonHourN 为实时的值减去上一小时的累计值
      val listPersonHourN = totalPersonCountsN - totalPersonCountsB

      //每小时驾车人次
      val totalPersonCountsHourKey: String = "BS_DRIVING_USER_NUM_BY_HOUR"
      jedis.hset(totalPersonCountsHourKey, time, listPersonHourN.toString)
      jedis.hset(totalHourKey, listHourKey(1), totalPersonCountsN.toString)


      //今日急加速次数
      val totalAccelerationRapidTimesKey: String = "BS_ACCELERATION_RAPID_TIMES_DAY"
      val totalAccelerationRapidTimesN: Int = jedis.get(totalAccelerationRapidTimesKey).toInt
      val totalAccelerationRapidTimesB: Int = jedis.hget(totalHourKey, listHourKey(2)).toInt
      val listAccelerationHourN: Int = totalAccelerationRapidTimesN - totalAccelerationRapidTimesB

      //每小时急加速次数
      val listAccelerationHour: String = "BS_ACCELERATION_RAPID_TIMES_BY_HOUR"
      jedis.hset(listAccelerationHour, time, listAccelerationHourN.toString)
      jedis.hset(totalHourKey, listHourKey(2), totalAccelerationRapidTimesN.toString)

      //今日急减速次数
      val totalDecelerationRapidTimesKey: String = "BS_DECELERATION_RAPID_TIMES_DAY"
      val totalDecelerationRapidTimesN: Int = jedis.get(totalDecelerationRapidTimesKey).toInt
      val totalDecelerationRapidTimesB: Int = jedis.hget(totalHourKey, listHourKey(3)).toInt
      val listDecelerationHourN: Int = totalDecelerationRapidTimesN - totalDecelerationRapidTimesB

      //每小时急减速次数
      val listDecelerationHour: String = "BS_DECELERATION_RAPID_TIMES_BY_HOUR"
      jedis.hset(listDecelerationHour, time, listDecelerationHourN.toString)
      jedis.hset(totalHourKey, listHourKey(3), totalDecelerationRapidTimesN.toString)

      //今日急转弯次数
      val totalSuddenTurnTimesKey: String = "BS_SUDDEN_TURN_TIMES_DAY"
      val totalSuddenTurnTimesN: Int = jedis.get(totalSuddenTurnTimesKey).toInt
      val totalSuddenTurnTimesB: Int = jedis.hget(totalHourKey, listHourKey(4)).toInt
      val listSuddenTurnHourN: Int = totalSuddenTurnTimesN - totalSuddenTurnTimesB

      //每小时急转弯次数
      val listSuddenTurnHour: String = "BS_SUDDEN_TURN_TIMES_BY_HOUR"
      jedis.hset(listSuddenTurnHour, time, listSuddenTurnHourN.toString)
      jedis.hset(totalHourKey, listHourKey(4), totalSuddenTurnTimesN.toString)

      //今日加速度>3G的数量
      val totalThreeGKey: String = "BS_THREE_G_TODAY"
      val totalThreeGTimesN: Int = jedis.get(totalThreeGKey).toInt
      val totalThreeGTimesB: Int = jedis.hget(totalHourKey, listHourKey(5)).toInt
      val listThreeGHourN: Int = totalThreeGTimesN - totalThreeGTimesB

      //每小时加速度>3G的数量
      val listThreeGHour: String = "BS_ACC_GREATER_THEN_3G_TIMES_BY_HOUR"
      jedis.hset(listThreeGHour, time, listThreeGHourN.toString)
      jedis.hset(totalHourKey, listHourKey(5), totalThreeGTimesN.toString)
    }
  }

  def redisHourGet_23(jedis: Jedis): Unit = {
    /*小时级
      今日里程曲线		  BS_TOTAL_MILE_TODAY_BY_HOUR
      今日驾车人次曲线	BS_DRIVING_USER_NUM_BY_HOUR
      今日三急行为曲线	BS_THREE_QUICKNESS_TODAY_BY_HOUR
    */
    val totalHourKey: String = "BS_BY_HOUR"
    val listHourKey: List[String] = List("totalMileage", "totalPersonCounts",
      "totalAccelerationRapidTimes", "totalDecelerationRapidTimes",
      "totalSuddenTurnTimes", "totalThreeGTimes")
    val a = new DataFormatUtils
    val time = a.getHour()

    //今日里程
    val totalMileageKey: String = "BS_MILE_TODAY"
    val totalMileageN: Int = jedis.get(totalMileageKey).toInt
    val totalMileageB: Int = jedis.hget(totalHourKey, listHourKey(0)).toInt
    val listMileageHourN: Int = totalMileageN - totalMileageB

    //每小时里程数
    val totalMileageHourKey: String = "BS_TOTAL_MILE_TODAY_BY_HOUR"
    if(listMileageHourN != 0){
      jedis.hset(totalMileageHourKey, time, listMileageHourN.toString)
      jedis.hset(totalHourKey, listHourKey(0), totalMileageN.toString)
    }


    //今日驾车人次
    val totalPersonCountsKey: String = "BS_DRIVING_USER_NUM_TODAY"
    val totalPersonCountsN = jedis.get(totalPersonCountsKey).toLong
    val totalPersonCountsB = jedis.hget(totalHourKey, listHourKey(1)).toLong
    val listPersonHourN = totalPersonCountsN - totalPersonCountsB

    //每小时驾车人次
    val totalPersonCountsHourKey: String = "BS_DRIVING_USER_NUM_BY_HOUR"
    if(listPersonHourN != 0){
      jedis.hset(totalPersonCountsHourKey, time, listPersonHourN.toString)
      jedis.hset(totalHourKey, listHourKey(1), totalPersonCountsN.toString)
    }



    //今日急加速次数
    val totalAccelerationRapidTimesKey: String = "BS_ACCELERATION_RAPID_TIMES_DAY"
    val totalAccelerationRapidTimesN: Int = jedis.get(totalAccelerationRapidTimesKey).toInt
    val totalAccelerationRapidTimesB: Int = jedis.hget(totalHourKey, listHourKey(2)).toInt
    val listAccelerationHourN: Int = totalAccelerationRapidTimesN - totalAccelerationRapidTimesB

    //每小时急加速次数
    val listAccelerationHour: String = "BS_ACCELERATION_RAPID_TIMES_BY_HOUR"

    if(listAccelerationHourN != 0){
      jedis.hset(listAccelerationHour, time, listAccelerationHourN.toString)
      jedis.hset(totalHourKey, listHourKey(2), totalAccelerationRapidTimesN.toString)
    }


    //今日急减速次数
    val totalDecelerationRapidTimesKey: String = "BS_DECELERATION_RAPID_TIMES_DAY"
    val totalDecelerationRapidTimesN: Int = jedis.get(totalDecelerationRapidTimesKey).toInt
    val totalDecelerationRapidTimesB: Int = jedis.hget(totalHourKey, listHourKey(3)).toInt
    val listDecelerationHourN: Int = totalDecelerationRapidTimesN - totalDecelerationRapidTimesB

    //每小时急减速次数
    val listDecelerationHour: String = "BS_DECELERATION_RAPID_TIMES_BY_HOUR"
    if(listDecelerationHourN != 0){
      jedis.hset(listDecelerationHour, time, listDecelerationHourN.toString)
      jedis.hset(totalHourKey, listHourKey(3), totalDecelerationRapidTimesN.toString)
    }


    //今日急转弯次数
    val totalSuddenTurnTimesKey: String = "BS_SUDDEN_TURN_TIMES_DAY"
    val totalSuddenTurnTimesN: Int = jedis.get(totalSuddenTurnTimesKey).toInt
    val totalSuddenTurnTimesB: Int = jedis.hget(totalHourKey, listHourKey(4)).toInt
    val listSuddenTurnHourN: Int = totalSuddenTurnTimesN - totalSuddenTurnTimesB

    //每小时急转弯次数
    val listSuddenTurnHour: String = "BS_SUDDEN_TURN_TIMES_BY_HOUR"
    if(listSuddenTurnHourN != 0){
      jedis.hset(listSuddenTurnHour, time, listSuddenTurnHourN.toString)
      jedis.hset(totalHourKey, listHourKey(4), totalSuddenTurnTimesN.toString)
    }


    //今日加速度>3G的数量
    val totalThreeGKey: String = "BS_THREE_G_TODAY"
    val totalThreeGTimesN: Int = jedis.get(totalThreeGKey).toInt
    val totalThreeGTimesB: Int = jedis.hget(totalHourKey, listHourKey(5)).toInt
    val listThreeGHourN: Int = totalThreeGTimesN - totalThreeGTimesB

    //每小时加速度>3G的数量
    val listThreeGHour: String = "BS_ACC_GREATER_THEN_3G_TIMES_BY_HOUR"
    if(listThreeGHourN != 0){
      jedis.hset(listThreeGHour, time, listThreeGHourN.toString)
      jedis.hset(totalHourKey, listHourKey(5), totalThreeGTimesN.toString)
    }

  }

  def redisMinuteGet(jedis: Jedis): Unit = {
    /*分鐘级
      處理行程打分
    */

    val scorePercentArrayKey: String = "BS_DRIVING_SCHEDULE_SCORE_TODAY"
    //今日行程评分分布
    val listPercent: List[String] = List("<60", "60-70", "70-80", "80-90", ">90")

    val initialList:Array[Int] = Array(200,400,500,3600,5300)
    var scorePercentArrayNew:List[String]=Nil

    val randomOf0_1=(new Random).nextInt(2)
    val randomOf0_2=(new Random).nextInt(2)
    val randomNum1=30+(new Random).nextInt(50)
    val randomNum2=30+(new Random).nextInt(50)
    val randomNum3=300+(new Random).nextInt(100)
    val randomNum4=50+(new Random).nextInt(30)


    if (randomOf0_1 == 0 && randomOf0_2 == 0){

      val v0 = ((initialList(0) + randomNum1) / 10000.00).formatted("%.4f")
      val v1 = ((initialList(1) + randomNum2)/10000.00).formatted("%.4f")
      val v3 = ((initialList(3) - randomNum1 + randomNum3 - randomNum4)/10000.00).formatted("%.4f")
      val v4 = ((initialList(4) - randomNum2 - randomNum3)/10000.00).formatted("%.4f")
      val v2 = (1 - (v0.toDouble + v1.toDouble + v3.toDouble + v4.toDouble)).formatted("%.4f")
      scorePercentArrayNew = scorePercentArrayNew :+ v0  :+ v1 :+ v2 :+ v3 :+ v4
    }

    else if (randomOf0_1 == 0 && randomOf0_2 == 1){

      val v0 = ((initialList(0) + randomNum1) / 10000.00).formatted("%.4f")
      val v1 = ((initialList(1) + randomNum2)/10000.00).formatted("%.4f")
      val v3 = ((initialList(3) - randomNum1 - randomNum3 - randomNum4)/10000.00).formatted("%.4f")
      val v4 = ((initialList(4) - randomNum2 + randomNum3)/10000.00).formatted("%.4f")
      val v2 = (1 - (v0.toDouble + v1.toDouble + v3.toDouble + v4.toDouble)).formatted("%.4f")
      scorePercentArrayNew = scorePercentArrayNew :+ v0  :+ v1 :+ v2 :+ v3 :+ v4
    }

    else if (randomOf0_1 == 1 && randomOf0_2 == 0){
      val v0 = ((initialList(0) - randomNum1) / 10000.00).formatted("%.4f")
      val v1 = ((initialList(1) - randomNum2) / 10000.00).formatted("%.4f")
      val v3 = ((initialList(3) + randomNum1 + randomNum3 + randomNum4) / 10000.00).formatted("%.4f")
      val v4 = ((initialList(4) + randomNum2 - randomNum3) / 10000.00).formatted("%.4f")
      val v2 = (1 - (v0.toDouble + v1.toDouble + v3.toDouble + v4.toDouble)).formatted("%.4f")
      scorePercentArrayNew = scorePercentArrayNew :+ v0  :+ v1 :+ v2 :+ v3 :+ v4

    }

    else if (randomOf0_1 == 1 && randomOf0_2 == 1){
      val v0 = ((initialList(0) - randomNum1) / 10000.00).formatted("%.4f")
      val v1 = ((initialList(1) - randomNum2) / 10000.00).formatted("%.4f")
      val v3 = ((initialList(3) + randomNum1 - randomNum3 + randomNum4) / 10000.00).formatted("%.4f")
      val v4 = ((initialList(4) + randomNum2 + randomNum3) / 10000.00).formatted("%.4f")
      val v2 = (1 - (v0.toDouble + v1.toDouble + v3.toDouble + v4.toDouble)).formatted("%.4f")
      scorePercentArrayNew = scorePercentArrayNew :+ v0  :+ v1 :+ v2 :+ v3 :+ v4

    }

    for (j <- 0 to 4) {
      jedis.hset(scorePercentArrayKey, listPercent(j), scorePercentArrayNew(j))
    }

  }



  def redisHourInitialize(jedis: Jedis): Unit = {
    /*小时级
      今日里程曲线		  BS_TOTAL_MILE_TODAY_BY_HOUR
      今日驾车人次曲线	BS_DRIVING_USER_NUM_BY_HOUR
      今日三急行为曲线	BS_THREE_QUICKNESS_TODAY_BY_HOUR*/
    val listHour: List[String] = List("00", "01", "02", "03", "04", "05", "06", "07", "08", "09", "10", "11", "12",
      "13", "14", "15", "16", "17", "18", "19", "20", "21", "22", "23")

    //每小时里程数
    val totalMileageHourKey: String = "BS_TOTAL_MILE_TODAY_BY_HOUR"
    val listMileageHour: List[Int] = List(0)
    for (i <- 0 to 23) {
      jedis.hset(totalMileageHourKey, listHour(i), listMileageHour(0).toString)
    }

    //每小时驾车人次
    val totalPersonCountsHourKey: String = "BS_DRIVING_USER_NUM_BY_HOUR"
    val listPersonHour: List[Int] = List(0)
    for (i <- 0 to 23) {
      jedis.hset(totalPersonCountsHourKey, listHour(i), listPersonHour(0).toString)
    }

    //每小时急加速次数
    val totalAccelerationRapidTimesHourKey: String = "BS_ACCELERATION_RAPID_TIMES_BY_HOUR"
    val listAccelerationHour: List[Int] = List(0)
    for (i <- 0 to 23) {
      jedis.hset(totalAccelerationRapidTimesHourKey, listHour(i), listAccelerationHour(0).toString)
    }

    //每小时急减速次数
    val totalDecelerationRapidTimesHourKey: String = "BS_DECELERATION_RAPID_TIMES_BY_HOUR"
    val listDecelerationHour: List[Int] = List(0)
    for (i <- 0 to 23) {
      jedis.hset(totalDecelerationRapidTimesHourKey, listHour(i), listDecelerationHour(0).toString)
    }

    //每小时急转弯次数
    val totalSuddenTurnTimesHourKey: String = "BS_SUDDEN_TURN_TIMES_BY_HOUR"
    val listSuddenTurnHour: List[Int] = List(0)
    for (i <- 0 to 23) {
      jedis.hset(totalSuddenTurnTimesHourKey, listHour(i), listSuddenTurnHour(0).toString)
    }

    //每小时加速度>3G的数量
    val totalThreeGHourKey: String = "BS_ACC_GREATER_THEN_3G_TIMES_BY_HOUR"
    val listThreeGHour: List[Int] = List(0)
    for (i <- 0 to 23) {
      jedis.hset(totalThreeGHourKey, listHour(i), listThreeGHour(0).toString)
    }
  }
}
