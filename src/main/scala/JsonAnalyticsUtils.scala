import com.alibaba.fastjson.{JSON, JSONArray, JSONObject}
import org.apache.spark.streaming.dstream.DStream



/**
  *输入DStreaming(json数据),输出DStream(需要的数据组成的元组)
  * 实质是将json数据中有用的数据提取到一个元组中
  * map操作
  */
class JsonAnalyticsUtils {

  def userEventsJson(jsonLine: DStream[String]): DStream[(Int, String, Int, Int, Int, Int, Int, Int, Int, Int, Int, Int, Int, Int, Int, Int, Int)] = {
    //数据按行切分
    val linesRdd = jsonLine.flatMap(line => line.split("\\r\\n"))


    //解析Json
    val detectorDstream = linesRdd.map(line => {
      val jsonLine: JSONObject = JSON.parseObject(line)
      //行程里程数
      val mileage: Int = jsonLine.getInteger("mileage")
      //设备号
      val terminalId: String = jsonLine.getString("terminalId")

      //急加速事件
      val acceRapidEvents: String = jsonLine.getString("acceRapidEvents")
      //急加速次数
      val accelerationRapidTimes: Int = JSON.parseObject(acceRapidEvents).getInteger("times")
      //急加速得分
      val accelerationRapidScore: Int = JSON.parseObject(acceRapidEvents).getInteger("score")

      //急加速>3G的次数
      var acceCount = 0
      //有急加速数据
      if (accelerationRapidTimes > 0) {
        //获取JsonArray
        val points: JSONArray = JSON.parseObject(acceRapidEvents).getJSONArray("points")
        //遍历数组
        val size = points.size
        for (i <- 0 to size - 1) {
          val accePointsAcceValue: Double = points.getJSONObject(i).getString("acceValue").toDouble
          if (accePointsAcceValue > 3) acceCount += 1
        }
      }

      //急减速事件
      val deceRapidEvents: String = jsonLine.getString("deceRapidEvents")
      //急减速次数
      val decelerationRapidTimes: Int = JSON.parseObject(deceRapidEvents).getInteger("times")
      //急減速得分
      val decelerationRapidScore: Int = JSON.parseObject(deceRapidEvents).getInteger("score")

      //急减速>3G的次数
      var deceCount = 0
      //有急减速数据
      if (decelerationRapidTimes > 0) {
        //获取JsonArray
        val points: JSONArray = JSON.parseObject(deceRapidEvents).getJSONArray("points")
        //遍历数组
        val size = points.size
        for (i <- 0 to size - 1) {
          val decePointsAcceValue: Double = points.getJSONObject(i).getString("acceValue").toDouble
          if (decePointsAcceValue < -4) deceCount += 1
        }
      }

      //急转弯事件
      val suddenTurnEvents: String = jsonLine.getString("suddenTurnEvents")
      //急转弯次数
      val suddenTurnTimes: Int = JSON.parseObject(suddenTurnEvents).getInteger("times")
      //急转弯得分
      val suddenTurnScore: Int = JSON.parseObject(suddenTurnEvents).getInteger("score")

      //超速事件
      val overSpeedEvents: String = jsonLine.getString("overSpeedEvents")
      //超速得分
      val overSpeedScore: Int = JSON.parseObject(overSpeedEvents).getInteger("score")

      //疲劳驾驶事件
      val tiredDrivingEvents: String = jsonLine.getString("tiredDrivingEvents")
      //疲劳驾驶得分
      val tiredDrivingScore: Int = JSON.parseObject(tiredDrivingEvents).getInteger("score")

      //操控得分
      val controlScore: Int = jsonLine.getInteger("controlScore")
      //经济得分
      val economyScore: Int = jsonLine.getInteger("economyScore")
      //专注得分
      val focusScore: Int = jsonLine.getInteger("focusScore")
      //路况得分
      val roadScore: Int = jsonLine.getInteger("roadScore")
      //环境得分
      val envirScore: Int = jsonLine.getInteger("envirScore")
      (mileage, terminalId, accelerationRapidTimes, accelerationRapidScore, decelerationRapidTimes,
        decelerationRapidScore, suddenTurnTimes, suddenTurnScore, overSpeedScore, tiredDrivingScore,
        acceCount, deceCount, controlScore, economyScore, focusScore, roadScore, envirScore)
      //(mileage, terminalId, accelerationRapidTimes, accelerationRapidScore, decelerationRapidTimes, decelerationRapidScore, suddenTurnTimes, suddenTurnScore, overSpeedScore, tiredDrivingScore, acceCount, deceCount)
    })
    detectorDstream
  }
}
