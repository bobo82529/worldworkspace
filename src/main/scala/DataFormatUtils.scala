import java.text.SimpleDateFormat
import java.util.Date


/**
  * 工具类，主要返回各种类型的时间参数kunf
  */
class DataFormatUtils extends Serializable {

  //获取当前小时数(0-23)
  def getHour(): String = {
    val now: Date = new Date()
    val dateFormat = new SimpleDateFormat("HH")
    val date = dateFormat.format(now)
    date
  }

  //当前时间,时间类型为时间戳(1513242062)
  def getNow_time(): Long = {
    val now: Date = new Date()
    val dateFormat: SimpleDateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
    val a = dateFormat.parse(dateFormat.format(now)).getTime
    var str = a + ""
    str.substring(0, 10).toLong
  }

  //获取当前小时的时间类型为时间戳,比如现在2017-12-14 17:01:03,返回2017-12-14 17:00:00的时间戳
  def getHour_time(): Long = {
    val now: Date = new Date()
    val dateFormat = new SimpleDateFormat("yyyy-MM-dd HH")
    val a = dateFormat.parse(dateFormat.format(now)).getTime
    var str = a + ""
    str.substring(0, 10).toLong
  }

  //获取当天0点的时间戳，比如2017-12-14 00:00:00的时间戳
  def getZero_time(): Long = {
    val now = new Date()
    val dateFormat = new SimpleDateFormat("yyyy-MM-dd")
    val a = dateFormat.parse(dateFormat.format(now)).getTime
    var str = a + ""
    str.substring(0, 10).toLong
  }

  //获取当前分钟的时间类型3333为时间戳,比如现在2017-12-14 17:01:03,返回2017-12-14 17:01:00的时间戳
  def getMinute_time(): Long = {
    val now: Date = new Date()
    val dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm")
    val a = dateFormat.parse(dateFormat.format(now)).getTime
    var str = a + ""
    str.substring(0, 10).toLong
  }
}
