import redis.clients.jedis.Jedis

class ActorSystemUtils extends Serializable {
  //端口
  val redisPort = 6379
  //测试外网
  //val redisHost = "118.89.60.46"
  //测试内网
  //val redisHost = "10.104.86.238"
  //正式内网
  val redisHost = "10.104.156.96"
  //其他环境
  //val redisHost = "192.168.171.235"

  def ini: Unit = {
    val Ini01 = new RedisUtils
    val jedisD = new Jedis(redisHost, redisPort)
    //实时级别的数据(统计当天的指标数据)更新策略
    Ini01.redisHourGet_23(jedisD)
    ///实时级别的数据(统计当天的指标数据),每天的数据都会在晚上0点清零
    Ini01.redisInitialize(jedisD)
    jedisD.close()
  }

  def hourGet: Unit = {
    val Ini02 = new RedisUtils
    val jedisH = new Jedis(redisHost, redisPort)
    //小时级
    /**************************************************************************
    lsm修改记录1
    修改每天凌晨小时级别数据为0的情况
    **************************************************************************/
    //修改之前
    //Ini02.redisHourGet(jedisH)
    //修改之后
    Ini02.redisHourGet_23(jedisH)
    jedisH.close()
  }

  def minuteGet: Unit = {
    val Ini03 = new RedisUtils
    val jedisH = new Jedis(redisHost, redisPort)
    //分钟级别
    /**************************************************************************
    lsm修改记录4
    修改每天凌晨小时级别数据为0的情况
      **************************************************************************/
    Ini03.redisMinuteGet(jedisH)
    jedisH.close()
  }
}
