package OperateHbase

import org.apache.hadoop.hbase.client.HBaseAdmin
import org.apache.hadoop.hbase.mapreduce.TableInputFormat
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.hbase.{HBaseConfiguration, HTableDescriptor, TableName}
import org.apache.spark.{SparkConf, SparkContext}


object testHBaseSelect {
  def main(args: Array[String]): Unit = {

    val sparkConf = new SparkConf()
      .setAppName("HBaseTest")
      .setMaster("local[2]")
    val sc = new SparkContext(sparkConf)

    val tablename = "bd_spark_gps_tenMinutes"
    val conf = HBaseConfiguration.create()
    //设置zooKeeper集群地址，也可以通过将hbase-site.xml导入classpath，但是建议在程序里这样设置
    conf.set("hbase.zookeeper.quorum","testmaster,testslave01,testslave02")
    conf.set("hbase.rootdir","hdfs://testmaster:8020/hbase")
    //设置zookeeper连接端口，默认2181
    //conf.set("hbase.zookeeper.property.clientPort", "2181")
    conf.set(TableInputFormat.INPUT_TABLE, tablename)

    // 如果表不存在则创建表
    val admin = new HBaseAdmin(conf)
    if (!admin.isTableAvailable(tablename)) {
      val tableDesc = new HTableDescriptor(TableName.valueOf(tablename))
      admin.createTable(tableDesc)
    }

    //读取数据并转化成rdd
    val hBaseRDD = sc.newAPIHadoopRDD(conf, classOf[TableInputFormat],
      classOf[org.apache.hadoop.hbase.io.ImmutableBytesWritable],
      classOf[org.apache.hadoop.hbase.client.Result])

    val count = hBaseRDD.count()
    println(count)

    hBaseRDD.foreach{case (_,result) =>{
      //获取行键
      val key = Bytes.toString(result.getRow)

      //通过列族和列名获取列
      val frequency = Bytes.toString(result.getValue("info".getBytes,"frequency".getBytes))
      val totalNum = Bytes.toString(result.getValue("info".getBytes,"totalNum".getBytes))
      val weakNum = Bytes.toString(result.getValue("info".getBytes,"weakNum".getBytes))
      val zeroSpeedNum = Bytes.toString(result.getValue("info".getBytes,"zeroSpeedNum".getBytes))
      val notOpenGpsNum = Bytes.toString(result.getValue("info".getBytes,"notOpenGpsNum".getBytes))
      val onlineNum = Bytes.toString(result.getValue("info".getBytes,"onlineNum".getBytes))
      //println("Row key:"+key+"weakNum:"+weakNum)
      println("Row key:"+key+" totalNum:"+totalNum+" weakNum:"+weakNum +" zeroSpeedNum:"+weakNum+zeroSpeedNum+" frequency:"+frequency+" notOpenGpsNum:"+notOpenGpsNum+" onlineNum:"+onlineNum)

    }}

    sc.stop()
    admin.close()

  }
}
