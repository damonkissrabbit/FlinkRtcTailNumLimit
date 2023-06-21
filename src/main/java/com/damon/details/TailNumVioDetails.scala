package com.damon.details

import com.damon.utils.TimeHelper._
import com.damon.utils.{InitPropertiesUtil, JedisClusterUtil}
import net.minidev.json.JSONObject
import net.minidev.json.parser.JSONParser
import org.apache.flink.streaming.api.scala._
import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.runtime.state.filesystem.FsStateBackend
import org.apache.flink.streaming.api.{CheckpointingMode, TimeCharacteristic}
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.connectors.kafka.{FlinkKafkaConsumer010, FlinkKafkaProducer010}

import java.util.{Date, Properties}
import scala.collection.mutable.ArrayBuffer


object TailNumVioDetails {

  lazy val kafkaProp: Properties = InitPropertiesUtil.initKafkaProp
  lazy val basicProp: Properties = InitPropertiesUtil.initBasicProp
  lazy val businessProp: Properties = InitPropertiesUtil.initBusinessProp


  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

    // 每隔10s启动一个检查点
    env.enableCheckpointing(1000)
    // 精确获取一次（默认值）
    env.getCheckpointConfig.setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE)
    // 确保检查点之间有 5s 的进度
    env.getCheckpointConfig.setMinPauseBetweenCheckpoints(5000)
    // 检查点必须在 3 分钟之内完成或被丢弃
    env.getCheckpointConfig.setCheckpointTimeout(180000)
    // 同一时间只允许进行一个检查点
    env.getCheckpointConfig.setMaxConcurrentCheckpoints(1)
    // 设置 checkpoints 的保存目录
    env.setStateBackend(new FsStateBackend("hdfs:///flink/checkpoints"))

    val properties = new Properties()
    properties.setProperty("bootstrap.servers", kafkaProp.getProperty("bootstrap.servers"))
    properties.setProperty("security.protocol", kafkaProp.getProperty("security.protocol"))
    properties.setProperty("sasl.kerberos.service.name", kafkaProp.getProperty("sasl.kerberos.service.name"))
    properties.setProperty("group.id", kafkaProp.getProperty("group1.id"))

    // kafka topic 进， kafka topic 出
    val producer010 = new FlinkKafkaProducer010(kafkaProp.getProperty("target2.topic"), new SimpleStringSchema(), properties)

    val consumer010 = new FlinkKafkaConsumer010(kafkaProp.getProperty("source.topic"), new SimpleStringSchema(), properties)

    val dataStream = env.addSource(consumer010)

    dataStream
      .map(data => getRecord(data)) // (点位编码, 经过时间, 号牌类型, 号牌号码)
      .filter(_._1 != null) // 过滤错误记录 (null, null, null, null)
      .flatMap(data => getKeyAreaByPoint(data))
      .filter(_._1.nonEmpty)
      .map(data => recordProcess(data))
      .assignTimestampsAndWatermarks(new TimestampExtractor(basicProp.getProperty("max.lagged.time").toInt))
      .keyBy(0)
      .timeWindow(Time.seconds(basicProp.getProperty("job.interval").toInt))
      .reduce((v1, v2) => (v1._1, v1._2, v1._3, v1._4, v1._5, v1._6))
      .map(data => toJson(data))

  }

  def toJson(data: (String, String, String, String, Long, String)): String = {
    println("toJson: " + data._1 + " " + data._2 + " " + data._3 + " " + data._4 + " " + data._5 + " " + data._6)
    val obj: JSONObject = new JSONObject()
    val passTime = milliSecondToTimestampString(data._5)
    obj.put("area_id", data._1)
    obj.put("license_type", data._2)
    obj.put("license_num", data._3)
    obj.put("limit_rules", data._4)
    obj.put("passing_time", passTime)
    obj.put("point_id", data._6)

    println("---" + obj.toJSONString())
    obj.toJSONString()
  }

  def getRecord(in: String): (String, String, String, String) = {
    try {
      val jsonParser = new JSONParser(JSONParser.DEFAULT_PERMISSIVE_MODE)
      val elems: JSONObject = jsonParser.parse(in).asInstanceOf[JSONObject]
      val pointId = elems.get(businessProp.getProperty("point.id")).toString
      val passTime = elems.get(businessProp.getProperty("passing.time")).toString
      val licenseType = elems.get(businessProp.getProperty("license.type")).toString
      val licenseNum = elems.get(businessProp.getProperty("license.number")).toString
      val flag = if (isTailNumberLimitTime(getTimeMillis(passTime))) true else false

      // 车辆首次出现 过滤
      val isFirstCatchFlag = uniqueJudgeFlag(licenseType, licenseNum)

      if (flag)
        (pointId, passTime, licenseType, licenseNum)
      else
        (null, null, null, null)
    } catch {
      case e: Exception => println("*************** getRecord jsonParser error: " + e.getMessage)
        (null, null, null, null)

    }
  }

  def uniqueJudgeFlag(licenseType: String, licenseNum: String): Boolean = {
    if (!JedisClusterUtil.exists("mtdap3_tail_num_vio_detail")) {
      AnyRef.synchronized {
        JedisClusterUtil.sAdd("mtdap3_tail_num_vio_detail", licenseType + licenseNum) // 将当前车辆放进 set 集合中
        JedisClusterUtil.pexpireAt("mtdap3_tail_num_vio_detail", timestampOfMidnight(new Date())) // 设置key的过期时间
      }
      true
    } else {
      AnyRef.synchronized {
        // 为了保证过去设置生效，在新纪录到来时都检查一下
        if (JedisClusterUtil.ttl("mtdap3_tail_num_vio_detail").get == -1) {
          JedisClusterUtil.pexpireAt("mtdap3_tail_num_vio_detail", timestampOfMidnight(new Date())) // 设置key在午夜时分过期
        }

        if (JedisClusterUtil.isSetMember("mtdap3_tail_num_vio_detail", licenseType + licenseNum)){
          false  // 如果set集合中已经有当前车辆了，则不计数
        } else {
          JedisClusterUtil.sAdd("mtdap3_tail_num_vio_detail", licenseType + licenseNum)
          true // 否则先放进新车辆，然后返回true
        }
      }
    }
  }

  def isLocalVehicle(licenseNum: String): Boolean = {
    // 将本地车辆放入到redis中，可以表面配置文件中文编码的问题
    val localVehicleFlag = JedisClusterUtil.get("local_vehicle").get.split(",")
    var isLocal = false
    for (flag <- localVehicleFlag){
      if (licenseNum.contains(flag))
        isLocal = true
    }
    isLocal
  }


  def getKeyAreaByPoint(data: (String, String, String, String)): Array[(String, String, String, String, String)] = {
    val pointAndArea = JedisClusterUtil.hkeys("mtdap3_keyarea")
    val pointId = data._1
    val areas = getAreaId(pointAndArea, pointId)

    val aBuffer = new ArrayBuffer[(String, String, String, String, String)]()
    for (areaId <- areas) {
      val opt = JedisClusterUtil.hget("mtdap3_keyarea", pointId + "_" + areaId)
      if (opt.isDefined && !opt.get.equals("")) {
        val jsonParser = new JSONParser(JSONParser.DEFAULT_PERMISSIVE_MODE)
        val jsonObj: JSONObject = jsonParser.parse(opt.get).asInstanceOf[JSONObject]
        val element = (jsonObj.get("area_id").toString, pointId, data._2, data._3, data._4)
        aBuffer += element
      } else {
        val element = (null, null, null, null)
        aBuffer += element
      }
    }
    aBuffer.toArray
  }

  def getAreaId(pointAndArea: java.util.Iterator[String], pointId: String): Array[String] = {
    val areas = ArrayBuffer[String]()
    while (pointAndArea.hasNext) {
      val element = pointAndArea.next().split("_")
      if (element(0).equals(pointId)) {
        areas += element(1)
      }
    }
    areas.toArray
  }

  /**
   * 主体计算函数，将每条过车记录判断是否违反限行车辆
   *
   * @param data (区域编码， 卡口编码， 经过时间， 号牌类型，号牌号码)
   * @return （区域编码，号牌类型， 号牌号码， 限行规则（周一尾号限行（1和6）），经过时间，过车点位）
   */
  def recordProcess(data: (String, String, String, String, String)): (String, String, String, String, Long, String) = {
    try {
      // getTimeMillis 将字符串你解析为事件格式，然后获取毫秒数
      var passTime = getTimeMillis(data._3)
      // 修正时间错误
      if (passTime - System.currentTimeMillis() > 0) {
        passTime = System.currentTimeMillis()
      }
      val areaId = data._1
      val licenseType = data._4
      val licenseNum = data._5

      var limitRules = ""
      // 通过今天是星期几来获取限行规则，限行规则存放在 redis 中，通过 hkey 和 key 来获取
      val limitRulesArray = getLimitRules(passTime)
      var limitOne: String = ""
      var limitTwo: String = ""
      for (i <- limitRulesArray.indices) {
        i match {
          case 0 => limitOne = limitRulesArray(i)
          case 1 => limitTwo = limitRulesArray(i)
        }
      }
      limitRules = limitOne + "_" + limitTwo

      // 判断号牌是否违法
      val flag = if (isTailNumberVio(licenseNum, passTime)) true else false
      val pointId = data._2
      if (flag)
        (areaId, licenseType, licenseNum, limitRules.substring(5, 8), passTime, pointId)
      else
        (null, null, null, null, 0L, null)
    } catch {
      case e => println("************ getRecord jsonParser error: " + e.getMessage)
        (null, null, null, null, 0L, null)
    }

  }

  /**
   * 判断当前号牌是否违反尾号限行，调用judgeLastTypeIsInt
   *
   * @param licenseNumStr 号牌号码
   * @param passingTime   通过时间，需要这个是要通过时间来获取 限行的规则
   * @return
   */
  def isTailNumberVio(licenseNumStr: String, passingTime: Long): Boolean = {
    val vehLastNum = JudgeLastTypeIsInt(licenseNumStr)
    val limitRulesArray = getLimitRules(passingTime)

    var str = ""
    for (i <- limitRulesArray.indices) {
      str = str + limitRulesArray(i)
    }
    val flag = if (str.contains(vehLastNum)) true else false
    flag
  }

  def JudgeLastTypeIsInt(licenseNum: String): String = {
    import java.util.regex.Pattern
    val regEx = "[^0-9]"
    val pattern = Pattern.compile(regEx)
    val m = pattern.matcher(licenseNum)
    val result = m.replaceAll("").trim
    val vehLastNum = result.substring(result.length - 1)
    vehLastNum
  }

  /**
   * Description 获取配置的限行规则         redis Hash(1:1_6;2:2_7;3:3_8;4:4_9;5:5_0)
   */
  def getLimitRules(passingTime: Long): Array[String] = {
    val passTime = passingTime
    val time = milliSecondToTimestampString(passTime)
    val flag = dayOfWeek(time).toString // 返回数字 1-7

    // 获取redis中的当前工作日的限行号码
    //    val getLimitRules: Option[String] = JedisClusterUtil.hget("mtdap3_limit_rule", flag)
    //    val limitNum = ArrayBuffer[String]()
    //    for (rules <- getLimitRules) {
    //      val element = rules.split("_")
    //      limitNum += (element(0))
    //      limitNum += (element(1))
    //    }
    val getLimitRules: String = JedisClusterUtil.hget("mtdap3_limit_rule", flag).toString
    val limitNum = ArrayBuffer[String]()
    val element = getLimitRules.split("_")
    limitNum += (element(0))
    limitNum += (element(1))
    limitNum.toArray
  }

}
