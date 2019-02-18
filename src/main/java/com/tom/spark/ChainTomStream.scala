package com.tom.spark

import java.net.URL

import com.alibaba.fastjson.{JSON, JSONArray, JSONObject}
import com.google.gson.JsonParser
import org.elasticsearch.action.index.IndexRequest
import org.elasticsearch.common.xcontent.XContentType
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.SparkConf
import org.apache.spark.streaming._
import org.apache.spark.streaming.kafka010.{ConsumerStrategies, KafkaUtils, LocationStrategies}

import scala.collection.mutable
import scala.collection.mutable.Map

object ChainTomStream {

  //  private val logger: Logger = LoggerFactory.getLogger(StreamingHsqChain.getClass)
  //  val sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")


  def main(args: Array[String]): Unit = {
    val groupId = args(0)
    val second = args(1)
    val offsetType = args(2)
    val appName = args(3)
    val ckPoint = args(4)

    val topic = "hsq-zipkin"
    val kafkaUrl = "10.0.0.211:9211,10.0.0.212:9212,10.0.0.213:9213,10.0.0.214:9214,10.0.0.215:9215"
    val checkpointPath = "hdfs://10.0.0.215:8020/home/hadoop/" + ckPoint


    val conf = new SparkConf().setAppName(appName)
      .set("spark.streaming.kafka.consumer.cache.enabled", "false")
      .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .set("spark.kryo.registrator", "net.haoshiqi.java.ChainRegistrator")
      // 限制从kafka中每秒每个分区拉取的数据量
      .set("spark.streaming.kafka.maxRatePerPartition", "8000")
    //     conf.setMaster("local[*]")

    val ssc = new StreamingContext(conf, Seconds(second.toInt))
    ssc.checkpoint(checkpointPath)

    val kafkaParams = Map[String, Object](
      "bootstrap.servers" -> kafkaUrl,
      "key.deserializer" -> classOf[StringDeserializer],
      "value.deserializer" -> classOf[StringDeserializer],
      "group.id" -> groupId,
      "auto.offset.reset" -> offsetType, // latest,earliest
      "enable.auto.commit" -> (true: java.lang.Boolean)
    )
    val topics = Array(topic)

    // 从kafka读取出来的流
    val kafaDirectStream = KafkaUtils.createDirectStream[String, String](
      ssc,
      // 在大多数情况下，您应该使用LocationStrategies.PreferConsistent如上所示。这将在可用的执行器之间均匀分配分区。
      LocationStrategies.PreferConsistent,
      ConsumerStrategies.Subscribe[String, String](topics, kafkaParams)
    )

    // 从kafka中读取数据后，先解析出来详细数据，插入es
    kafaDirectStream.map(line => dealDetailJson(line.value()))

    // 每次处理上几分钟的数据
    val mapvalues = kafaDirectStream.window(Seconds(8 * 60)).map(line => {
      //      解析kafka里面的日志
      val res = dealKafkaJson(line.value())
      res
    }).filter(_ != None).repartition(200)
      .map(x => {
        //将traceId解析出来，然后进行聚合
        val resJsonObjectString = x.getOrElse("")
        val resJsonObject = JSON.parseObject(resJsonObjectString)
        var traceId = ""
        if (resJsonObject.containsKey("traceId")) traceId = resJsonObject.getString("traceId")
        (traceId, resJsonObject)
      }).filter(_._1 != "").reduceByKey((obj1: JSONObject, obj2: JSONObject) => {
      //   {
      //          "traceId": "415946b871a65c49f6be24d2430e648c",
      //          "maxDuration": 5419,
      //          "timestamp": 1550010847543641,
      //          "spanTotal": 6,
      //          "orderIds": ["2184433692"],
      //          "userIds":["a","b"],
      //          "serverSpan": {
      //            "mysql": {
      //              "subSpanTotal": 1,
      //              "subMaxDuration": 1419
      //            },
      //            "hsq2": {
      //              "subSpanTotal": 5,
      //              "subMaxDuration": 5419
      //            }
      //          }
      //        }
      val resJson = new JSONObject()
      //      将两个json合并到一起,用obj1作为返回结果了。
      val maxDuration = obj1.getInteger("maxDuration") + obj2.getInteger("maxDuration")
      resJson.put("maxDuration", maxDuration)
      //      算出最近一次该trace出现的时间,这个时间其实是16位
      val timestam1 = obj1.getLongValue("timestamp")
      val timestam2 = obj2.getLongValue("timestamp")
      val maxTimeStamp = if (timestam1 > timestam2) timestam1 else timestam2
      resJson.put("timestamp", maxTimeStamp)
      //      SPAN total
      val spanTotal = obj1.getInteger("spanTotal") + obj2.getInteger("spanTotal")
      resJson.put("spanTotal", spanTotal)
      //      order id
      val orderIdsSet = mutable.HashSet[String]()
      val orderIdsArray: JSONArray = new JSONArray()
      if (obj1.containsKey("orderIds")) obj1.getJSONArray("orderIds").toArray.foreach(k => orderIdsSet.add(k.toString))
      if (obj2.containsKey("orderIds")) obj2.getJSONArray("orderIds").toArray.foreach(k => orderIdsSet.add(k.toString))
      //      去重
      for (i <- orderIdsSet) orderIdsArray.add(i)
      resJson.put("orderIds", orderIdsArray)
      //      user id
      val usrIdSet = mutable.HashSet[String]()
      val usrIdArray = new JSONArray()
      if (obj1.containsKey("userIds")) obj1.getJSONArray("userIds").toArray.foreach(k => usrIdSet.add(k.toString))
      if (obj2.containsKey("userIds")) obj2.getJSONArray("userIds").toArray.foreach(k => usrIdSet.add(k.toString))
      for (i <- usrIdSet) usrIdArray.add(i)
      resJson.put("userIds", usrIdArray)
      //      serverSpan
      val serverSpan1 = obj1.getJSONObject("serverSpan")
      val serverSpan2 = obj2.getJSONObject("serverSpan")
      //      取最大的 subSpanTotal，subMaxDuration
      for (key1 <- serverSpan1.keySet().toArray) {
        val span1=serverSpan1.getJSONObject(key1.toString)
        val subSpanTotal1 = span1.getInteger("subSpanTotal")
        resJson.put("subSpanTotal",subSpanTotal1)
        val subMaxDuration1 = span1.getInteger("subMaxDuration")
        resJson.put("subMaxDuration", subMaxDuration1)
        for (key2 <- serverSpan2.keySet().toArray) {
          val span2=serverSpan2.getJSONObject(key2.toString)
          if (key1.toString == key2.toString) {
            val subSpanTotal2 = span2.getInteger("subSpanTotal")
            resJson.put("subSpanTotal", if (subSpanTotal1 > subSpanTotal2) subSpanTotal1 else subSpanTotal2)
            val subMaxDuration2 = span2.getInteger("subMaxDuration")
            resJson.put("subMaxDuration", if (subMaxDuration1 > subMaxDuration2) subMaxDuration1 else subMaxDuration2)
          }
        }
      }
      resJson
    })

    mapvalues.map(((x:String,y:JSONObject))=>y)

    // 将最终的数据放入到es中。
    //    mapvalues.foreachRDD(rdd => {
    //      rdd.coalesce(500).foreachPartition(partitionOfRecords => {
    //        partitionOfRecords.foreach(pair => {
    //          //          EsClientUtil.bulkListAdd(pair._2.toString)
    //        })
    //      })
    //    })

    ssc.start()
    ssc.awaitTermination()
  }

  /**
    * 解析出trace的详细信息，放入hsq-zipkin-detail-xxx   索引中
    *
    * @param msgStr
    */
  def dealDetailJson(msgStr: String) = {
    try {
      val chainJsonArray = JSON.parseArray(msgStr)
      //一行日志有很多个json串
      for (i <- 0 to chainJsonArray.size() - 1) {
        val chainSubObj = chainJsonArray.getJSONObject(i)
        // 后面处理的主要是binaryAnnotations解析出来的字符串进行处理
        val binArr = chainSubObj.getJSONArray("binaryAnnotations")
        var host = ""
        var path = ""
        var status = 200
        //获取subspan里面的数据
        for (n <- 0 to binArr.size() - 1) {
          val binJsonObject = binArr.getJSONObject(n)
          if (binJsonObject.containsKey("key")) {
            val key = binJsonObject.getString("key")
            if (key.equals("http.url")) {
              val url = new URL(binJsonObject.getString("value"))
              host = url.getHost
              path = url.getPath
            }
            if (key == "http.status") status = binJsonObject.getInteger("value")
          }
        }
        if (host.length > 0 && status >= 300) {
          if (path.length > 1 && path.substring(0, 2).equals("//")) path = path.substring(1, path.length)
          chainSubObj.put("serverCenter", host + path)
          chainSubObj.put("status", status)
        }
        val timestamp = chainSubObj.get("timestamp").toString
        val id = chainSubObj.getString("id")
        val day = ChainJson.getDay(timestamp)
        val indexRequest = new IndexRequest("hsq-zipkin-detail-" + day, "doc", id)
        EsClientUtil.bulkAdd(indexRequest.source(chainSubObj.toString, XContentType.JSON))
      }
    } catch {
      case e: Exception =>
        e.printStackTrace()
    }
  }

  /**
    * 处理传递过来的原始数据，将其处理转换成一个trace的信息。
    *
    * @param msgStr
    * @return
    */
  def dealMsgJson(msgStr: String): Option[String] = {
    val resultJson = new JSONObject()
    try {
      val chainJsonArray = JSON.parseArray(msgStr)
      //     这个方法返回的基本信息字段
      var maxDuration = 0
      var traceId = ""
      var beginTimestamp = ""
      val spanTotal = chainJsonArray.size
      val orderIdsSet = mutable.HashSet[String]()
      val userIdsSet = mutable.HashSet[String]()
      //所有服务的集合
      val serviceMaps = mutable.HashMap[String, JSONObject]()
      //一行日志有很多个json串
      for (i <- 0 to chainJsonArray.size() - 1) {
        val chainSubObj = chainJsonArray.getJSONObject(i)
        val duration = chainSubObj.getInteger("duration")
        if (i == 0) { // 如果i==0的话，获取时间戳和商务id
          beginTimestamp = chainSubObj.get("timestamp").toString()
          traceId = chainSubObj.get("traceId").toString()
        }
        // 获取最大的时间
        maxDuration = if (duration > maxDuration) duration else maxDuration
        // 后面处理的主要是binaryAnnotations解析出来的字符串进行处理
        val binArr = chainSubObj.getJSONArray("binaryAnnotations")
        var serviceName = ""
        var subMaxDuration = duration;
        var host = ""
        var path = ""
        var status = 200
        //获取subspan里面的数据
        for (n <- 0 to binArr.size() - 1) {
          val binJsonObject = binArr.getJSONObject(n)
          if (n == 0) {
            var subSpanTotal = 1
            serviceName = binJsonObject.getJSONObject("endpoint").getString("serviceName")
            if (serviceMaps.contains(serviceName)) {
              subMaxDuration = if (serviceMaps.get(serviceName).get.getInteger("subMaxDuration") > duration) serviceMaps.get(serviceName).get.getInteger("subMaxDuration")
              else duration
              subSpanTotal = serviceMaps.get(serviceName).get.getInteger("subSpanTotal") + 1
            }
            val durationSpanTotal = new JSONObject
            durationSpanTotal.put("subMaxDuration", subMaxDuration)
            durationSpanTotal.put("subSpanTotal", subSpanTotal)
            serviceMaps.put(serviceName, durationSpanTotal)
          }
          //          此处是拿userId和orderIds，还有其他的值
          if (binJsonObject.containsKey("key")) {
            val key = binJsonObject.getString("key")
            if (key.equals("http.url")) {
              val urlstr = binJsonObject.getString("value")
              val url = new URL(urlstr)
              host = url.getHost
              path = url.getPath
              val urlParser = URLParser.fromURL(urlstr).compile
              try {
                val userId = urlParser.getParameter("userId")
                if (userId != null) userIdsSet.add(userId)
                val userIds = urlParser.getParameter("userIds")
                if (userIds != null) {
                  userIds.split(",").foreach(x => userIdsSet.add(x))
                }
                val orderId = urlParser.getParameter("orderId")
                if (orderId != null) orderIdsSet.add(orderId)
                val orderIds = urlParser.getParameter("orderIds")
                if (orderIds != null) {
                  orderIds.split(",").foreach(x => orderIdsSet.add(x))
                }
              } catch {
                case ex: Exception =>
                  ex.printStackTrace()
              }
            }
            if (key == "http.status") status = binJsonObject.getInteger("value")
          }
        }
      }
      if (!userIdsSet.isEmpty || !orderIdsSet.isEmpty) {
        val userIdsArray: JSONArray = new JSONArray()
        userIdsSet.toArray.foreach(x => userIdsArray.add(x))
        resultJson.put("userIds", set2JsonString(userIdsSet))
        val orderIdsArray: JSONArray = new JSONArray()
        userIdsSet.toArray.foreach(x => orderIdsArray.add(x))
        resultJson.put("orderIds", orderIdsSet)
      }
      val serviceNameJson = new JSONObject()
      for ((x: String, y: JSONObject) <- serviceMaps) serviceNameJson.put(x, y)
      //      返回的一个jsonObject的数据
      resultJson.put("traceId", traceId)
      resultJson.put("maxDuration", maxDuration)
      resultJson.put("timestamp", beginTimestamp)
      resultJson.put("spanTotal", spanTotal)
      resultJson.put("serverSpan", serviceNameJson)
      //      清空
      serviceMaps.clear
      userIdsSet.clear
      orderIdsSet.clear
    } catch {
      case e: Exception =>
        e.printStackTrace()
    }
    Some(resultJson.toJSONString)
  }

  /**
    * 处理kafka中的数据
    *
    * @param chainStr
    * @return
    */
  def dealKafkaJson(chainStr: String): Option[String] = {
    var res: Option[String] = None
    if (chainStr != null && !chainStr.isEmpty) {
      if (chainStr.substring(0, 1) == "{") {
        val obj = new JsonParser().parse(chainStr).getAsJsonObject
        if (obj.has("message")) {
          val msg = obj.get("message").toString
          if (msg.substring(0, 1) == "[") {
            res = dealMsgJson(msg)
          }
        }
      }
      else if (chainStr.substring(0, 1) == "[") {
        res = dealMsgJson(chainStr)
      }
    }
    res
  }

  /**
    * 把一个set变成json string
    *
    * @param theSet
    * @return
    */
  def set2JsonString(theSet: mutable.HashSet[String]): String = {
    val orderIdsArray: JSONArray = new JSONArray()
    theSet.toArray.foreach(x => orderIdsArray.add(x))
    orderIdsArray.toJSONString
  }
}
