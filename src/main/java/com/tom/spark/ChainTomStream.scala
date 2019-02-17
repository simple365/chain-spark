//package com.tom.spark
//
//import com.alibaba.fastjson.JSON
//import com.google.gson.JsonParser
////import com.tom.spark.ChainJson
//import org.apache.kafka.common.serialization.StringDeserializer
//import org.apache.spark.SparkConf
//import org.apache.spark.streaming.{Seconds, State, StateSpec, StreamingContext}
//import org.apache.spark.streaming.kafka010.{ConsumerStrategies, KafkaUtils, LocationStrategies}
//
//import scala.collection.mutable.Map
//
//object ChainTomStream {
//
//  //  private val logger: Logger = LoggerFactory.getLogger(StreamingHsqChain.getClass)
//  //  val sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
//
//  def main(args: Array[String]): Unit = {
//    val groupId = args(0)
//    val second = args(1)
//    val offsetType = args(2)
//    val appName = args(3)
//    val ckPoint = args(4)
//
//    val initJson = "{\"traceId\":\"0\",\"maxDuration\":0,\"timestamp\":0, \"spanTotal\":0, \"userIds\":[],\"orderIds\":[],\"serverSpan\":{}}"
//    val topic = "hsq-zipkin"
//    val kafkaUrl = "10.0.0.211:9211,10.0.0.212:9212,10.0.0.213:9213,10.0.0.214:9214,10.0.0.215:9215"
//    val checkpointPath = "hdfs://10.0.0.215:8020/home/hadoop/" + ckPoint
//
//
//    val conf = new SparkConf().setAppName(appName)
//      .set("spark.streaming.kafka.consumer.cache.enabled", "false")
//      .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
//      .set("spark.kryo.registrator", "net.haoshiqi.java.ChainRegistrator")
//      // 限制从kafka中每秒每个分区拉取的数据量
//      .set("spark.streaming.kafka.maxRatePerPartition", "8000")
//    //     conf.setMaster("local[*]")
//
//    val ssc = new StreamingContext(conf, Seconds(second.toInt))
//    //      ssc.sparkContext.setLogLevel("debug")
//
//    // todo 从es获得全局状态
//    ssc.checkpoint(checkpointPath)
//
//    val kafkaParams = Map[String, Object](
//      "bootstrap.servers" -> kafkaUrl,
//      "key.deserializer" -> classOf[StringDeserializer],
//      "value.deserializer" -> classOf[StringDeserializer],
//      "group.id" -> groupId,
//      "auto.offset.reset" -> offsetType, // latest,earliest
//      "enable.auto.commit" -> (true: java.lang.Boolean)
//    )
//
//
//    val topics = Array(topic)
//    val initialRDD = ssc.sparkContext.parallelize(List(("0", initJson)))
//
//    // 从kafka读取出来的流
//
//    val kafaDirectStream = KafkaUtils.createDirectStream[String, String](
//      ssc,
//      // 在大多数情况下，您应该使用LocationStrategies.PreferConsistent如上所示。这将在可用的执行器之间均匀分配分区。
//      LocationStrategies.PreferConsistent,
//      ConsumerStrategies.Subscribe[String, String](topics, kafkaParams)
//    )
//
//    // 从kafka中读取数据后，过滤掉不为空掉数据
//    val filtervalues = kafaDirectStream.flatMap(line => {
//      val jsonStr = dealKafkaJson(line.value(), "chain")
//      val obj = JSON.parseObject(jsonStr)
//      Some(obj)
//    }).filter(_ != null).window()
//
//
//    // 数据换成一个二元组
//    val mapvalues = filtervalues.repartition(50).map(x => (x.get("traceId").toString(), x.toString))
//
//
//    // 状态数据进行累加
//    // 从初始状态读取
//    val mappingFunc = (key: String, one: Option[String], state: State[String]) => {
//      val value = one.getOrElse(initJson)
//      val stateValue = state.getOption.getOrElse(initJson)
//      //      val res = "{\"traceId\":\"0\",\"maxDuration\":0,\"timestamp\":"+System.currentTimeMillis()+",\"spanTotal\":0,\"userIds\":[],\"orderIds\":[],\"serverSpan\":{}}"
//      val res = ChainJson.upsertJson(stateValue, value)
//      val output = (key, stateValue)
//      state.update(stateValue)
//      output
//    }
//    val mapWithStateValues = mapvalues.mapWithState(StateSpec.function(mappingFunc).initialState(initialRDD))
//
//    // 将最终的数据放入到es中。
//    mapWithStateValues.foreachRDD(rdd => {
//      //          val offsetRanges = rdd.asInstanceOf[HasOffsetRanges].offsetRanges
//      rdd.coalesce(500).foreachPartition(partitionOfRecords => {
//        partitionOfRecords.foreach(pair => {
//          //          EsClientUtil.bulkListAdd(pair._2.toString)
//        })
//      })
//    })
//
//    ssc.start()
//    ssc.awaitTermination()
//  }
//
//
//
//  /**
//    * 很重要的代码
//    *
//    * @param msgStr
//    * @return
//    */
//  def dealMsgJson(msgStr: String): String = {
//    var listStr = ""
//    try {
//      val jsonArray = JSON.parseArray(msgStr)
//      var maxDuration = 0
//      var traceId = ""
//      var beginTimestamp = ""
//      val spanTotal = jsonArray.size
//      var i = 0
//      while (i < jsonArray.size()) {
//        val chainSubObj= jsonArray.getJSONObject(i)
//        val duration =  chainSubObj.getInteger("duration")
//        if (i == 0) { // 如果i==0的话，获取时间戳和商务id
//          beginTimestamp = chainSubObj.get("timestamp").toString()
//          traceId = chainSubObj.get("traceId").toString()
//        }
//        // 获取最大的时间
//        maxDuration = if(duration > maxDuration) duration else maxDuration
//        // 后面处理的主要是binaryAnnotations解析出来的字符串进行处理
//        val binArr = chainSubObj.getJSONArray("binaryAnnotations")
//        val num = binArr.size
//        var serviceName = ""
//        var host = ""
//        var path = ""
//        var status = 200
//        var n = 0
//        while ( {
//          n < num
//        }) {
//          if (n == 0) {
//            var subSpanTotal = 1
//            val subMap = new Nothing
//            serviceName = binArr.get(n).getAsJsonObject.get("endpoint").getAsJsonObject.get("serviceName").getAsString
//            if (serviceMaps.containsKey(serviceName)) {
//              subMaxDuration = if (serviceMaps.get(serviceName).get("subMaxDuration").getAsInt > duration) serviceMaps.get(serviceName).get("subMaxDuration").getAsInt
//              else duration
//              subSpanTotal = serviceMaps.get(serviceName).get("subSpanTotal").getAsInt + 1
//            }
//            subMap.put("subMaxDuration", subMaxDuration)
//            subMap.put("subSpanTotal", subSpanTotal)
//            serviceMaps.put(serviceName, new Nothing().parse(gson.toJson(subMap)).getAsJsonObject)
//            subMap.clear
//          }
//          if (binArr.get(n).getAsJsonObject.has("key")) {
//            val key = binArr.get(n).getAsJsonObject.get("key").getAsString
//            if (key == "http.url") {
//              val urlstr = binArr.get(n).getAsJsonObject.get("value").getAsString
//              val url = new Nothing(urlstr)
//              host = url.getHost
//              path = url.getPath
//              val urlParser = URLParser.fromURL(urlstr).compile
//              try {
//                val userId = urlParser.getParameter("userId")
//                if (userId != null) userIdsSet.add(userId)
//                val userIds = urlParser.getParameter("userIds")
//                if (userIds != null) {
//                  val userArr = userIds.split(",")
//                  val tmpUserIdSet = new Nothing(Arrays.asList(userArr))
//                  userIdsSet.addAll(tmpUserIdSet)
//                }
//                val orderId = urlParser.getParameter("orderId")
//                if (orderId != null) orderIdsSet.add(orderId)
//                val orderIds = urlParser.getParameter("orderIds")
//                if (orderIds != null) {
//                  val orderIdArr = orderIds.split(",")
//                  val tmpOrderIdSet = new Nothing(Arrays.asList(orderIdArr))
//                  orderIdsSet.addAll(tmpOrderIdSet)
//                }
//              } catch {
//                case ex: Exception =>
//                  ex.printStackTrace()
//              }
//            }
//            if (key == "http.status") status = binArr.get(n).getAsJsonObject.get("value").getAsInt
//          }
//
//          {
//            n += 1;
//            n - 1
//          }
//        }
//        if (host.length > 0 && status >= 300) {
//          if (path.length > 1 && path.substring(0, 2) == "//") path = path.substring(1, path.length)
//          chainSubObj.addProperty("serverCenter", host + path)
//          chainSubObj.addProperty("status", status)
//        }
//        val timestamp = jsonObject.get("timestamp").toString
//        val id = jsonObject.get("id").toString
//        val day = getDay(timestamp)
//        val indexRequest = new IndexRequest("hsq-zipkin-detail-" + day, "doc", id)
//        EsClientUtil.bulkAdd(indexRequest.source(chainSubObj.toString, XContentType.JSON))
//
//        {
//          i += 1;
//          i - 1
//        }
//      }
//      var extStr = ""
//      if (!userIdsSet.isEmpty || !orderIdsSet.isEmpty) {
//        val userIdsJson = gson.toJson(userIdsSet)
//        val orderIdsJson = gson.toJson(orderIdsSet)
//        if (!userIdsJson.isEmpty) extStr += ",\"userIds\":" + userIdsJson
//        if (!orderIdsJson.isEmpty) extStr += ",\"orderIds\":" + orderIdsJson
//      }
//      val serverNmaeStr = ",\"serverSpan\":" + gson.toJson(serviceMaps)
//      serviceMaps.clear
//      listStr = "{\"traceId\":\"" + traceId + "\",\"maxDuration\":" + maxDuration + ",\"timestamp\":" + beginTimestamp + ",\"spanTotal\":" + spanTotal + extStr + serverNmaeStr + "}"
//      userIdsSet.clear
//      orderIdsSet.clear
//    } catch {
//      case e: Exception =>
//        e.printStackTrace()
//    }
//    //        System.out.println("--------->dealJson:"+ listStr);
//    //        JKafkaUtil.Producer("hsq-zipkin-list", listStr);
//    listStr
//  }
//
//  /**
//    * 第一次聚合，consumer kafka
//    *
//    * @param chainStr
//    * @return
//    */
//  def dealKafkaJson(chainStr: String): String = {
//    val resStr = ""
//    if (chainStr != null && !chainStr.isEmpty) {
//      if (chainStr.substring(0, 1) == "{") {
//        val obj = new JsonParser().parse(chainStr).getAsJsonObject
//        if (obj.has("message")) {
//          val msg = obj.get("message").toString
//          if (msg.substring(0, 1) == "[") {
//            resStr = dealMsgJson(msg)
//          }
//        }
//      }
//      else if (chainStr.substring(0, 1) == "[") {
//        resStr = dealMsgJson(chainStr)
//      }
//    }
//    resStr
//  }
//
//}
