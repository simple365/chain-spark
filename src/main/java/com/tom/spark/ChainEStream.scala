package com.tom.spark

import java.net.URL

import com.alibaba.fastjson.{JSON, JSONArray, JSONObject}
import com.google.gson.JsonParser
import org.apache.spark.streaming.kafka010.{CanCommitOffsets, HasOffsetRanges}
import org.slf4j.{Logger, LoggerFactory}
//import org.elasticsearch.action.index.IndexRequest
//import org.elasticsearch.common.xcontent.XContentType
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.SparkConf
import org.apache.spark.streaming._
import org.apache.spark.streaming.kafka010.{ConsumerStrategies, KafkaUtils, LocationStrategies}

import scala.collection.mutable
import scala.collection.mutable.Map

object ChainEStream {

    private val logger: Logger = LoggerFactory.getLogger(ChainEStream.getClass)
  //  val sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")


  def main(args: Array[String]): Unit = {
    val groupId = args(0)
    val second = args(1)
    val offsetType = args(2)
    val appName = args(3)
    val ckPoint = args(4)


//    val topic = "hsq-zipkin"
//    val kafkaUrl = "10.0.0.211:9211,10.0.0.212:9212,10.0.0.213:9213,10.0.0.214:9214,10.0.0.215:9215"
//    val checkpointPath = "hdfs://10.0.0.215:8020/home/hadoop/" + ckPoint
    val topic = "test"
    val kafkaUrl = "localhost:9092"
    val checkpointPath = "hdfs:///tmp/" + ckPoint


    val conf = new SparkConf().setAppName(appName)
//      .set("spark.streaming.kafka.consumer.cache.enabled", "false")
//      .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
//      .set("spark.kryo.registrator", "net.haoshiqi.java.ChainRegistrator")
      // 限制从kafka中每秒每个分区拉取的数据量
//      .set("spark.streaming.kafka.maxRatePerPartition", "8000")
         conf.setMaster("local[*]")

    val ssc = new StreamingContext(conf, Seconds(second.toInt))
//    ssc.checkpoint(checkpointPath)

    val kafkaParams = Map[String, Object](
      "bootstrap.servers" -> kafkaUrl,
      "key.deserializer" -> classOf[StringDeserializer],
      "value.deserializer" -> classOf[StringDeserializer],
      "group.id" -> groupId,
      "auto.offset.reset" -> offsetType, // latest,earliest
      "enable.auto.commit" -> (false: java.lang.Boolean)
    )
    val topics = Array(topic)

    // 从kafka读取出来的流
    val kafaDirectStream = KafkaUtils.createDirectStream[String, String](
      ssc,
      //位置策略（可用的Executor上均匀分配分区）
      LocationStrategies.PreferConsistent,
      //消费策略（订阅固定的主题集合）
      ConsumerStrategies.Subscribe[String, String](topics, kafkaParams)
    )

    // 从kafka中读取数据后，先解析出来详细数据，插入es
    kafaDirectStream.foreachRDD(rdd=>{
      //获取该RDD对于的偏移量
      val offsetRanges = rdd.asInstanceOf[HasOffsetRanges].offsetRanges
      rdd.map(line => {
        //拿出对应的数据
        var res: String = ""
        val chainStr=line.value()
        if (chainStr != null && !chainStr.isEmpty) {
          if (chainStr.substring(0, 1) == "{") {
            val obj = new JsonParser().parse(chainStr).getAsJsonObject
            if (obj.has("message")) {
              val msg = obj.get("message").toString
              if (msg.substring(0, 1) == "[") {
                res = dealDetailJson(msg)
              }
            }
          }
          else if (chainStr.substring(0, 1) == "[") {
            res = dealDetailJson(chainStr)
          }
        }
        res
      })
      //异步更新偏移量到kafka中
      kafaDirectStream.asInstanceOf[CanCommitOffsets].commitAsync(offsetRanges)

    })
    ssc.start()
    ssc.awaitTermination()
  }


  /**
    * 解析出trace的详细信息，放入hsq-zipkin-detail-xxx   索引中
    *
    * @param msgStr
    */
  def dealDetailJson(msgStr: String) = {
    val resArray=new JSONArray()
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
//        val indexRequest = new IndexRequest("hsq-zipkin-detail-" + day, "doc", id)
//        EsClientUtil.bulkAdd(indexRequest.source(chainSubObj.toString, XContentType.JSON))
        resArray.add(chainSubObj.toJSONString)
      }
    } catch {
      case e: Exception =>
        e.printStackTrace()
    }
    resArray.toJSONString
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
