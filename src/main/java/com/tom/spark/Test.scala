package com.tom.spark

import java.net.URL

import com.alibaba.fastjson.{JSON, JSONObject}
import com.google.gson.JsonParser
import org.elasticsearch.action.index.IndexRequest
import org.elasticsearch.common.xcontent.XContentType

import scala.collection.mutable

object Test {
  def main(args: Array[String]): Unit = {
    val kstr = "{\"@timestamp\":\"2019-02-17T12:36:05.257Z\",\"beat\":{\"hostname\":\"hsq-2\",\"name\":\"hsq-2\",\"version\":\"5.3.1\"},\"input_type\":\"log\",\"message\":[{\"annotations\":[{\"endpoint\":{\"ipv4\":\"10.47.98.130\",\"port\":9229,\"serviceName\":\"hsq2\"},\"timestamp\":1550406964911335,\"value\":\"cs\"},{\"endpoint\":{\"ipv4\":\"10.47.98.130\",\"port\":9229,\"serviceName\":\"hsq2\"},\"timestamp\":1550406964911722,\"value\":\"cr\"}],\"binaryAnnotations\":[{\"endpoint\":{\"ipv4\":\"r-bp1a6949798c82c4.redis.rds.aliyuncs.com\",\"port\":6379,\"serviceName\":\"redis\"},\"key\":\"sa\",\"value\":\"true\"},{\"endpoint\":{\"ipv4\":\"10.47.98.130\",\"port\":9229,\"serviceName\":\"hsq2\"},\"key\":\"db.statement\",\"value\":\"connect r-bp1a6949798c82c4.redis.rds.ali...,6379,\"},{\"endpoint\":{\"ipv4\":\"10.47.98.130\",\"port\":9229,\"serviceName\":\"hsq2\"},\"key\":\"db.type\",\"value\":\"redis\"}],\"duration\":387,\"id\":\"ad79edf29ca348da594350b090ba2b48\",\"name\":\"connect\",\"parentId\":\"f8f34867b87b31094b5b12e4b8add9f6\",\"timestamp\":1550406964911335,\"traceId\":\"76d68a6f593b386780c80a8c01397f98\",\"version\":\"php-4\"},{\"annotations\":[{\"endpoint\":{\"ipv4\":\"10.47.98.130\",\"port\":9229,\"serviceName\":\"hsq2\"},\"timestamp\":1550406964911767,\"value\":\"cs\"},{\"endpoint\":{\"ipv4\":\"10.47.98.130\",\"port\":9229,\"serviceName\":\"hsq2\"},\"timestamp\":1550406964912190,\"value\":\"cr\"}],\"binaryAnnotations\":[{\"endpoint\":{\"ipv4\":\"r-bp1a6949798c82c4.redis.rds.aliyuncs.com\",\"port\":6379,\"serviceName\":\"redis\"},\"key\":\"sa\",\"value\":\"true\"},{\"endpoint\":{\"ipv4\":\"10.47.98.130\",\"port\":9229,\"serviceName\":\"hsq2\"},\"key\":\"db.statement\",\"value\":\"auth H3110W0r1d,\"},{\"endpoint\":{\"ipv4\":\"10.47.98.130\",\"port\":9229,\"serviceName\":\"hsq2\"},\"key\":\"db.type\",\"value\":\"redis\"}],\"duration\":423,\"id\":\"20394c8962a16aae1a7f39e59db6643f\",\"name\":\"auth\",\"parentId\":\"f8f34867b87b31094b5b12e4b8add9f6\",\"timestamp\":1550406964911767,\"traceId\":\"76d68a6f593b386780c80a8c01397f98\",\"version\":\"php-4\"},{\"annotations\":[{\"endpoint\":{\"ipv4\":\"10.47.98.130\",\"port\":9229,\"serviceName\":\"hsq2\"},\"timestamp\":1550406964912574,\"value\":\"cs\"},{\"endpoint\":{\"ipv4\":\"10.47.98.130\",\"port\":9229,\"serviceName\":\"hsq2\"},\"timestamp\":1550406964913151,\"value\":\"cr\"}],\"binaryAnnotations\":[{\"endpoint\":{\"ipv4\":\"r-bp1a6949798c82c4.redis.rds.aliyuncs.com\",\"port\":6379,\"serviceName\":\"redis\"},\"key\":\"sa\",\"value\":\"true\"},{\"endpoint\":{\"ipv4\":\"10.47.98.130\",\"port\":9229,\"serviceName\":\"hsq2\"},\"key\":\"db.statement\",\"value\":\"get k_uToken_v2:f2fc2fb5a612bc788ffa...,\"},{\"endpoint\":{\"ipv4\":\"10.47.98.130\",\"port\":9229,\"serviceName\":\"hsq2\"},\"key\":\"db.type\",\"value\":\"redis\"}],\"duration\":577,\"id\":\"4fb57ade95d9270cb29a8a1b0380e779\",\"name\":\"get\",\"parentId\":\"f8f34867b87b31094b5b12e4b8add9f6\",\"timestamp\":1550406964912574,\"traceId\":\"76d68a6f593b386780c80a8c01397f98\",\"version\":\"php-4\"},{\"annotations\":[{\"endpoint\":{\"ipv4\":\"10.47.98.130\",\"port\":9229,\"serviceName\":\"hsq2\"},\"timestamp\":1550406964913380,\"value\":\"cs\"},{\"endpoint\":{\"ipv4\":\"10.47.98.130\",\"port\":9229,\"serviceName\":\"hsq2\"},\"timestamp\":1550406964913943,\"value\":\"cr\"}],\"binaryAnnotations\":[{\"endpoint\":{\"ipv4\":\"r-bp1a6949798c82c4.redis.rds.aliyuncs.com\",\"port\":6379,\"serviceName\":\"redis\"},\"key\":\"sa\",\"value\":\"true\"},{\"endpoint\":{\"ipv4\":\"10.47.98.130\",\"port\":9229,\"serviceName\":\"hsq2\"},\"key\":\"db.statement\",\"value\":\"get k_openApiInterface:dwd\\\\openapi\\\\m...,\"},{\"endpoint\":{\"ipv4\":\"10.47.98.130\",\"port\":9229,\"serviceName\":\"hsq2\"},\"key\":\"db.type\",\"value\":\"redis\"}],\"duration\":563,\"id\":\"673b4178e599c29c53fec4f4e03254ca\",\"name\":\"get\",\"parentId\":\"f8f34867b87b31094b5b12e4b8add9f6\",\"timestamp\":1550406964913380,\"traceId\":\"76d68a6f593b386780c80a8c01397f98\",\"version\":\"php-4\"},{\"annotations\":[{\"endpoint\":{\"ipv4\":\"10.47.98.130\",\"port\":9229,\"serviceName\":\"hsq2\"},\"timestamp\":1550406964914037,\"value\":\"cs\"},{\"endpoint\":{\"ipv4\":\"10.47.98.130\",\"port\":9229,\"serviceName\":\"hsq2\"},\"timestamp\":1550406964914570,\"value\":\"cr\"}],\"binaryAnnotations\":[{\"endpoint\":{\"ipv4\":\"r-bp1a6949798c82c4.redis.rds.aliyuncs.com\",\"port\":6379,\"serviceName\":\"redis\"},\"key\":\"sa\",\"value\":\"true\"},{\"endpoint\":{\"ipv4\":\"10.47.98.130\",\"port\":9229,\"serviceName\":\"hsq2\"},\"key\":\"db.statement\",\"value\":\"incr k_openApiInterface:dwd\\\\openapi\\\\m...,\"},{\"endpoint\":{\"ipv4\":\"10.47.98.130\",\"port\":9229,\"serviceName\":\"hsq2\"},\"key\":\"db.type\",\"value\":\"redis\"}],\"duration\":533,\"id\":\"1544c90d507eb816f036bfaa55209b00\",\"name\":\"incr\",\"parentId\":\"f8f34867b87b31094b5b12e4b8add9f6\",\"timestamp\":1550406964914037,\"traceId\":\"76d68a6f593b386780c80a8c01397f98\",\"version\":\"php-4\"},{\"annotations\":[{\"endpoint\":{\"ipv4\":\"10.47.98.130\",\"port\":9229,\"serviceName\":\"hsq2\"},\"timestamp\":1550406964914640,\"value\":\"cs\"},{\"endpoint\":{\"ipv4\":\"10.47.98.130\",\"port\":9229,\"serviceName\":\"hsq2\"},\"timestamp\":1550406964915217,\"value\":\"cr\"}],\"binaryAnnotations\":[{\"endpoint\":{\"ipv4\":\"r-bp1a6949798c82c4.redis.rds.aliyuncs.com\",\"port\":6379,\"serviceName\":\"redis\"},\"key\":\"sa\",\"value\":\"true\"},{\"endpoint\":{\"ipv4\":\"10.47.98.130\",\"port\":9229,\"serviceName\":\"hsq2\"},\"key\":\"db.statement\",\"value\":\"get k_openApiInterface:dwd\\\\openapi\\\\m...,\"},{\"endpoint\":{\"ipv4\":\"10.47.98.130\",\"port\":9229,\"serviceName\":\"hsq2\"},\"key\":\"db.type\",\"value\":\"redis\"}],\"duration\":577,\"id\":\"fe299331cf28ac350a427e8f3bd314e0\",\"name\":\"get\",\"parentId\":\"f8f34867b87b31094b5b12e4b8add9f6\",\"timestamp\":1550406964914640,\"traceId\":\"76d68a6f593b386780c80a8c01397f98\",\"version\":\"php-4\"},{\"annotations\":[{\"endpoint\":{\"ipv4\":\"10.47.98.130\",\"port\":9229,\"serviceName\":\"hsq2\"},\"timestamp\":1550406964915274,\"value\":\"cs\"},{\"endpoint\":{\"ipv4\":\"10.47.98.130\",\"port\":9229,\"serviceName\":\"hsq2\"},\"timestamp\":1550406964915927,\"value\":\"cr\"}],\"binaryAnnotations\":[{\"endpoint\":{\"ipv4\":\"r-bp1a6949798c82c4.redis.rds.aliyuncs.com\",\"port\":6379,\"serviceName\":\"redis\"},\"key\":\"sa\",\"value\":\"true\"},{\"endpoint\":{\"ipv4\":\"10.47.98.130\",\"port\":9229,\"serviceName\":\"hsq2\"},\"key\":\"db.statement\",\"value\":\"incr k_openApiInterface:dwd\\\\openapi\\\\m...,\"},{\"endpoint\":{\"ipv4\":\"10.47.98.130\",\"port\":9229,\"serviceName\":\"hsq2\"},\"key\":\"db.type\",\"value\":\"redis\"}],\"duration\":653,\"id\":\"5ec3a81b0a431d9ea4837498f5d3cf4b\",\"name\":\"incr\",\"parentId\":\"f8f34867b87b31094b5b12e4b8add9f6\",\"timestamp\":1550406964915274,\"traceId\":\"76d68a6f593b386780c80a8c01397f98\",\"version\":\"php-4\"},{\"annotations\":[{\"endpoint\":{\"ipv4\":\"10.47.98.130\",\"port\":9229,\"serviceName\":\"hsq2\"},\"timestamp\":1550406964916025,\"value\":\"cs\"},{\"endpoint\":{\"ipv4\":\"10.47.98.130\",\"port\":9229,\"serviceName\":\"hsq2\"},\"timestamp\":1550406964916978,\"value\":\"cr\"}],\"binaryAnnotations\":[{\"endpoint\":{\"ipv4\":\"r-bp1a6949798c82c4.redis.rds.aliyuncs.com\",\"port\":6379,\"serviceName\":\"redis\"},\"key\":\"sa\",\"value\":\"true\"},{\"endpoint\":{\"ipv4\":\"10.47.98.130\",\"port\":9229,\"serviceName\":\"hsq2\"},\"key\":\"db.statement\",\"value\":\"get k_openApiInterface:dwd\\\\openapi\\\\m...,\"},{\"endpoint\":{\"ipv4\":\"10.47.98.130\",\"port\":9229,\"serviceName\":\"hsq2\"},\"key\":\"db.type\",\"value\":\"redis\"}],\"duration\":953,\"id\":\"cd92de022c80d99387e28ebf1c2d4a01\",\"name\":\"get\",\"parentId\":\"f8f34867b87b31094b5b12e4b8add9f6\",\"timestamp\":1550406964916025,\"traceId\":\"76d68a6f593b386780c80a8c01397f98\",\"version\":\"php-4\"},{\"annotations\":[{\"endpoint\":{\"ipv4\":\"10.47.98.130\",\"port\":9229,\"serviceName\":\"hsq2\"},\"timestamp\":1550406964917236,\"value\":\"cs\"},{\"endpoint\":{\"ipv4\":\"10.47.98.130\",\"port\":9229,\"serviceName\":\"hsq2\"},\"timestamp\":1550406964919230,\"value\":\"cr\"}],\"binaryAnnotations\":[{\"endpoint\":{\"ipv4\":\"r-bp1a6949798c82c4.redis.rds.aliyuncs.com\",\"port\":6379,\"serviceName\":\"redis\"},\"key\":\"sa\",\"value\":\"true\"},{\"endpoint\":{\"ipv4\":\"10.47.98.130\",\"port\":9229,\"serviceName\":\"hsq2\"},\"key\":\"db.statement\",\"value\":\"incr k_openApiInterface:dwd\\\\openapi\\\\m...,\"},{\"endpoint\":{\"ipv4\":\"10.47.98.130\",\"port\":9229,\"serviceName\":\"hsq2\"},\"key\":\"db.type\",\"value\":\"redis\"}],\"duration\":1994,\"id\":\"90b90fc8d6e9ecbfe7c4a7441a4884ad\",\"name\":\"incr\",\"parentId\":\"f8f34867b87b31094b5b12e4b8add9f6\",\"timestamp\":1550406964917236,\"traceId\":\"76d68a6f593b386780c80a8c01397f98\",\"version\":\"php-4\"},{\"annotations\":[{\"endpoint\":{\"ipv4\":\"10.47.98.130\",\"port\":9229,\"serviceName\":\"hsq2\"},\"timestamp\":1550406964919276,\"value\":\"cs\"},{\"endpoint\":{\"ipv4\":\"10.47.98.130\",\"port\":9229,\"serviceName\":\"hsq2\"},\"timestamp\":1550406964920371,\"value\":\"cr\"}],\"binaryAnnotations\":[{\"endpoint\":{\"ipv4\":\"r-bp1a6949798c82c4.redis.rds.aliyuncs.com\",\"port\":6379,\"serviceName\":\"redis\"},\"key\":\"sa\",\"value\":\"true\"},{\"endpoint\":{\"ipv4\":\"10.47.98.130\",\"port\":9229,\"serviceName\":\"hsq2\"},\"key\":\"db.statement\",\"value\":\"get k_openApiInterface:dwd\\\\openapi\\\\m...,\"},{\"endpoint\":{\"ipv4\":\"10.47.98.130\",\"port\":9229,\"serviceName\":\"hsq2\"},\"key\":\"db.type\",\"value\":\"redis\"}],\"duration\":1095,\"id\":\"07e358316c207eb1786dca8e7b987e7b\",\"name\":\"get\",\"parentId\":\"f8f34867b87b31094b5b12e4b8add9f6\",\"timestamp\":1550406964919276,\"traceId\":\"76d68a6f593b386780c80a8c01397f98\",\"version\":\"php-4\"},{\"annotations\":[{\"endpoint\":{\"ipv4\":\"10.47.98.130\",\"port\":9229,\"serviceName\":\"hsq2\"},\"timestamp\":1550406964920609,\"value\":\"cs\"},{\"endpoint\":{\"ipv4\":\"10.47.98.130\",\"port\":9229,\"serviceName\":\"hsq2\"},\"timestamp\":1550406964921121,\"value\":\"cr\"}],\"binaryAnnotations\":[{\"endpoint\":{\"ipv4\":\"r-bp1a6949798c82c4.redis.rds.aliyuncs.com\",\"port\":6379,\"serviceName\":\"redis\"},\"key\":\"sa\",\"value\":\"true\"},{\"endpoint\":{\"ipv4\":\"10.47.98.130\",\"port\":9229,\"serviceName\":\"hsq2\"},\"key\":\"db.statement\",\"value\":\"incr k_openApiInterface:dwd\\\\openapi\\\\m...,\"},{\"endpoint\":{\"ipv4\":\"10.47.98.130\",\"port\":9229,\"serviceName\":\"hsq2\"},\"key\":\"db.type\",\"value\":\"redis\"}],\"duration\":512,\"id\":\"2d4fd7ae29c00c1f593a080c93162509\",\"name\":\"incr\",\"parentId\":\"f8f34867b87b31094b5b12e4b8add9f6\",\"timestamp\":1550406964920609,\"traceId\":\"76d68a6f593b386780c80a8c01397f98\",\"version\":\"php-4\"},{\"annotations\":[{\"endpoint\":{\"ipv4\":\"10.47.98.130\",\"port\":9229,\"serviceName\":\"hsq2\"},\"timestamp\":1550406964907563,\"value\":\"sr\"},{\"endpoint\":{\"ipv4\":\"10.47.98.130\",\"port\":9229,\"serviceName\":\"hsq2\"},\"timestamp\":1550406964921788,\"value\":\"ss\"}],\"binaryAnnotations\":[{\"endpoint\":{\"ipv4\":\"10.47.98.130\",\"port\":9229,\"serviceName\":\"hsq2\"},\"key\":\"http.url\",\"value\":\"http://m.api.haoshiqi.net/market/specialtopiccomponentgroupskulist?token=f2fc2fb5a612bc788ffa708d3623176c\\u0026uid=90212841\\u0026zoneId=857\\u0026uuid=90212841\\u0026channel=alipay_shh\\u0026spm=alipay_shh/99zczeeho\\u0026v=2.4.8\\u0026terminal=aliapp\\u0026device=Xiaomi\\u0026swidth=360\\u0026sheight=572\\u0026net=4G\\u0026appId=2017112000051610\\u0026pageNum=1\\u0026pageLimit=20\\u0026needPagination=1\\u0026componentGroupId=683\\u0026topicCode=ef062a59791cf742a91b40ff02ba0495\"},{\"endpoint\":{\"ipv4\":\"10.47.98.130\",\"port\":9229,\"serviceName\":\"hsq2\"},\"key\":\"path\",\"value\":\"/data/app/openapi/current/index.php\"}],\"duration\":14225,\"id\":\"f8f34867b87b31094b5b12e4b8add9f6\",\"name\":\"GET\",\"timestamp\":1550406964907563,\"traceId\":\"76d68a6f593b386780c80a8c01397f98\",\"version\":\"php-4\"}],\"offset\":1324971444,\"source\":\"/data/logs/hsq-zipkin/zipkin.log\",\"type\":\"hsq-zipkin\"}"
    val t=dealKafkaJson(kstr)
    println(t)
  }

  /**
    * 很重要的代码
    *
    * @param msgStr
    * @return
    */
  def dealMsgJson(msgStr: String): String = {
    var listStr = ""
    try {
      val jsonArray = JSON.parseArray(msgStr)
      var maxDuration = 0
      var traceId = ""
      var beginTimestamp = ""
      val spanTotal = jsonArray.size
      val orderIdsSet = mutable.HashSet[String]()
      val userIdsSet = mutable.HashSet[String]()
//所有服务的集合
      val serviceMaps = mutable.HashMap[String, JSONObject]()

      for (i <- 0 to jsonArray.size()-1) {
        val chainSubObj = jsonArray.getJSONObject(i)
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
        for (n <- 0 to binArr.size()-1) {
          val binJsonObject = binArr.getJSONObject(n)
          if (n == 0) {
            var subSpanTotal = 1
            serviceName = binJsonObject.getJSONObject("endpoint").getString("serviceName")
            if (serviceMaps.contains(serviceName)) {
              subMaxDuration = if (serviceMaps.get(serviceName).get.getInteger("subMaxDuration") > duration) serviceMaps.get(serviceName).get.getInteger("subMaxDuration")
              else duration
              subSpanTotal = serviceMaps.get(serviceName).get.getInteger("subSpanTotal") + 1
            }
            val durationSpanTotal=new JSONObject
            durationSpanTotal.put("subMaxDuration", subMaxDuration)
            durationSpanTotal.put("subSpanTotal", subSpanTotal)
            serviceMaps.put(serviceName, durationSpanTotal)
          }
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
        if (host.length > 0 && status >= 300) {
          if (path.length > 1 && path.substring(0, 2) .equals("//")) path = path.substring(1, path.length)
          chainSubObj.put("serverCenter", host + path)
          chainSubObj.put("status", status)
        }
        val timestamp = chainSubObj.get("timestamp").toString
        val id = chainSubObj.get("id").toString
        val day = ChainJson.getDay(timestamp)
        val indexRequest = new IndexRequest("hsq-zipkin-detail-" + day, "doc", id)
        EsClientUtil.bulkAdd(indexRequest.source(chainSubObj.toString, XContentType.JSON))
      }
      var extStr = ""
      if (!userIdsSet.isEmpty || !orderIdsSet.isEmpty) {
        val userIdsJson = userIdsSet.toString()
        val orderIdsJson = orderIdsSet.toString()
        if (!userIdsJson.isEmpty) extStr += ",\"userIds\":" + userIdsJson
        if (!orderIdsJson.isEmpty) extStr += ",\"orderIds\":" + orderIdsJson
        println(orderIdsJson,orderIdsJson)
      }
      val serverNmaeStr = ",\"serverSpan\":" + serviceMaps.toString()
      serviceMaps.clear
      listStr = "{\"traceId\":\"" + traceId + "\",\"maxDuration\":" + maxDuration + ",\"timestamp\":" + beginTimestamp + ",\"spanTotal\":" + spanTotal + extStr + serverNmaeStr + "}"
      userIdsSet.clear
      orderIdsSet.clear
    } catch {
      case e: Exception =>
        e.printStackTrace()
    }
    //        System.out.println("--------->dealJson:"+ listStr);
    //        JKafkaUtil.Producer("hsq-zipkin-list", listStr);
    listStr
  }

  /**
    * 第一次聚合，consumer kafka
    *
    * @param chainStr
    * @return
    */
  def dealKafkaJson(chainStr: String): String = {
    var resStr = ""
    if (chainStr != null && !chainStr.isEmpty) {
      if (chainStr.substring(0, 1) == "{") {
        val obj = new JsonParser().parse(chainStr).getAsJsonObject
        if (obj.has("message")) {
          val msg = obj.get("message").toString
          if (msg.substring(0, 1) == "[") {
            resStr = dealMsgJson(msg)
          }
        }
      }
      else if (chainStr.substring(0, 1) == "[") {
        resStr = dealMsgJson(chainStr)
      }
    }
    resStr
  }
}
