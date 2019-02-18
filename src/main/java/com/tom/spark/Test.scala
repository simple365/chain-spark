package com.tom.spark

import com.alibaba.fastjson.{JSON, JSONArray, JSONObject}
import com.google.gson.JsonArray
import com.tom.spark.ChainTomStream.set2JsonString
import org.json4s.DefaultFormats
import org.json4s.jackson.Json

import scala.collection.mutable

object Test {

  def main(args: Array[String]): Unit = {
    val kstr = "{\"@timestamp\":\"2019-02-17T12:36:05.257Z\",\"beat\":{\"hostname\":\"hsq-2\",\"name\":\"hsq-2\",\"version\":\"5.3.1\"},\"input_type\":\"log\",\"message\":[{\"annotations\":[{\"endpoint\":{\"ipv4\":\"10.47.98.130\",\"port\":9229,\"serviceName\":\"hsq2\"},\"timestamp\":1550406964911335,\"value\":\"cs\"},{\"endpoint\":{\"ipv4\":\"10.47.98.130\",\"port\":9229,\"serviceName\":\"hsq2\"},\"timestamp\":1550406964911722,\"value\":\"cr\"}],\"binaryAnnotations\":[{\"endpoint\":{\"ipv4\":\"r-bp1a6949798c82c4.redis.rds.aliyuncs.com\",\"port\":6379,\"serviceName\":\"redis\"},\"key\":\"sa\",\"value\":\"true\"},{\"endpoint\":{\"ipv4\":\"10.47.98.130\",\"port\":9229,\"serviceName\":\"hsq2\"},\"key\":\"db.statement\",\"value\":\"connect r-bp1a6949798c82c4.redis.rds.ali...,6379,\"},{\"endpoint\":{\"ipv4\":\"10.47.98.130\",\"port\":9229,\"serviceName\":\"hsq2\"},\"key\":\"db.type\",\"value\":\"redis\"}],\"duration\":387,\"id\":\"ad79edf29ca348da594350b090ba2b48\",\"name\":\"connect\",\"parentId\":\"f8f34867b87b31094b5b12e4b8add9f6\",\"timestamp\":1550406964911335,\"traceId\":\"76d68a6f593b386780c80a8c01397f98\",\"version\":\"php-4\"},{\"annotations\":[{\"endpoint\":{\"ipv4\":\"10.47.98.130\",\"port\":9229,\"serviceName\":\"hsq2\"},\"timestamp\":1550406964911767,\"value\":\"cs\"},{\"endpoint\":{\"ipv4\":\"10.47.98.130\",\"port\":9229,\"serviceName\":\"hsq2\"},\"timestamp\":1550406964912190,\"value\":\"cr\"}],\"binaryAnnotations\":[{\"endpoint\":{\"ipv4\":\"r-bp1a6949798c82c4.redis.rds.aliyuncs.com\",\"port\":6379,\"serviceName\":\"redis\"},\"key\":\"sa\",\"value\":\"true\"},{\"endpoint\":{\"ipv4\":\"10.47.98.130\",\"port\":9229,\"serviceName\":\"hsq2\"},\"key\":\"db.statement\",\"value\":\"auth H3110W0r1d,\"},{\"endpoint\":{\"ipv4\":\"10.47.98.130\",\"port\":9229,\"serviceName\":\"hsq2\"},\"key\":\"db.type\",\"value\":\"redis\"}],\"duration\":423,\"id\":\"20394c8962a16aae1a7f39e59db6643f\",\"name\":\"auth\",\"parentId\":\"f8f34867b87b31094b5b12e4b8add9f6\",\"timestamp\":1550406964911767,\"traceId\":\"76d68a6f593b386780c80a8c01397f98\",\"version\":\"php-4\"},{\"annotations\":[{\"endpoint\":{\"ipv4\":\"10.47.98.130\",\"port\":9229,\"serviceName\":\"hsq2\"},\"timestamp\":1550406964912574,\"value\":\"cs\"},{\"endpoint\":{\"ipv4\":\"10.47.98.130\",\"port\":9229,\"serviceName\":\"hsq2\"},\"timestamp\":1550406964913151,\"value\":\"cr\"}],\"binaryAnnotations\":[{\"endpoint\":{\"ipv4\":\"r-bp1a6949798c82c4.redis.rds.aliyuncs.com\",\"port\":6379,\"serviceName\":\"redis\"},\"key\":\"sa\",\"value\":\"true\"},{\"endpoint\":{\"ipv4\":\"10.47.98.130\",\"port\":9229,\"serviceName\":\"hsq2\"},\"key\":\"db.statement\",\"value\":\"get k_uToken_v2:f2fc2fb5a612bc788ffa...,\"},{\"endpoint\":{\"ipv4\":\"10.47.98.130\",\"port\":9229,\"serviceName\":\"hsq2\"},\"key\":\"db.type\",\"value\":\"redis\"}],\"duration\":577,\"id\":\"4fb57ade95d9270cb29a8a1b0380e779\",\"name\":\"get\",\"parentId\":\"f8f34867b87b31094b5b12e4b8add9f6\",\"timestamp\":1550406964912574,\"traceId\":\"76d68a6f593b386780c80a8c01397f98\",\"version\":\"php-4\"},{\"annotations\":[{\"endpoint\":{\"ipv4\":\"10.47.98.130\",\"port\":9229,\"serviceName\":\"hsq2\"},\"timestamp\":1550406964913380,\"value\":\"cs\"},{\"endpoint\":{\"ipv4\":\"10.47.98.130\",\"port\":9229,\"serviceName\":\"hsq2\"},\"timestamp\":1550406964913943,\"value\":\"cr\"}],\"binaryAnnotations\":[{\"endpoint\":{\"ipv4\":\"r-bp1a6949798c82c4.redis.rds.aliyuncs.com\",\"port\":6379,\"serviceName\":\"redis\"},\"key\":\"sa\",\"value\":\"true\"},{\"endpoint\":{\"ipv4\":\"10.47.98.130\",\"port\":9229,\"serviceName\":\"hsq2\"},\"key\":\"db.statement\",\"value\":\"get k_openApiInterface:dwd\\\\openapi\\\\m...,\"},{\"endpoint\":{\"ipv4\":\"10.47.98.130\",\"port\":9229,\"serviceName\":\"hsq2\"},\"key\":\"db.type\",\"value\":\"redis\"}],\"duration\":563,\"id\":\"673b4178e599c29c53fec4f4e03254ca\",\"name\":\"get\",\"parentId\":\"f8f34867b87b31094b5b12e4b8add9f6\",\"timestamp\":1550406964913380,\"traceId\":\"76d68a6f593b386780c80a8c01397f98\",\"version\":\"php-4\"},{\"annotations\":[{\"endpoint\":{\"ipv4\":\"10.47.98.130\",\"port\":9229,\"serviceName\":\"hsq2\"},\"timestamp\":1550406964914037,\"value\":\"cs\"},{\"endpoint\":{\"ipv4\":\"10.47.98.130\",\"port\":9229,\"serviceName\":\"hsq2\"},\"timestamp\":1550406964914570,\"value\":\"cr\"}],\"binaryAnnotations\":[{\"endpoint\":{\"ipv4\":\"r-bp1a6949798c82c4.redis.rds.aliyuncs.com\",\"port\":6379,\"serviceName\":\"redis\"},\"key\":\"sa\",\"value\":\"true\"},{\"endpoint\":{\"ipv4\":\"10.47.98.130\",\"port\":9229,\"serviceName\":\"hsq2\"},\"key\":\"db.statement\",\"value\":\"incr k_openApiInterface:dwd\\\\openapi\\\\m...,\"},{\"endpoint\":{\"ipv4\":\"10.47.98.130\",\"port\":9229,\"serviceName\":\"hsq2\"},\"key\":\"db.type\",\"value\":\"redis\"}],\"duration\":533,\"id\":\"1544c90d507eb816f036bfaa55209b00\",\"name\":\"incr\",\"parentId\":\"f8f34867b87b31094b5b12e4b8add9f6\",\"timestamp\":1550406964914037,\"traceId\":\"76d68a6f593b386780c80a8c01397f98\",\"version\":\"php-4\"},{\"annotations\":[{\"endpoint\":{\"ipv4\":\"10.47.98.130\",\"port\":9229,\"serviceName\":\"hsq2\"},\"timestamp\":1550406964914640,\"value\":\"cs\"},{\"endpoint\":{\"ipv4\":\"10.47.98.130\",\"port\":9229,\"serviceName\":\"hsq2\"},\"timestamp\":1550406964915217,\"value\":\"cr\"}],\"binaryAnnotations\":[{\"endpoint\":{\"ipv4\":\"r-bp1a6949798c82c4.redis.rds.aliyuncs.com\",\"port\":6379,\"serviceName\":\"redis\"},\"key\":\"sa\",\"value\":\"true\"},{\"endpoint\":{\"ipv4\":\"10.47.98.130\",\"port\":9229,\"serviceName\":\"hsq2\"},\"key\":\"db.statement\",\"value\":\"get k_openApiInterface:dwd\\\\openapi\\\\m...,\"},{\"endpoint\":{\"ipv4\":\"10.47.98.130\",\"port\":9229,\"serviceName\":\"hsq2\"},\"key\":\"db.type\",\"value\":\"redis\"}],\"duration\":577,\"id\":\"fe299331cf28ac350a427e8f3bd314e0\",\"name\":\"get\",\"parentId\":\"f8f34867b87b31094b5b12e4b8add9f6\",\"timestamp\":1550406964914640,\"traceId\":\"76d68a6f593b386780c80a8c01397f98\",\"version\":\"php-4\"},{\"annotations\":[{\"endpoint\":{\"ipv4\":\"10.47.98.130\",\"port\":9229,\"serviceName\":\"hsq2\"},\"timestamp\":1550406964915274,\"value\":\"cs\"},{\"endpoint\":{\"ipv4\":\"10.47.98.130\",\"port\":9229,\"serviceName\":\"hsq2\"},\"timestamp\":1550406964915927,\"value\":\"cr\"}],\"binaryAnnotations\":[{\"endpoint\":{\"ipv4\":\"r-bp1a6949798c82c4.redis.rds.aliyuncs.com\",\"port\":6379,\"serviceName\":\"redis\"},\"key\":\"sa\",\"value\":\"true\"},{\"endpoint\":{\"ipv4\":\"10.47.98.130\",\"port\":9229,\"serviceName\":\"hsq2\"},\"key\":\"db.statement\",\"value\":\"incr k_openApiInterface:dwd\\\\openapi\\\\m...,\"},{\"endpoint\":{\"ipv4\":\"10.47.98.130\",\"port\":9229,\"serviceName\":\"hsq2\"},\"key\":\"db.type\",\"value\":\"redis\"}],\"duration\":653,\"id\":\"5ec3a81b0a431d9ea4837498f5d3cf4b\",\"name\":\"incr\",\"parentId\":\"f8f34867b87b31094b5b12e4b8add9f6\",\"timestamp\":1550406964915274,\"traceId\":\"76d68a6f593b386780c80a8c01397f98\",\"version\":\"php-4\"},{\"annotations\":[{\"endpoint\":{\"ipv4\":\"10.47.98.130\",\"port\":9229,\"serviceName\":\"hsq2\"},\"timestamp\":1550406964916025,\"value\":\"cs\"},{\"endpoint\":{\"ipv4\":\"10.47.98.130\",\"port\":9229,\"serviceName\":\"hsq2\"},\"timestamp\":1550406964916978,\"value\":\"cr\"}],\"binaryAnnotations\":[{\"endpoint\":{\"ipv4\":\"r-bp1a6949798c82c4.redis.rds.aliyuncs.com\",\"port\":6379,\"serviceName\":\"redis\"},\"key\":\"sa\",\"value\":\"true\"},{\"endpoint\":{\"ipv4\":\"10.47.98.130\",\"port\":9229,\"serviceName\":\"hsq2\"},\"key\":\"db.statement\",\"value\":\"get k_openApiInterface:dwd\\\\openapi\\\\m...,\"},{\"endpoint\":{\"ipv4\":\"10.47.98.130\",\"port\":9229,\"serviceName\":\"hsq2\"},\"key\":\"db.type\",\"value\":\"redis\"}],\"duration\":953,\"id\":\"cd92de022c80d99387e28ebf1c2d4a01\",\"name\":\"get\",\"parentId\":\"f8f34867b87b31094b5b12e4b8add9f6\",\"timestamp\":1550406964916025,\"traceId\":\"76d68a6f593b386780c80a8c01397f98\",\"version\":\"php-4\"},{\"annotations\":[{\"endpoint\":{\"ipv4\":\"10.47.98.130\",\"port\":9229,\"serviceName\":\"hsq2\"},\"timestamp\":1550406964917236,\"value\":\"cs\"},{\"endpoint\":{\"ipv4\":\"10.47.98.130\",\"port\":9229,\"serviceName\":\"hsq2\"},\"timestamp\":1550406964919230,\"value\":\"cr\"}],\"binaryAnnotations\":[{\"endpoint\":{\"ipv4\":\"r-bp1a6949798c82c4.redis.rds.aliyuncs.com\",\"port\":6379,\"serviceName\":\"redis\"},\"key\":\"sa\",\"value\":\"true\"},{\"endpoint\":{\"ipv4\":\"10.47.98.130\",\"port\":9229,\"serviceName\":\"hsq2\"},\"key\":\"db.statement\",\"value\":\"incr k_openApiInterface:dwd\\\\openapi\\\\m...,\"},{\"endpoint\":{\"ipv4\":\"10.47.98.130\",\"port\":9229,\"serviceName\":\"hsq2\"},\"key\":\"db.type\",\"value\":\"redis\"}],\"duration\":1994,\"id\":\"90b90fc8d6e9ecbfe7c4a7441a4884ad\",\"name\":\"incr\",\"parentId\":\"f8f34867b87b31094b5b12e4b8add9f6\",\"timestamp\":1550406964917236,\"traceId\":\"76d68a6f593b386780c80a8c01397f98\",\"version\":\"php-4\"},{\"annotations\":[{\"endpoint\":{\"ipv4\":\"10.47.98.130\",\"port\":9229,\"serviceName\":\"hsq2\"},\"timestamp\":1550406964919276,\"value\":\"cs\"},{\"endpoint\":{\"ipv4\":\"10.47.98.130\",\"port\":9229,\"serviceName\":\"hsq2\"},\"timestamp\":1550406964920371,\"value\":\"cr\"}],\"binaryAnnotations\":[{\"endpoint\":{\"ipv4\":\"r-bp1a6949798c82c4.redis.rds.aliyuncs.com\",\"port\":6379,\"serviceName\":\"redis\"},\"key\":\"sa\",\"value\":\"true\"},{\"endpoint\":{\"ipv4\":\"10.47.98.130\",\"port\":9229,\"serviceName\":\"hsq2\"},\"key\":\"db.statement\",\"value\":\"get k_openApiInterface:dwd\\\\openapi\\\\m...,\"},{\"endpoint\":{\"ipv4\":\"10.47.98.130\",\"port\":9229,\"serviceName\":\"hsq2\"},\"key\":\"db.type\",\"value\":\"redis\"}],\"duration\":1095,\"id\":\"07e358316c207eb1786dca8e7b987e7b\",\"name\":\"get\",\"parentId\":\"f8f34867b87b31094b5b12e4b8add9f6\",\"timestamp\":1550406964919276,\"traceId\":\"76d68a6f593b386780c80a8c01397f98\",\"version\":\"php-4\"},{\"annotations\":[{\"endpoint\":{\"ipv4\":\"10.47.98.130\",\"port\":9229,\"serviceName\":\"hsq2\"},\"timestamp\":1550406964920609,\"value\":\"cs\"},{\"endpoint\":{\"ipv4\":\"10.47.98.130\",\"port\":9229,\"serviceName\":\"hsq2\"},\"timestamp\":1550406964921121,\"value\":\"cr\"}],\"binaryAnnotations\":[{\"endpoint\":{\"ipv4\":\"r-bp1a6949798c82c4.redis.rds.aliyuncs.com\",\"port\":6379,\"serviceName\":\"redis\"},\"key\":\"sa\",\"value\":\"true\"},{\"endpoint\":{\"ipv4\":\"10.47.98.130\",\"port\":9229,\"serviceName\":\"hsq2\"},\"key\":\"db.statement\",\"value\":\"incr k_openApiInterface:dwd\\\\openapi\\\\m...,\"},{\"endpoint\":{\"ipv4\":\"10.47.98.130\",\"port\":9229,\"serviceName\":\"hsq2\"},\"key\":\"db.type\",\"value\":\"redis\"}],\"duration\":512,\"id\":\"2d4fd7ae29c00c1f593a080c93162509\",\"name\":\"incr\",\"parentId\":\"f8f34867b87b31094b5b12e4b8add9f6\",\"timestamp\":1550406964920609,\"traceId\":\"76d68a6f593b386780c80a8c01397f98\",\"version\":\"php-4\"},{\"annotations\":[{\"endpoint\":{\"ipv4\":\"10.47.98.130\",\"port\":9229,\"serviceName\":\"hsq2\"},\"timestamp\":1550406964907563,\"value\":\"sr\"},{\"endpoint\":{\"ipv4\":\"10.47.98.130\",\"port\":9229,\"serviceName\":\"hsq2\"},\"timestamp\":1550406964921788,\"value\":\"ss\"}],\"binaryAnnotations\":[{\"endpoint\":{\"ipv4\":\"10.47.98.130\",\"port\":9229,\"serviceName\":\"hsq2\"},\"key\":\"http.url\",\"value\":\"http://m.api.haoshiqi.net/market/specialtopiccomponentgroupskulist?token=f2fc2fb5a612bc788ffa708d3623176c\\u0026uid=90212841\\u0026zoneId=857\\u0026uuid=90212841\\u0026channel=alipay_shh\\u0026spm=alipay_shh/99zczeeho\\u0026v=2.4.8\\u0026terminal=aliapp\\u0026device=Xiaomi\\u0026swidth=360\\u0026sheight=572\\u0026net=4G\\u0026appId=2017112000051610\\u0026pageNum=1\\u0026pageLimit=20\\u0026needPagination=1\\u0026componentGroupId=683\\u0026topicCode=ef062a59791cf742a91b40ff02ba0495\"},{\"endpoint\":{\"ipv4\":\"10.47.98.130\",\"port\":9229,\"serviceName\":\"hsq2\"},\"key\":\"path\",\"value\":\"/data/app/openapi/current/index.php\"}],\"duration\":14225,\"id\":\"f8f34867b87b31094b5b12e4b8add9f6\",\"name\":\"GET\",\"timestamp\":1550406964907563,\"traceId\":\"76d68a6f593b386780c80a8c01397f98\",\"version\":\"php-4\"}],\"offset\":1324971444,\"source\":\"/data/logs/hsq-zipkin/zipkin.log\",\"type\":\"hsq-zipkin\"}"
//    val t=ChainTomStream.dealKafkaJson(kstr)
    println(1550010847543641L>Long.MaxValue)
    println("1550010847543641">"250010847543641")
    println("1550010847543641".length)
    for (i <- 0 to 3){
      println(i)
    }
    val t=ChainTomStream.dealKafkaJson(kstr)
    println(t)

    val orderIdsSet = mutable.HashSet[String]()
    orderIdsSet.add("test")
    orderIdsSet.add("be")
    val st=Json(DefaultFormats).write(orderIdsSet)
    println(st)

    val str1="{\"traceId\":\"415946b871a65c49f6be24d2430e648c\",\"maxDuration\":5419,\"timestamp\":1550010847543641,\"spanTotal\":6,\"orderIds\":[\"2184433692\",\"24433692\"],\"userIds\":[\"a\",\"b\"],\"serverSpan\":{\"mysql\":{\"subSpanTotal\":1,\"subMaxDuration\":1419},\"hsq2\":{\"subSpanTotal\":5,\"subMaxDuration\":5419}}}"
    val str2="{\"traceId\":\"415946b871a65c49f6be24d2430e648c\",\"maxDuration\":6419,\"timestamp\":2550010847543641,\"spanTotal\":2,\"orderIds\":[\"24433692\"],\"userIds\":[\"a\",\"c\"],\"serverSpan\":{\"mysql\":{\"subSpanTotal\":1,\"subMaxDuration\":2345},\"hsq2\":{\"subSpanTotal\":5,\"subMaxDuration\":5419}}}"

    val obj1=JSON.parseObject(str1)
    val obj2=JSON.parseObject(str2)
    println(testCompare(obj1,obj2))
  }
   def testCompare(obj1: JSONObject, obj2: JSONObject): JSONObject ={

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

   }
}
