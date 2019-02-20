package com.tom.spark

import com.alibaba.fastjson.JSON
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.SparkConf
import org.apache.spark.streaming.kafka010.{ConsumerStrategies, KafkaUtils, LocationStrategies}
import org.apache.spark.streaming.{Seconds, State, StateSpec, StreamingContext}

import scala.collection.mutable.Map

object StreamingHsqChain {

//  private val logger: Logger = LoggerFactory.getLogger(StreamingHsqChain.getClass)
//  val sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")

  def main (args: Array[String]): Unit = {



    val groupId    = args(0)
    val second     = args(1)
    val offsetType = args(2)
    val appName    = args(3)
    val ckPoint    = args(4)


    val initJson   = "{\"traceId\":\"0\",\"maxDuration\":0,\"timestamp\":0, \"spanTotal\":0, \"userIds\":[],\"orderIds\":[],\"serverSpan\":{}}"
    val topic      = "hsq-zipkin"
//    val checkpointPath = "./chainck1"
    val kafkaUrl   = "10.0.0.211:9211,10.0.0.212:9212,10.0.0.213:9213,10.0.0.214:9214,10.0.0.215:9215"

    val checkpointPath = "hdfs://10.0.0.215:8020/home/hadoop/"+ckPoint



    // 从初始状态读取


    val mappingFunc = (key: String, one: Option[String], state: State[String]) => {
      val value = one.getOrElse(initJson)
      val stateValue = state.getOption.getOrElse(initJson)
      val res = "{\"traceId\":\"0\",\"maxDuration\":0,\"timestamp\":"+System.currentTimeMillis()+",\"spanTotal\":0,\"userIds\":[],\"orderIds\":[],\"serverSpan\":{}}"
//      val res = ChainJson.upsertJson(stateValue, value)
      val output = (key,stateValue)
      state.update(stateValue)
      output
    }


    def isJson(json:String):Boolean ={
      var res:Boolean = true
      try {
        val obj = JSON.parseObject(json)
      }catch {
        case e: Exception => println("======>"+e)
          res = false
      }
      res
    }


    def funcToCreateSSC(): StreamingContext = {
      val conf = new SparkConf().setAppName(appName)
                                .set("spark.streaming.kafka.consumer.cache.enabled", "false")
                                .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
                                .set("spark.kryo.registrator", "net.haoshiqi.java.ChainRegistrator")
                                // 限制从kafka中每秒每个分区拉取的数据量
                                .set("spark.streaming.kafka.maxRatePerPartition", "8000")
//     conf.setMaster("local[*]")

      // 集群调优方面设置
        conf.set("spark.shuffle.io.maxRetries", "6")
            .set("spark.shuffle.io.retryWait", "10s")
            .set("spark.shuffle.sort.bypassMergeThreshold", "400")
            .set("spark.shuffle.file.buffer", "64")
            .set("spark.reducer.maxSizeInFlight", "96")
            .set("spark.locality.wait", "2")
            .set("auto.commit.interval.ms","1000")
            .set("session.timeout.ms","30000")
//            .set("spark.speculation","true")
//            .set("spark.speculation.interval","100")
//            .set("spark.speculation.quantile","0.75")
//            .set("spark.speculation.multiplier","1.5")


      val ssc  = new StreamingContext(conf, Seconds(second.toInt))
//      ssc.sparkContext.setLogLevel("debug")

      // todo 从es获得全局状态
      ssc.checkpoint(checkpointPath)

      val kafkaParams = Map[String, Object](
        "bootstrap.servers"  -> kafkaUrl,
        "key.deserializer"   -> classOf[StringDeserializer],
        "value.deserializer" -> classOf[StringDeserializer],
        "group.id"           -> groupId,
        "auto.offset.reset"  -> offsetType, // latest,earliest
        "enable.auto.commit" -> (true: java.lang.Boolean)
      )



      val topics = Array(topic)
      val initialRDD = ssc.sparkContext.parallelize(List(("0", initJson)))
      // 从kafka读取出来的流
      val kafaDirectStream = KafkaUtils.createDirectStream[String, String](
        ssc,
        // 在大多数情况下，您应该使用LocationStrategies.PreferConsistent如上所示。这将在可用的执行器之间均匀分配分区。
        LocationStrategies.PreferConsistent,
        ConsumerStrategies.Subscribe[String, String](topics, kafkaParams)
      )

      // 从kafka中读取数据后，过滤掉不为空掉数据
      val filtervalues = kafaDirectStream.flatMap(line => {
//        val jsonStr = ChainJson.dealKafkaJson(line.value(), "chain")
        val jsonStr=""
        val obj = JSON.parseObject(jsonStr)
        Some(obj)
      }).filter(_ != null)

      // 数据换成一个二元组
      val mapvalues = filtervalues.repartition(50).map(x => (x.get("traceId").toString(), x.toString))

      // 状态数据进行累加
      val mapWithStateValues = mapvalues.mapWithState(StateSpec.function(mappingFunc).initialState(initialRDD))

      // 将最终的数据放入到es中。
      mapWithStateValues.foreachRDD(rdd => {
//          val offsetRanges = rdd.asInstanceOf[HasOffsetRanges].offsetRanges
          rdd.coalesce(500).foreachPartition(partitionOfRecords => {
            partitionOfRecords.foreach(pair => {
//              EsClientUtil.bulkListAdd(pair._2.toString)
            })
          })
        })
      ssc
    }

    val ssc = funcToCreateSSC()

    ssc.start()
    ssc.awaitTermination()
  }
}
