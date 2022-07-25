package com.imooc.flink

import java.util.Properties

import com.alibaba.fastjson.JSON
import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer

/**
 * 任务2：
 * 实时维护粉丝关注数据
 * Created by xuwei
 */
object RealTimeFollowScala {
  def main(args: Array[String]): Unit = {
    var appName = "RealTimeFollowScala"
    var kafkaBrokers = "bigdata01:9092,bigdata02:9092,bigdata03:9092"
    var groupId = "con_f_1"
    var topic = "user_follow"
    var boltUrl = "bolt://bigdata04:7687"
    var userName = "neo4j"
    var passWord = "admin"
    if(args.length>0){
      appName = args(0)
      kafkaBrokers = args(1)
      groupId = args(2)
      topic = args(3)
      boltUrl = args(4)
      userName = args(5)
      passWord = args(6)
    }

    val env = StreamExecutionEnvironment.getExecutionEnvironment

    //指定FlinkKafkaConsumer相关配置
    val prop = new Properties()
    prop.setProperty("bootstrap.servers",kafkaBrokers)
    prop.setProperty("group.id",groupId)
    val kafkaConsumer = new FlinkKafkaConsumer[String](topic, new SimpleStringSchema(), prop)
    //kafka consumer的消费策略设置
    kafkaConsumer.setStartFromGroupOffsets()

    //指定kafka作为source
    import org.apache.flink.api.scala._
    val text = env.addSource(kafkaConsumer)

    //解析json数据中的核心字段
    val tupStream = text.map(line => {
      val jsonObj = JSON.parseObject(line)
      val desc = jsonObj.getString("desc")
      val followerUid = jsonObj.getString("followeruid")
      val followUid = jsonObj.getString("followuid")
      (desc, followerUid, followUid)
    })

    //使用Neo4jSink维护粉丝关注数据
    val param = Map("boltUrl"->boltUrl,"userName"->userName,"passWord"->passWord)
    tupStream.addSink(new Neo4jSink(param))

    env.execute(appName)
  }

}
