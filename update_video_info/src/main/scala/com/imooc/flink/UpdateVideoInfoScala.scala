package com.imooc.flink

import com.alibaba.fastjson.JSON
import org.apache.flink.api.common.operators.Order
import org.apache.flink.api.scala.ExecutionEnvironment
import org.neo4j.driver.{AuthTokens, GraphDatabase}
import org.slf4j.LoggerFactory

/**
 * 任务5：
 * 每周一计算最近一个月主播视频评级
 * 把最近几次视频评级在3B+或2A+的主播，在neo4j中设置flag=1
 *
 * 注意：在执行程序之前，需要先把flag=1的重置为0
 * Created by lei
 */
object UpdateVideoInfoScala {
  val logger = LoggerFactory.getLogger("UpdateVideoInfoScala")
  def main(args: Array[String]): Unit = {
    var filePath = "hdfs://bigdata01:9000/data/video_info/20260201"
    var boltUrl = "bolt://bigdata04:7687"
    var userName = "neo4j"
    var passWord = "admin"
    if(args.length>0){
      filePath = args(0)
      boltUrl = args(1)
      userName = args(2)
      passWord = args(3)
    }

    //在Driver端执行此代码，将flag=1的值重置为0
    //获取neo4j的连接
    val driver = GraphDatabase.driver(boltUrl, AuthTokens.basic(userName, passWord))
    //开启一个会话
    val session = driver.session()
    session.run("match (a:User) where a.flag=1 set a.flag = 0")

    //关闭会话
    session.close()
    //关闭连接
    driver.close()

    val env = ExecutionEnvironment.getExecutionEnvironment
    //读取hdfs中的数据
    val text = env.readTextFile(filePath)

    //添加隐式转换代码
    import org.apache.flink.api.scala._

    //解析数据中的uid、rating、timestamp
    val tup3Set = text.map(line => {
      try {
        val jsonObj = JSON.parseObject(line)
        val uid = jsonObj.getString("uid")
        val rating = jsonObj.getString("rating")
        val timestamp: Long = jsonObj.getLong("timestamp")
        (uid, rating, timestamp)
      } catch {
        case ex: Exception => logger.error("json数据解析失败：" + line)
          ("0", "0", 0L)
      }
    })

    //过滤异常数据
    val filterSet = tup3Set.filter(_._2 != "0")

    //获取用户最近3场直播(视频)的评级信息
    val top3Set = filterSet.groupBy(0)
      .sortGroup(2, Order.DESCENDING)
      .reduceGroup(it => {
        val list = it.toList
        //(2002,A,1769913940002)	(2002,A,1769913940001)	(2002,A,1769913940000)
        //uid,rating,timestamp \t uid,rating,timestamp \t uid,rating,timestamp
        val top3 = list.take(3).mkString("\t")
        //(2002,(2002,A,1769913940002)	(2002,A,1769913940001)	(2002,A,1769913940000))
        (list.head._1, top3)
      })

    //过滤出来满足3场B+的数据
    val top3BSet = top3Set.filter(tup => {
      var flag = false
      val fields = tup._2.split("\t")
      if (fields.length == 3) {
        //3场B+，表示里面没有出现C和D
        val tmp_str = fields(0).split(",")(1) + "," + fields(1).split(",")(1) + "," + fields(2).split(",")(1)
        if (!tmp_str.contains("C") && !tmp_str.contains("D")) {
          flag = true
        }
      }
      flag
    })

    //把满足3场B+的数据更新到neo4j中，设置flag=1
    top3BSet.mapPartition(it=>{
      //获取neo4j的连接
      val driver = GraphDatabase.driver(boltUrl, AuthTokens.basic(userName, passWord))
      //开启一个会话
      val session = driver.session()
      it.foreach(tup=>{
        session.run("match (a:User {uid:'"+tup._1+"'}) where a.level >=15 set a.flag = 1")
      })
      //关闭会话
      session.close()
      //关闭连接
      driver.close()
      ""
    }).print()


    //过滤出来满足2场A+的数据
    val top2ASet = top3Set.filter(tup => {
      var flag = false
      val fields = tup._2.split("\t")
      if (fields.length >= 2) {
        //2场A+，获取最近两场直播评级，里面不能出现B、C、D
        val tmp_str = fields(0).split(",")(1) + "," + fields(1).split(",")(1)
        if (!tmp_str.contains("B") && !tmp_str.contains("C") && !tmp_str.contains("D")) {
          flag = true
        }
      }
      flag
    })

    //把满足2场A+的数据更新到neo4j中，设置flag=1
    top2ASet.mapPartition(it=>{
      //获取neo4j的连接
      val driver = GraphDatabase.driver(boltUrl, AuthTokens.basic(userName, passWord))
      //开启一个会话
      val session = driver.session()
      it.foreach(tup=>{
        session.run("match (a:User {uid:'"+tup._1+"'}) where a.level >=4 set a.flag = 1")
      })
      //关闭会话
      session.close()
      //关闭连接
      driver.close()
      ""
    }).print()

  }

}
