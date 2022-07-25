package com.imooc.flink

import org.apache.flink.api.scala.ExecutionEnvironment
import org.neo4j.driver.{AuthTokens, GraphDatabase}

/**
 * 任务3：
 * 每天定时更新主播等级
 * Created by xuwei
 */
object UpdateUserLevelScala {
  def main(args: Array[String]): Unit = {
    var filePath = "hdfs://bigdata01:9000/data/cl_level_user/20260201"
    var boltUrl = "bolt://bigdata04:7687"
    var userName = "neo4j"
    var passWord = "admin"
    if(args.length>0){
      filePath = args(0)
      boltUrl = args(1)
      userName = args(2)
      passWord = args(3)
    }

    val env = ExecutionEnvironment.getExecutionEnvironment
    //读取hdfs中的数据
    val text = env.readTextFile(filePath)

    //校验数据准确性
    val filterSet = text.filter(line => {
      val fields = line.split("\t")
      if (fields.length == 8 && !fields(0).equals("id")) {
        true
      } else {
        false
      }
    })

    //添加隐式转换代码
    import org.apache.flink.api.scala._

    //处理数据
    filterSet.mapPartition(it=>{
      //获取neo4j的连接
      val driver = GraphDatabase.driver(boltUrl, AuthTokens.basic(userName, passWord))
      //开启一个会话
      val session = driver.session()
      it.foreach(line=>{
        val fields = line.split("\t")
        //添加等级
        session.run("merge (u:User {uid:'"+fields(1).trim+"'}) set u.level = "+fields(3).trim)
      })
      //关闭会话
      session.close()
      //关闭连接
      driver.close()
      ""
    }).print()
  }

}
