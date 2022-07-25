package com.imooc.flink

import org.apache.flink.api.scala.ExecutionEnvironment
import org.neo4j.driver.{AuthTokens, GraphDatabase}

import scala.collection.mutable.{ArrayBuffer, ListBuffer}

/**
 * 任务6：
 * 每周一计算最近一周内主活主播的三度关系列表
 * 注意：
 * 1：待推荐主播最近一周内活跃过
 * 2：待推荐主播等级>4
 * 3：待推荐主播最近1个月视频评级满足3B+或2A+(flag=1)
 * 4：待推荐主播的粉丝列表关注重合度>2
 * Created by lei
 */
object GetRecommendListScala {
  def main(args: Array[String]): Unit = {
    var appName = "GetRecommendListScala"
    var boltUrl = "bolt://bigdata04:7687"
    var userName = "neo4j"
    var passWord = "admin"
    var timestamp = 0L //过滤最近一周内是否活跃过
    var dumplicateNum = 2 //粉丝列表关注重合度
    var level = 4 //主播等级
    var outputPath = "hdfs://bigdata01:9000/data/recommend_data/20260201"
    if(args.length>0){
      appName = args(0)
      boltUrl = args(1)
      userName = args(2)
      passWord = args(3)
      timestamp = args(4).toLong
      dumplicateNum = args(5).toInt
      level = args(6).toInt
      outputPath = args(7)
    }

    val env = ExecutionEnvironment.getExecutionEnvironment
    //添加隐式转换代码
    import  org.apache.flink.api.scala._
    val param = Map("boltUrl"->boltUrl,"userName"->userName,"passWord"->passWord,"timestamp"->timestamp.toString,"level"->level.toString)
    //获取一周内主活的主播 并且主播等级大于4的数据
    val uidSet = env.createInput(new Neo4jInputFormat(param))

    //一次处理一批
    //过滤出粉丝关注重合度>2的数据，并且对关注重合度倒序排序
    //最终的数据格式是：主播id,待推荐的主播id
    val mapSet = uidSet.mapPartition(it=>{
      //获取neo4j的连接
      val driver = GraphDatabase.driver(boltUrl, AuthTokens.basic(userName, passWord))
      //开启一个会话
      val session = driver.session()
      //保存计算出来的结果
      val resultArr = ArrayBuffer[String]()
      it.foreach(uid=>{
        //计算一个用户的三度关系(主播的三度关系)
        //注意：数据量打了之后，这个计算操作是非常耗时
        val result = session.run("match (a:User {uid:'"+uid+"'}) <-[:follow]- (b:User) -[:follow]-> (c:User) return a.uid as auid,c.uid as cuid,count(c.uid) as sum order by sum desc limit 30")
        //对b、c的主活时间进行过滤，以及对c的level和flag值进行过滤
        /*val result = session.run("match (a:User {uid:'"+uid+"'}) <-[:follow]- (b:User) -[:follow]-> (c:User)" +
          " where b.timestamp >= "+timestamp+" and c.timestamp >= "+timestamp+" and c.level >= "+level +" and c.flag =1 " +
          " return a.uid as auid,c.uid as cuid,count(c.uid) as sum order by sum desc limit 30")*/
        while(result.hasNext){
          val record = result.next()
          val sum = record.get("sum").asInt()
          if(sum > dumplicateNum){
            resultArr += record.get("auid").asString()+"\t"+record.get("cuid").asString()
          }
        }
      })
      //关闭会话
      session.close()
      //关闭连接
      driver.close()
      resultArr.iterator
    })

    //把数据转成tupl2的形式
    val tup2Set = mapSet.map(line => {
      val splits = line.split("\t")
      (splits(0), splits(1))
    })

    //根据主播id进行分组，可以获取到这个主播的待推荐列表
    val reduceSet = tup2Set.groupBy(_._1).reduceGroup(it => {
      val list = it.toList
      val tmpList = ListBuffer[String]()
      for (l <- list) {
        tmpList += l._2
      }
      //把结果组装成这种形式 1001   1002,1003,1004
      (list.head._1, tmpList.toList.mkString(","))
    })

    //注意：writeAsCsv只能保存tuple类型的数据
    //writerAsText可以支持任何类型，如果是对象，会调用对象的toString方法写入到文件中
    reduceSet.writeAsCsv(outputPath,"\n","\t")

    //执行任务
    env.execute(appName)

  }

}
