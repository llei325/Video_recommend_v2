#!/bin/bash
#默认获取上周一的时间
dt=`date -d "7 days ago" +"%Y%m%d"`
if [ "x$1" != "x" ]
then
    dt=`date -d "7 days ago $1" +"%Y%m%d"`
fi


masterUrl="yarn-cluster"
appName="GetRecommendListScala"`date +%s`
boltUrl="bolt://bigdata04:7687"
userName="neo4j"
passWord="admin"
#获取上周一的时间戳(单位：毫秒)
timestamp=`date --date="${dt}" +%s`000
#粉丝列表关注重合度
dumplicateNum=2
#主播等级
level=4

#输出结果数据路径
outputPath="hdfs://bigdata01:9000/data/recommend_data/${dt}"


#注意：需要将flink脚本路径配置到linux的环境变量中
flink run \
-m ${masterUrl} \
-ynm ${appName} \
-yqu default \
-yjm 1024 \
-ytm 1024 \
-ys 1 \
-p 5 \
-c com.imooc.flink.GetRecommendListScala \
/data/soft/video_recommend_v2/jobs/get_recommend_list-1.0-SNAPSHOT-jar-with-dependencies.jar ${appName} ${boltUrl} ${userName} ${passWord} ${timestamp} ${dumplicateNum} ${level} ${outputPath}

#验证任务执行状态
appStatus=`yarn application -appStates FINISHED -list | grep ${appName} | awk '{print $8}'`
if [ "${appStatus}" != "SUCCEEDED" ]
then
    echo "任务执行失败"
    # 发送短信或者邮件
else
    echo "任务执行成功"
fi