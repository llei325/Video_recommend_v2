#!/bin/bash
masterUrl="yarn-cluster"
appName="RealTimeFollowScala"
kafkaBrokers="bigdata01:9092,bigdata02:9092,bigdata03:9092"
groupId="con_f_1"
topic="user_follow"
boltUrl="bolt://bigdata04:7687"
userName="neo4j"
passWord="admin"

#注意：需要将flink脚本路径配置到linux的环境变量中
flink run \
-m ${masterUrl} \
-ynm ${appName} \
-yqu default \
-yjm 1024 \
-ytm 1024 \
-ys 1 \
-p 5 \
-c com.imooc.flink.RealTimeFollowScala \
/data/soft/video_recommend_v2/jobs/real_time_follow-1.0-SNAPSHOT-jar-with-dependencies.jar ${appName} ${kafkaBrokers} ${groupId} ${topic} ${boltUrl} ${userName} ${passWord}
