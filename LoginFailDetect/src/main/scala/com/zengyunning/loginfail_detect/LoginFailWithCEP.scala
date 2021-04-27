package com.zengyunning.loginfail_detect

import org.apache.flink.cep.PatternSelectFunction
import org.apache.flink.cep.scala.{CEP, PatternStream}
import org.apache.flink.cep.scala.pattern.Pattern
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.time.Time

import java.net.URL
import java.util

/**
 * Copyright (c) 2021 zengyunning All Rights Reserved
 *
 * Project: UserBehaviorAnalysis
 * Package: com.zengyunning.loginfail_detect
 * Version: V 1.0
 *
 * Create by zengyunning on 2021/04/26 23:01
 */
object LoginFailWithCEP {

    def main(args: Array[String]): Unit = {

        val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
        env.setParallelism(1)

        // 读取数据
        val url: URL = getClass.getResource("/datas/LoginLog.csv")
        val inputStream: DataStream[String] = env.readTextFile(url.getPath)

        // 转换成样例类类型，并提取时间戳和Watermark
        val logEventStream: DataStream[LoginEvent] = inputStream
                .map(
                    data => {
                        val arr: Array[String] = data.split(",")
                        LoginEvent(arr(0).toLong, arr(1), arr(2), arr(3).toLong)
                    }
                )
                .assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor[LoginEvent](Time.seconds(3)) {
                    override def extractTimestamp(element: LoginEvent): Long = element.timestamp * 1000L
                })

        // 1.定义匹配的模式：要求是一个登录失败事件后，紧跟另一个登录失败事件
        val loginFailPattern: Pattern[LoginEvent, LoginEvent] = Pattern
                .begin[LoginEvent]("firstFail").where(_.eventType == "fail")
                .next("secondFail").where(_.eventType == "fail")
                .within(Time.seconds(2))

        // 2.将模式应用到数据流上，得到一个PatternStream
        val patternStream: PatternStream[LoginEvent] = CEP.pattern(logEventStream.keyBy(_.userId), loginFailPattern)

        // 3.检出符合模式的数据流，需要调用select
        val loginFailWarningStream: DataStream[LoginFailWarning] = patternStream.select(new LoginFailEventMatch())

        loginFailWarningStream.print()

        env.execute("login fail with cep job")

    }
}

// 实现自定义PatternSelectFunction
class LoginFailEventMatch() extends PatternSelectFunction[LoginEvent, LoginFailWarning] {
    override def select(pattern: util.Map[String, util.List[LoginEvent]]): LoginFailWarning = {
        // 当前匹配到的事件序列，就保存到Map里
//        val firstFailEvent: LoginEvent = pattern.get("firstFail").iterator().next()
        val firstFailEvent: LoginEvent = pattern.get("firstFail").get(0)
        val secondFailEvent: LoginEvent = pattern.get("secondFail").iterator().next()
        LoginFailWarning(firstFailEvent.userId, firstFailEvent.timestamp, secondFailEvent.timestamp, "login fail 2 times in 2s.")
    }
}
