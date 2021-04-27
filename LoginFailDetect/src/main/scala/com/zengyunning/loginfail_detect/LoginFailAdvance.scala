package com.zengyunning.loginfail_detect

import org.apache.flink.api.common.state.{ListState, ListStateDescriptor}
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.util.Collector

import java.net.URL
import java.util

/**
 * Copyright (c) 2021 zengyunning All Rights Reserved
 *
 * Project: UserBehaviorAnalysis
 * Package: com.zengyunning.loginfail_detect
 * Version: V 1.0
 *
 * Create by zengyunning on 2021/04/25 23:21
 */
object LoginFailAdvance {

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

        // 进行判断和检测，如果 2 秒之内连续 两次 登录失败，输出报警信息
        val loginFailWarningStream: DataStream[LoginFailWarning] = logEventStream
                .keyBy(_.userId)
                .process(new LoginFailWarningAdvanceResult())

        loginFailWarningStream.print()
        env.execute("login fail detect job")

    }

    /**
     *
     * 说明：
     * 1.timerTsState：此时不需要定时器————使用loginFailListState保存每次登录事件，将当前数据（登录失败）与前一条登录事件（失败）进行判断——如果二者时间戳差值在 2秒 之内，则直接报警，而不需要注册定时器 2秒 后再执行报警；
     * 由于不需要注册定时器，因此也不再需要定义timerTsState保存定时器的时间戳；
     */
    class LoginFailWarningAdvanceResult() extends KeyedProcessFunction[Long, LoginEvent, LoginFailWarning] {
        // 定义状态，保存当前所有的登录失败事件
        lazy val loginFailListState: ListState[LoginEvent] = getRuntimeContext.getListState(new ListStateDescriptor[LoginEvent]("loginfail-list", classOf[LoginEvent]))

        override def processElement(value: LoginEvent, ctx: KeyedProcessFunction[Long, LoginEvent, LoginFailWarning]#Context, out: Collector[LoginFailWarning]): Unit = {
            // 首先判断事件类型
            if (value.eventType == "fail") {
                // 1.如果是失败，进一步做判断
                val iter: util.Iterator[LoginEvent] = loginFailListState.get().iterator()
                // 判断之前是否有登录失败事件
                if (iter.hasNext) {
                    // 1.1 如果有，那么判断两次失败的时间差
                    val first_lastFailEvent: LoginEvent = iter.next()
                    if (value.timestamp - first_lastFailEvent.timestamp < 2) { // TODO LoginEvent-timestamp单位为：秒
                        // 如果在2秒之内，输出报警
//                        out.collect(LoginFailWarning(ctx.getCurrentKey, first_lastFailEvent.timestamp, value.timestamp, "login fail in 2s for 2 times."))
                        out.collect(LoginFailWarning(value.userId, first_lastFailEvent.timestamp, value.timestamp, "login fail 2 times in 2s."))
                    }
                    // 如果在2秒之外，不做任何处理
                    // 不管报不报警，当前都已处理完毕，将状态更新为最近依次登录失败的事件
                    loginFailListState.clear()
                    loginFailListState.add(value)
                } else {
                    // 1.2 如果没有，直接把当前事件添加到ListState中
                    loginFailListState.add(value)
                }
            } else {
                // 2.如果是成功，直接清空状态
                loginFailListState.clear()
            }
        }
    }
}
