package com.zengyunning.loginfail_detect

import org.apache.flink.api.common.state.{ListState, ListStateDescriptor, ValueState, ValueStateDescriptor}
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.util.Collector

import java.net.URL
import java.util
import scala.collection.mutable.ListBuffer

/**
 * Copyright (c) 2021 zengyunning All Rights Reserved
 *
 * Project: UserBehaviorAnalysis
 * Package: com.zengyunning.loginfail_detect
 * Version: V 1.0
 *
 * Create by zengyunning on 2021/04/25 14:29
 */

// 输入的登录事件样例类
case class LoginEvent(userId: Long, ip: String, eventType: String, timestamp: Long)
// 输出报警信息样例类
case class LoginFailWarning(userId: Long, firstFailTime: Long, lastFailTime: Long, warningMsg: String)  // lastFailTime：如果需求要求“在 2/3 秒之内连续三/五次登录失败”，...

object LoginFail {

    def main(args: Array[String]): Unit = {

        val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)  // 事件时间：输入数据携带有timestamp时间戳
        env.setParallelism(1)  // 不影响结果“正确性”

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

        // 进行判断和检测，如果 2 秒之内连续 两次 登录失败，输出报警信息——类似之前做过的 “传感器温度 10 秒内连续上升报警” 案例
        /**
         * TODO 说明：
         *      1.当前需求类似之前做过的 “传感器温度 10 秒内连续上升报警” 案例，实现技术：
         *          定时器、状态编程（设置状态：保存之前的温度值，然后进行判断），因此我们需要使用 ProcessFunction；
         *          1-1.对于某个需求，如果使用简单的“聚合”或“开窗”无法进行实现，则选用 ProcessFunction；
         *          1-2.在使用 ProcessFunction之前——由于 ProcessFunction中涉及到状态（编程），而对于状态（定义时），我们一般选用KeyedState【按照某个key分组之后，然后进行状态设置——仅针对“当前这一组”有用】，...；
         */
        val loginFailWarningStream: DataStream[LoginFailWarning] = logEventStream
                .keyBy(_.userId)
                .process(new LoginFailWarningResult(2))

        loginFailWarningStream.print()
        env.execute("login fail detect job")

    }
}

/**
 * TODO 说明：
 *      1.processElement(...)：每来一条数据，都会调用该方法；
 *      2.定义状态：
 *          思路1：Boolean类型标识位记录上次登录是否失败（true：表示上次登录失败），如果第一次登录失败————若第二次仍然登录失败，则“一直等着”；若第二次登录成功，则删除定时器；
 *              但该方式扩展性较差，如需求 2 秒之内连续 五次 登录失败，则无法使用true和false表示 5次 不同 登录状态；
 *          思路2：使用ListState保存每次登录 失败 状态————使用List保存当前所有登录失败的状态/数据，如果当前是第一次登录失败，则注册定时器（ 2s/5s 之后触发）；
 *                      在此过程中，若后续登录均失败，就将其每次登录状态保存到List中：
 *                          ①若2s/5s内，所有登录均为失败状态，则定时器在2s/5s之后就会被触发；
 *                          ②触发的时候，判断List中有几次登录失败；
 *                          ③若需求要求2s/5s内连续 两次/十次 登录失败，failCount-list.size >= 2/10，则报警；
 *                      在此过程中，若有一次登录成功，则清空List、删除定时器；
 */
class LoginFailWarningResult(failTimes: Int) extends KeyedProcessFunction[Long, LoginEvent, LoginFailWarning] {
    // 定义状态，保存当前所有的登录失败事件，保存定时器的时间戳（删除定时器时，必须知道其时间戳）
    lazy val loginFailListState: ListState[LoginEvent] = getRuntimeContext.getListState(new ListStateDescriptor[LoginEvent]("loginfail-list", classOf[LoginEvent]))
    lazy val timerTsState: ValueState[Long] = getRuntimeContext.getState(new ValueStateDescriptor[Long]("timer-ts", classOf[Long]))

    override def processElement(value: LoginEvent, ctx: KeyedProcessFunction[Long, LoginEvent, LoginFailWarning]#Context, out: Collector[LoginFailWarning]): Unit = {
        // （每来一条数据）判断其事件类型——若登录失败，则添加到loginFailListState中；若登录成功，则清空loginFailListState、删除定时器【重新“开始”】
        // 判断当前登录事件是成功还是失败
        if(value.eventType == "fail") {
            loginFailListState.add(value)

            /**
             * 是否注册定时器：
             *      思路1：定义Boolean类型标识位判断是否是第一次登录；
             *      思路2：取巧的方式——使用定时器（时间戳）timerTsState（若一开始没有定时器注册，则其时间戳timerTsState.value为0L【1970-01-01:00:00:00，因为实际中不可能注册该时间戳定时器，因此代表“没有数据过来”】，
             *              则注册定时器）
             */
            // 如果没有定时器，那么注册一个2秒后的定时器
            if(timerTsState.value() == 0) {
                val ts: Long = value.timestamp * 1000L + 2000L
                ctx.timerService().registerEventTimeTimer(ts)
                timerTsState.update(ts)
            }
        }else {
            // 如果是成功，那么直接清空状态、删除定时器，重新开始（即下一次又有数据来了之后，如果其登录事件value.eventType == "fail"，则按照上面if逻辑执行）
            ctx.timerService().deleteEventTimeTimer(timerTsState.value())
            loginFailListState.clear()
            timerTsState.clear()
        }
    }

    /**
     * TODO 说明：
     *      1.onTimer(...)：没有登录事件为成功的数据到来（即当前userId的用户连续登录失败），...；
     *
     */
    override def onTimer(timestamp: Long, ctx: KeyedProcessFunction[Long, LoginEvent, LoginFailWarning]#OnTimerContext, out: Collector[LoginFailWarning]): Unit = {
        val allLoginFailList: ListBuffer[LoginEvent] = new ListBuffer[LoginEvent]()
        val iter: util.Iterator[LoginEvent] = loginFailListState.get().iterator()
        while (iter.hasNext) {
            allLoginFailList += iter.next()
        }

        // 判断登录失败事件的个数，如果超过了上限，报警
        if(allLoginFailList.length >= failTimes) {
//            out.collect(LoginFailWarning(ctx.getCurrentKey, allLoginFailList.head.timestamp, allLoginFailList.last.timestamp, "login fail in 2s for" + allLoginFailList.length + "times."))
            out.collect(LoginFailWarning(allLoginFailList.head.userId, allLoginFailList.head.timestamp, allLoginFailList.last.timestamp, "login fail in 2s for" + allLoginFailList.length + "times."))
        }

        // 清空状态
        loginFailListState.clear()
        timerTsState.clear()
    }
}
