package com.zengyunning.market_analysis

import org.apache.flink.api.common.functions.AggregateFunction
import org.apache.flink.api.common.state.{ValueState, ValueStateDescriptor}
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.scala.function.WindowFunction
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector

import java.net.URL
import java.sql.Timestamp

/**
 * Copyright (c) 2021 zengyunning All Rights Reserved
 *
 * Project: UserBehaviorAnalysis
 * Package: com.zengyunning.market_analysis
 * Version: V 1.0
 *
 * Create by zengyunning on 2021/04/23 20:22
 */

// 定义输入输出数据样例类
case class AdClickLog(userId: Long, adId: Long, province: String, city: String, timestamp: Long)
case class AdClickCountByProvince(windowEnd: String, province: String, count: Long)
// TODO 侧输出流黑名单报警信息样例类
case class BlackListUserWarning(userId:Long, adId: Long, msg: String)

object AdClickAnalysis {

    def main(args: Array[String]): Unit = {

        val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
        env.setParallelism(1)

        // 从文件中读取数据
        val url: URL = getClass.getResource("/datas/AdClickLog.csv")
        val inputStream: DataStream[String] = env.readTextFile(url.getPath)

        // 转换成样例类，并提取时间戳和Watermark
        val adLogStream: DataStream[AdClickLog] = inputStream
                .map(
                    data => {
                        val arr: Array[String] = data.split(",")
                        AdClickLog(arr(0).toLong, arr(1).toLong, arr(2), arr(3), arr(4).toLong)
                    }
                )
                .assignAscendingTimestamps(_.timestamp * 1000L)

        // TODO 插入一步过滤操作，并将有刷单行为的用户输出到侧输出流（黑名单报警）
        val filterBlackListUserStream: DataStream[AdClickLog] = adLogStream
                .keyBy(data => (data.userId, data.adId))
                .process(new FilterBlackListUserResult(100))  // TODO 选用KeyedProcessFunction

        // 开窗聚合统计
//        val adCountResultStream: DataStream[AdClickCountByProvince] = adLogStream
        val adCountResultStream: DataStream[AdClickCountByProvince] = filterBlackListUserStream
                .keyBy(_.province)
                .timeWindow(Time.hours(1L), Time.seconds(5L))
                .aggregate(new AdCountAgg(), new AdCountWindowResult())

        adCountResultStream.print("count result")
        filterBlackListUserStream.getSideOutput(new OutputTag[BlackListUserWarning]("warning")).print("warning")


        env.execute("ad count statistics job")

    }
}

class AdCountAgg() extends AggregateFunction[AdClickLog, Long, Long] {

    override def createAccumulator(): Long = 0L

    override def add(value: AdClickLog, accumulator: Long): Long = accumulator + 1

    override def getResult(accumulator: Long): Long = accumulator

    override def merge(a: Long, b: Long): Long = a + b
}

class AdCountWindowResult() extends WindowFunction[Long, AdClickCountByProvince, String, TimeWindow] {
    override def apply(key: String, window: TimeWindow, input: Iterable[Long], out: Collector[AdClickCountByProvince]): Unit = {
        val end: String = new Timestamp(window.getEnd).toString
        out.collect(AdClickCountByProvince(end, key, input.head))
    }
}

/**
 * TODO 说明：
 *      1.processElement(...)：每来一条数据，都会调用该方法；
 *      2.lazy：必须使用lazy或在open(...)生命周期中定义状态，否则当前类在new实例化时getRuntimeContext获取不到ctx上下文
 *      3.isBlackState：（必须定义为状态）不能定义为本地变量，这样，对于当前Slot分区上执行的任务，所有/每一条数据来了之后都可以访问该变量（更改其值）；
 */
// 自定义KeyedProcessFunction
class FilterBlackListUserResult(maxCount: Long) extends KeyedProcessFunction[(Long, Long), AdClickLog, AdClickLog] {
    // 定义状态，保存用户对广告的点击量，每天0点定时清空状态的时间戳，标记当前用户是否已经进入黑名单（对于某个用户某条广告，只要首次刷单达到上限，就加入黑名单【输出到侧输出流报警】）
    lazy val countState: ValueState[Long] = getRuntimeContext.getState(new ValueStateDescriptor[Long]("count", classOf[Long]))
    lazy val resetTimerTsState: ValueState[Long] = getRuntimeContext.getState(new ValueStateDescriptor[Long]("reset-ts", classOf[Long]))
    lazy val isBlackState: ValueState[Boolean] = getRuntimeContext.getState(new ValueStateDescriptor[Boolean]("is-black", classOf[Boolean]))

    override def processElement(value: AdClickLog, ctx: KeyedProcessFunction[(Long, Long), AdClickLog, AdClickLog]#Context, out: Collector[AdClickLog]): Unit = {
        val currCount: Long = countState.value()

        // （对于某个用户某条广告）判断只要是第一个数据来了，直接注册0点的清空状态定时器
        if(currCount == 0) {  // TODO 因为每来一条数据都会调用processElement(...)方法，所以currCount（状态countState/resetTimerTsState/isBlackState）——对应某个用户+某条广告——>可以通过currCount == 0L判断是否是第一条数据
            val ts: Long = (ctx.timerService().currentProcessingTime() / (1000 * 60 * 60 * 24) + 1) * (24 * 60 * 60 * 1000) - 8 * 60 * 60 * 1000
            resetTimerTsState.update(ts)
            ctx.timerService().registerProcessingTimeTimer(ts)
        }

        // 判断count值是否已经达到定义的阈值，如果超过就输出到黑名单
        if(currCount >= maxCount) {
            // 判断是否已经在黑名单中，没有的话才输出到侧输出流
            if(!isBlackState.value()) {
                isBlackState.update(true)
                ctx.output(new OutputTag[BlackListUserWarning]("warning"), BlackListUserWarning(value.userId, value.adId, "Click ad over " + maxCount + " times today."))

            }
            return
        }

        // （如果currCount没有超过上限maxCount）正常情况，count加1，然后将数据原样输出
        countState.update(currCount + 1)
        out.collect(value)
    }

    override def onTimer(timestamp: Long, ctx: KeyedProcessFunction[(Long, Long), AdClickLog, AdClickLog]#OnTimerContext, out: Collector[AdClickLog]): Unit = {
        if(timestamp == resetTimerTsState) {
            isBlackState.clear()
            countState.clear()
        }
    }
}
