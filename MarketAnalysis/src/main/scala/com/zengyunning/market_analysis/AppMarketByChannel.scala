package com.zengyunning.market_analysis

import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.source.{RichSourceFunction, SourceFunction}
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.scala.function.ProcessWindowFunction
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector

import java.sql.Timestamp
import java.util.UUID
import scala.util.Random

/**
 * Copyright (c) 2021 zengyunning All Rights Reserved
 *
 * Project: UserBehaviorAnalysis
 * Package: com.zengyunning.market_analysis
 * Version: V 1.0
 *
 * Create by zengyunning on 2021/04/22 22:30
 */

// 定义输入数据样例类
case class MarketUserBehavior(userId: String, behavior: String, channel: String, timestamp: Long)
// 定义输出数据样例类
case class MarketViewCount(windowStart: String, windowEnd: String, channel: String, behavior: String, count: Long)

/**
 * TODO 说明：
 *      1.run(...)：一般来讲，底层线程会调用该方法不停地生成数据；
 *      2.cancel()：在什么情况下取消生成数据源（停止生成数据）；
 */
// 自定义测试数据源
class SimulatedSource() extends RichSourceFunction[MarketUserBehavior] {

    // 是否运行的标识位
    var running: Boolean = true
    // 定义用户行为和渠道的集合
    val behaviorSet: Seq[String] = Seq("view", "download", "install", "uninstall")
    val channelSet: Seq[String] = Seq("appstore", "weibo", "wechat", "tieba")
    val rand: Random = Random

    override def run(ctx: SourceFunction.SourceContext[MarketUserBehavior]): Unit = {
        // 定义一个生成数据的最大数量
        val maxCounts: Long = Long.MaxValue
        // 当前已生成的数据量
        var count: Long = 0L

        // while循环，不停地随机产生数据
        while (running && count < maxCounts) {
            val id: String = UUID.randomUUID().toString
            val behavior: String = behaviorSet(rand.nextInt(behaviorSet.size))
            val channel: String = channelSet(rand.nextInt(channelSet.size))
            val ts: Long = System.currentTimeMillis()

            ctx.collect(MarketUserBehavior(id, behavior, channel, ts))
            count += 1
            Thread.sleep(50L)
        }
    }

    override def cancel(): Unit = running = false
}

object AppMarketByChannel {

    def main(args: Array[String]): Unit = {

        val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
        env.setParallelism(1)
        // TODO 当前可以不设置 事件时间语义——因为并行度已经设置为1，数据（timestamp时间戳）按照系统时间生成，那么后续（数据）处理时必定按照时间顺序处理
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

        // 读取数据源
        val inputStream: DataStream[MarketUserBehavior] = env.addSource(new SimulatedSource)
        // TODO 由于生成数据源已经是样例类类型，因此不需要再map(...)转换
        val dataStream: DataStream[MarketUserBehavior] = inputStream
                .assignAscendingTimestamps(_.timestamp)

        // 开窗统计输出
        val resultStream: DataStream[MarketViewCount] = dataStream
                .filter(_.behavior != "uninstall")
                .keyBy(data => (data.channel, data.behavior))
                .timeWindow(Time.days(1L), Time.seconds(5L))
                .process(new MarketCountByChannel())

        resultStream.print()

        env.execute("app market by channel job")

    }

}

// 自定义ProcessWindowFunction
class MarketCountByChannel() extends ProcessWindowFunction[MarketUserBehavior, MarketViewCount, (String, String), TimeWindow] {
    /**
     * TODO 说明：
     *      1.process(...)：不是每来一条数据就调用该方法，而是所有数据都“收集齐了”才调用该方法【触发执行时机：不是说所有数据都“收集齐了”就立即执行，而是当前窗口要触发计算（不一定是关闭、FIRE和PURGE）、要输出结果时】；
     *      2.elements.size：增量/预聚合函数（count + 1）和全量聚合函数的比较
     *          全量代价（elements.size）：这种方式较为简便（不用每来一条数据都要计算一次count + 1），但代价是需要缓存所有数据（到内存中）
     *          增量/预聚合：只需要缓存一个数count（然后每来一条数据 —— accumulator+1）
     *          一般而言，推荐使用增量/预聚合，......
     */
    override def process(key: (String, String), context: Context, elements: Iterable[MarketUserBehavior], out: Collector[MarketViewCount]): Unit = {
        val start: String = new Timestamp(context.window.getStart).toString
        val end: String = new Timestamp(context.window.getEnd).toString
        val channel: String = key._1
        val behavior: String = key._2
        val count: Long = elements.size.toLong

        out.collect(MarketViewCount(start, end, channel, behavior, count))
    }
}
