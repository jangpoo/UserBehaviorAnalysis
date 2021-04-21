package com.zengyunning.networkflow_analysis

import org.apache.flink.api.common.functions.{AggregateFunction, MapFunction}
import org.apache.flink.api.common.state.{ValueState, ValueStateDescriptor}
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.scala.function.WindowFunction
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector

import java.net.URL
import scala.util.Random

/**
 * Copyright (c) 2021 zengyunning All Rights Reserved
 *
 * Project: UserBehaviorAnalysis
 * Package: com.zengyunning.networkflow_analysis
 * Version: V 1.0
 *
 * Create by zengyunning on 2021/04/20 12:33
 */

/**
 * TODO 说明：
 *      如果某些需求用到相同数据（如样例类），则可以将其提取到util公共包下，但实际/真实开发中，并非所有需求的输入/输出数据格式都一样，因此这里我们选择在各自需求中单独定义
 */
// TODO 定义输入数据样例类
case class UserBehavior(userId: Long, itemId: Long, categorId: Long, behavior: String, timestamp: Long)

// TODO 定义输出pv统计的样例类
case class PvCount(windowEnd: Long, count: Long)

object PageView {

    def main(args: Array[String]): Unit = {

        val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
//        env.setParallelism(1)

        // 从文件中读取数据
        /**
         * TODO 说明：
         *      绝对路径（本地盘符）方式在Flink程序打包提交到执行环境时可能会导致问题，因此实际开发中不建议采用这种读取文件方式
         */
//        val inputPath: String = "E:\\IDEAWorkspace\\UserBehaviorAnalysis\\HotItemsAnalysis\\src\\main\\resources\\datas\\UserBehavior.csv"
//        val inputStream: DataStream[String] = env.readTextFile(inputPath)

//        val url: URL = PageView.getClass.getResource("datas/UserBehavior.csv")
//        val url: URL = PageView.getClass.getResource("/datas/UserBehavior.csv")
        val resource: URL = getClass.getResource("/datas/UserBehavior.csv")
        val inputStream: DataStream[String] = env.readTextFile(resource.getPath)

        // 转换成样例类类型，并提取时间戳和Watermark
        val dataStream: DataStream[UserBehavior] = inputStream
                .map(
                    data => {
                        val arr: Array[String] = data.split(",")
                        // 因为用户行为数据各个字段间没有空格，这里我们暂不调用trim()————清洗后的数据，ETL时已经调用trim()处理过
                        UserBehavior(arr(0).toLong, arr(1).toLong, arr(2).toLong, arr(3), arr(4).toLong)
                    }
                )
                .assignAscendingTimestamps(_.timestamp * 1000L)     // 用户行为数据timestamp字段单位是秒

        val pvStream: DataStream[PvCount] = dataStream
                .filter(_.behavior == "pv")
                // TODO PV需求不需要分组；但如果不分组，后面直接Window开窗则需要“windowAll”，因此我们可以像之前WordCount【每来一条数据，则将其map成二元组】那样使用map()
//                .map(data => ("pv", 1L)) // 定义一个pv字符串作为分组的dummy key
                .map(new MyMapper())
                .keyBy(_._1) // 所有数据会被分到同一个组
                .timeWindow(Time.hours(1)) // 1小时滚动窗口
                // TODO 因为此时需要用到Window“信息”，所以必须使用WindowFunction窗口函数，但我们又希望每来/到达一条数据就统计一次————aggregate()
                .aggregate(new PvCountAgg(), new PvCountWindowResult())  // new PvCountAgg()：增量预聚合统计     new PvCountWindowResult()：包装成我们想要的样例类类型输出————没有后续排序操作

        val totalPvStream: DataStream[PvCount] = pvStream
                .keyBy(_.windowEnd)
//                .sum("count")
                .process(new TotalPvCountResult())

        totalPvStream.print()

        env.execute("pv job")

    }

}

// 自定义预聚合函数
class PvCountAgg() extends AggregateFunction[(String, Long), Long, Long]{
    override def createAccumulator(): Long = 0L

    override def add(value: (String, Long), accumulator: Long): Long = accumulator + 1

    override def getResult(accumulator: Long): Long = accumulator

    override def merge(a: Long, b: Long): Long = a + b
}

// 自定义窗口函数————“拿到”预聚合结果之后才做的处理
class PvCountWindowResult() extends WindowFunction[Long, PvCount, String, TimeWindow] {
    override def apply(key: String, window: TimeWindow, input: Iterable[Long], out: Collector[PvCount]): Unit = {
        out.collect(PvCount(window.getEnd, input.head))
    }
}

// 自定义Mapper，随机生成分组的key
class MyMapper() extends MapFunction[UserBehavior, (String, Long)] {
    override def map(value: UserBehavior): (String, Long) = {
        (Random.nextString(10), 1L)     // 问题：Java版如何实现
    }
}

class TotalPvCountResult() extends KeyedProcessFunction[Long, PvCount, PvCount] {
    /**
     * TODO 说明：
     *      每来/到达一条数据【元素——keyBy(...)分组之后】，都会执行一次processElement(...)方法；如果我们希望实现增量聚合，则使用ValueState值状态；而如果希望将到达的数据攒起来，最后再进行计算，则选用ListState——
     *  每来一条数据就保存到ListState中，最后将所有数据“拿出来”进行“叠加”【参考之前的ListState使用案例】；
     */
    // 增量聚合————定义一个状态，保存当前所有count总和
    lazy val totalPvCountResultState: ValueState[Long] = getRuntimeContext.getState(new ValueStateDescriptor[Long]("total-pv", classOf[Long]))

    override def processElement(value: PvCount, ctx: KeyedProcessFunction[Long, PvCount, PvCount]#Context, out: Collector[PvCount]): Unit = {
        // 每来一个数据，将count值叠加在当前的状态上
        val currentTotalCount: Long = totalPvCountResultState.value()
        totalPvCountResultState.update(currentTotalCount + value.count)
//        totalPvCountResultState.update(totalPvCountResultState.value() + value.count)
        // 注册一个 windowEnd+1ms 后触发的定时器
        ctx.timerService().registerEventTimeTimer(value.windowEnd + 1)
    }

    override def onTimer(timestamp: Long, ctx: KeyedProcessFunction[Long, PvCount, PvCount]#OnTimerContext, out: Collector[PvCount]): Unit = {
        // 只需要将totalPvCountResultState“拿出来”（封装成样例类PvCount）输出即可————当前不需要进行排序操作
        val totalPvCount: Long = totalPvCountResultState.value()
//        out.collect(PvCount(timestamp-1, totalPvCount))
        out.collect(PvCount(ctx.getCurrentKey, totalPvCount))

        totalPvCountResultState.clear()
    }
}
