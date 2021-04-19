package com.zengyunning.networkflow_analysis

import org.apache.flink.api.common.functions.AggregateFunction
import org.apache.flink.api.common.state.{ListState, ListStateDescriptor, MapState, MapStateDescriptor}
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.scala.function.WindowFunction
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector

import java.sql.Timestamp
import java.text.SimpleDateFormat
import java.util
import java.util.Map
import scala.collection.mutable.ListBuffer

/**
 * Copyright (c) 2021 zengyunning All Rights Reserved
 *
 * Project: UserBehaviorAnalysis
 * Package: com.zengyunning.networkflow_analysis
 * Version: V 1.0
 *
 * Create by zengyunning on 2021/04/16 21:30
 */

// 定义输入数据样例类
case class ApacheLogEvent(ip: String, userId: String, timestamp: Long, method: String, url: String)

/**
 * TODO 说明：
 *      由于是热门页面统计（即TopN需求），跟之前代码流程一样：先进行开窗聚合操作，得到聚合结果之后再按照每一个窗口分组、排序————定义一个定时器，（窗口）收集到数据后进行排序输出；
 */
// 定义窗口聚合结果样例类
case class PageViewCount(url: String, windowEnd: Long, count: Long)

object HotPagesNetworkFlow {

    def main(args: Array[String]): Unit = {

        // 创建流处理执行环境
        val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
        env.setParallelism(1)   // 保证不影响结果正确性、方便测试
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

        // 读取数据，转换成样例类并提取时间戳和Watermark
//        val inputPath: String = "E:\\IDEAWorkspace\\UserBehaviorAnalysis\\NetworkFlowAnalysis\\src\\main\\resources\\datas\\apache.log"
//        val inputStream: DataStream[String] = env.readTextFile(inputPath)
        val inputStream: DataStream[String] = env.socketTextStream("hadoop202", 7777)

        val dataStream: DataStream[ApacheLogEvent] = inputStream
                .map(
                    data => {
                        val arr: Array[String] = data.split(" ")
                        // 对事件时间进行转换，得到时间戳
                        val simpleDateFormat: SimpleDateFormat = new SimpleDateFormat("dd/MM/yyyy:HH:mm:ss")
                        val ts: Long = simpleDateFormat.parse(arr(3)).getTime
                        ApacheLogEvent(arr(0), arr(1), ts, arr(5), arr(6))
                    }
                )
//                .assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor[ApacheLogEvent](Time.minutes(1)) {
                .assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor[ApacheLogEvent](Time.seconds(1)) {
                    override def extractTimestamp(element: ApacheLogEvent): Long = element.timestamp
                })

        // 进行开窗聚合，以及排序输出
        val aggStream: DataStream[PageViewCount] = dataStream
//                .filter(ApacheLogEvent => {ApacheLogEvent.method == "GET"})
//                .keyBy(ApacheLogEvent => ApacheLogEvent.url)
                .filter(_.method == "GET")
                .keyBy(_.url)
                .timeWindow(Time.minutes(10), Time.seconds(5))
                .allowedLateness(Time.minutes(1))
                .sideOutputLateData(new OutputTag[ApacheLogEvent]("late"))
                .aggregate(new PageCountAgg(), new PageViewCountWindowResult())

        val resultStream: DataStream[String] = aggStream
//                .keyBy(PageViewCount => PageViewCount.windowEnd)
                .keyBy(_.windowEnd)
                .process(new TopNHotPages(3))

        dataStream.print("data")
        aggStream.print("agg")
        aggStream.getSideOutput(new OutputTag[ApacheLogEvent]("late")).print("late")
        resultStream.print()

        env.execute("hotpages job")

    }
}

class PageCountAgg() extends AggregateFunction[ApacheLogEvent, Long, Long] {
    override def createAccumulator(): Long = 0L

    override def add(value: ApacheLogEvent, accumulator: Long): Long = accumulator + 1

    override def getResult(accumulator: Long): Long = accumulator

    override def merge(a: Long, b: Long): Long = a + b
}

class PageViewCountWindowResult() extends WindowFunction[Long, PageViewCount, String, TimeWindow] {
    override def apply(key: String, window: TimeWindow, input: Iterable[Long], out: Collector[PageViewCount]): Unit = {
        out.collect(PageViewCount(key, window.getEnd, input.iterator.next()))
    }
}

class TopNHotPages(topSize: Int) extends KeyedProcessFunction[Long, PageViewCount, String] {

//    lazy val pageViewCountListState: ListState[PageViewCount] = getRuntimeContext.getListState(new ListStateDescriptor[PageViewCount]("pageViewCount-list", classOf[PageViewCount]))
    lazy val pageViewCountMapState: MapState[String, Long] = getRuntimeContext.getMapState(new MapStateDescriptor[String, Long]("pageViewCount-map", classOf[String], classOf[Long]))

    override def processElement(value: PageViewCount, ctx: KeyedProcessFunction[Long, PageViewCount, String]#Context, out: Collector[String]): Unit = {
//        pageViewCountListState.add(value)
        pageViewCountMapState.put(value.url, value.count)
        ctx.timerService().registerEventTimeTimer(value.windowEnd + 1)
        // 另外注册一个定时器，1分钟之后触发，这时窗口已经彻底关闭，不再有聚合结果输出，可以情况状态
        ctx.timerService().registerEventTimeTimer(value.windowEnd + 60000L)
    }

    override def onTimer(timestamp: Long, ctx: KeyedProcessFunction[Long, PageViewCount, String]#OnTimerContext, out: Collector[String]): Unit = {

        /*
        val allPageViewCounts: ListBuffer[PageViewCount] = ListBuffer()
        val iter: util.Iterator[PageViewCount] = pageViewCountListState.get().iterator()

        while (iter.hasNext) {
            allPageViewCounts += iter.next()
        }
        */

        // 判断定时器触发时间，如果已经是窗口结束时间1min之后，那么直接情况状态
        if(timestamp == ctx.getCurrentKey + 60000L) {
            pageViewCountMapState.clear()
            return
        }

        val allPageViewCounts: ListBuffer[(String, Long)] = ListBuffer()
        val iter: util.Iterator[Map.Entry[String, Long]] = pageViewCountMapState.entries().iterator()

        while (iter.hasNext) {
            val entry: Map.Entry[String, Long] = iter.next()
            allPageViewCounts += ((entry.getKey, entry.getValue))
        }

        ctx.getCurrentKey

        // 提前清空状态
//        pageViewCountListState.clear()

        // 按照访问量排序并输出TopN
//        val sortedPageViewCouts: ListBuffer[PageViewCount] = allPageViewCounts.sortWith(_.count > _.count).take(topSize)
        val sortedPageViewCouts: ListBuffer[(String, Long)] = allPageViewCounts.sortWith(_._2 > _._2).take(topSize)

        // 将排名信息格式化成String，便于打印输出可视化展示
        val result: StringBuilder = new StringBuilder
        result.append("窗口结束时间：").append(new Timestamp(timestamp - 1)).append("\n")

        // 遍历结果列表中的每个ItemViewCount，输出到一行
        for(i <- sortedPageViewCouts.indices) {
//            val currentPageViewCount: PageViewCount = sortedPageViewCouts(i)
val currentPageViewCount: (String, Long) = sortedPageViewCouts(i)
            result.append("NO").append(i + 1).append("：\t")
//                    .append("页面URL = ").append(currentPageViewCount.url).append("\t")
                    .append("页面URL = ").append(currentPageViewCount._1).append("\t")
//                    .append("热门度 = ").append(currentPageViewCount.count).append("\n")
                    .append("热门度 = ").append(currentPageViewCount._2).append("\n")
        }

        // 每次for循环结束后，为不同窗口间设置间隔符
        result.append("\n==========================================\n\n")

        // 控制（每个窗口数据）输出显示频率：因为是从文件中读取数据过程太快（处理、输出结果也很快）
        Thread.sleep(1000)

        // 将result写出到输出缓冲中
        out.collect(result.toString())
    }
}
