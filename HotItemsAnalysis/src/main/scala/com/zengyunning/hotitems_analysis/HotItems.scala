package com.zengyunning.hotitems_analysis

import org.apache.flink.api.common.functions.AggregateFunction
import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.api.common.state.{ListState, ListStateDescriptor}
import org.apache.flink.api.java.tuple.{Tuple, Tuple1}
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.scala.function.WindowFunction
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer
import org.apache.flink.util.Collector

import java.sql.Timestamp
import java.util
import java.util.Properties
import scala.collection.mutable.ListBuffer

/**
 * Copyright (c) 2021 zengyunning All Rights Reserved
 *
 * Project: UserBehaviorAnalysis
 * Package: com.zengyunning.hotitems_analysis
 * Version: V 1.0
 *
 * Create by zengyunning on 2021/04/12 17:49
 */
object HotItems {

    def main(args: Array[String]): Unit = {

        // 创建流处理执行环境
        val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
        /**
         * TODO 并行度：
         *      我们希望不影响最终输出结果（因为是在Console控制台打印输出，希望按照顺序输出正常的结果），尤其是读取文件数据[最终处理后输出]时——
         *  如果是真实场景中，前后相差5min的窗口不会出现乱序数据结果；但如果像我们这里读文件数据，那么就没准儿了（从文件读取数据过程很快，说不定5min
         *  的窗口就会出现乱序，因此为了最终结果显式更合理，我们将全局并行度设置为1——不影响结果正确性）。
         */
        env.setParallelism(1)
        /**
         * TODO 时间语义/时间特性：
         *      由于用户行为数据带有时间戳、我们当前要统计的热门商品不是 “(数据)在什么时候做计算，统计计算时候时间段的热门”，而是统计用户点击行为发生时候
         *  时间段的热门，因此应该使用事件时间。
         */
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)   // 定义事件时间语义

        // 从文件中读取数据，并转换成样例类；（由于是事件时间，因此需要指定当前时间戳和Watermark）提取时间戳生成Watermark
//        val inputPath: String = "datas/UserBehavior.csv"
//        val inputPath: String = "src/main/resources/datas/UserBehavior.csv"
//        val inputPath: String = "E:\\IDEAWorkspace\\UserBehaviorAnalysis\\HotItemsAnalysis\\src\\main\\resources\\datas\\UserBehavior.csv"
//        val inputStream: DataStream[String] = env.readTextFile(inputPath)

        // 从Kafka读取数据
        val properties: Properties = new Properties()
        properties.setProperty("bootstrap.servers", "hadoop202:9092")
        properties.setProperty("group.id", "consumer-group")
        properties.setProperty("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
        properties.setProperty("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")

        val inputStream: DataStream[String] = env.addSource(new FlinkKafkaConsumer[String]("hotitems", new SimpleStringSchema(), properties))

        val dataStream: DataStream[UserBehavior] = inputStream
                .map(
                    data => {
                        val arr: Array[String] = data.split(",")
                        // 因为用户行为数据各个字段间没有空格，这里我们暂不调用trim()————清洗后的数据，ETL时已经调用trim()处理过
                        UserBehavior(arr(0).toLong, arr(1).toLong, arr(2).toLong, arr(3), arr(4).toLong)
                    }
                )
                .assignAscendingTimestamps(_.timestamp * 1000L)     // 用户行为数据timestamp字段单位是秒
//                .assignAscendingTimestamps(UserBehavior => {UserBehavior.timestamp * 1000L})

        /**
         * TODO 业务流程：
         *      DataStream--分区-->KeyedStream--时间窗-->WindowedStream--聚合-->DataStream
         *          聚合：窗口聚合——.aggregate(new CountAgg(), new WindowResultFunction())
         *          其中，参数1：new CountAgg()——预聚合的增量聚合函数   参数2：new WindowResultFunction()——全窗口函数
         */
        // 得到窗口聚合结果
        val aggStream: DataStream[ItemViewCount] = dataStream
                .filter(_.behavior == "pv") // 过滤pv行为
                .keyBy("itemId") // 按照商品ID分组
                // 开窗
                .timeWindow(Time.hours(1), Time.minutes(5)) // 设置滑动窗口进行统计————统计1h之内、每5min统计一次
                .aggregate(new CountAgg(), new ItemViewCountWindowResult())

        /**
         * TODO 后续操作：
         *      基于窗口聚合结果 针对每一个窗口的数据做分组，
         *      然后再自己定义状态将其收集起来，（还要定义一个如100ms/1ms延迟触发的定时器）再进行排序；
         */
        val resultStream: DataStream[String] = aggStream
                .keyBy("windowEnd") // 按照窗口分组，收集当前窗口内的商品count数据
                .process(new TopNHotItems(5))   // 自定义处理流程

        dataStream.print("data")
        aggStream.print("agg")
        resultStream.print()

        env.execute("hotitems job")

    }

}


// TODO 定义输入数据样例类
case class UserBehavior(userId: Long, itemId: Long, categorId: Long, behavior: String, timestamp: Long)

// TODO 定义窗口聚合结果样例类（也可以不定义，直接使用三元组）
case class ItemViewCount(itemId: Long, windowEnd: Long, count: Long)

// TODO 定义输出数据样例类（这里直接使用样例类case class ItemViewCount，如果希望可视化效果更佳直观友好，可以包装成String字符串，
//    然后在控制台打印输出，形如："当前窗口时间是...，排名第一的热门商品是...，其count值为...；排名第二的热门商品是...，其count值为..."）

// 自定义预聚合函数AggregateFunction；聚合状态[createAccumulator/accumulator]就是当前商品的count值
class CountAgg() extends AggregateFunction[UserBehavior, Long, Long] {
    override def createAccumulator(): Long = 0L     // 聚合状态  // 0.0是Double类型

    // TODO 类似Table API中的accumulate()方法————每来一条数据调用一次add()，count值加1
    override def add(value: UserBehavior, accumulator: Long): Long = accumulator + 1

    // 返回ACC状态给.aggregate(..., ...)中的第二个参数WindowFunction进行操作
    override def getResult(accumulator: Long): Long = accumulator

    // 主要用在SessionWindow会话窗口中用于窗口合并——这里没什么用
    override def merge(a: Long, b: Long): Long = a + b
}

// 自定义窗口函数WindowFunction
class ItemViewCountWindowResult() extends WindowFunction[Long, ItemViewCount, Tuple, TimeWindow]{
    override def apply(key: Tuple, window: TimeWindow, input: Iterable[Long], out: Collector[ItemViewCount]): Unit = {
        val itemId: Long = key.asInstanceOf[Tuple1[Long]].f0
        val windowEnd: Long = window.getEnd
        val count: Long = input.iterator.next()
        out.collect(ItemViewCount(itemId, windowEnd, count))
    }
}

// 自定义KeyedProcessFunction
class TopNHotItems(topSize: Int) extends KeyedProcessFunction[Tuple, ItemViewCount, String]{

    /**
     * TODO 说明：
     *      1.因为涉及状态编程，因此需要先定义状态：ListState————每一个窗口都应该有一个ListState用于保存当前窗口内 所有的商品对应的count值
     *      2.如果采用生命周期的方式（定义状态），由于在一开始 类【class TopNHOtItems(topSize: Int)】本身创建的时候还没有运行时上下文，而定义键控状态（Keyed State）需要基于运行时
     *  上下文才能获取其状态句柄；所以，这里定义（状态）时可以在‘外面’先声明出来，然后到open()生命周期里面 从运行时上下文中获取（具体的状态句柄）
     */
    // 定义状态：ListState
    // ListState[T]：这里泛型T实际需要的是(itemId, count)二元组，而ItemViewCount仅比所需多了窗口信息（windowEnd属性），因此这里为了方便我们直接将泛型T定义为 ItemViewCount
    // private：仅当前类中能够访问该属性（状态），其它类中不允许访问（即使持有该类对象，除非该类提供了对应的get/set方法）——保证安全性（防止被修改）
    private var itemViewCountListState: ListState[ItemViewCount] = _

    override def open(parameters: Configuration): Unit = {
        itemViewCountListState = getRuntimeContext.getListState(new ListStateDescriptor[ItemViewCount]("itemViewCount-list", classOf[ItemViewCount]))
    }

    /**
     * TODO 说明：每来一个元素/一条数据【value: ItemViewCount】，都会到这里执行该方法
     *      value：过来的一个元素/一条数据
     *      ctx：上下文————因为是KeyedProcessFunction；上下文中有TimerService（可以注册定时器）、可以获取当前（数据）处理时间和Watermark，等等
     *      out：输出数据
     */
    override def processElement(value: ItemViewCount, ctx: KeyedProcessFunction[Tuple, ItemViewCount, String]#Context, out: Collector[String]): Unit = {
        /**
         * TODO 说明：
         *      我们希望每来一条数据就直接将其添加到itemViewCountListState: ListState[ItemViewCount]中进行排序操作，而且我们希望排序操作在“后面”【定时器被触发的时候】执行————如果
         *  希望进行优化，可以设置为每来一条数据就执行一次排序操作；而我们现在不做优化，因此只需要将到来的数据添加到状态中即可
         */
        // 每来一条数据，直接加入ListState
        itemViewCountListState.add(value)
        // 注册一个 windowEnd+1 之后触发的定时器
        /**
         * TODO 说明：
         *      我们希望每来一条数据就直接将其添加到itemViewCountListState: ListState[ItemViewCount]中进行排序操作，而且我们希望排序操作在“后面”【定时器被触发的时候】执行————如果
         *  希望进行优化，可以设置为每来一条数据就执行一次排序操作；而我们现在不做优化，因此只需要将到来的数据添加到状态中即可
         */
        ctx.timerService().registerEventTimeTimer(value.windowEnd + 1)
    }

    /**
     * TODO 说明：
     *      真正最后要做的排序、输出操作在这里完成
     */
    // 当定时器触发，可以认为所有窗口统计结果都已到齐，可以排序输出了
    override def onTimer(timestamp: Long, ctx: KeyedProcessFunction[Tuple, ItemViewCount, String]#OnTimerContext, out: Collector[String]): Unit = {
        // 为了方便排序，另外定义一个ListBuffer(可以动态地往里面添加数据) 用于保存ListState里面的所有数据
//        println(itemViewCountListState.get())
        val allItemViewCounts: ListBuffer[ItemViewCount] = ListBuffer()
        val iter: util.Iterator[ItemViewCount] = itemViewCountListState.get().iterator()
        while (iter.hasNext) {
            allItemViewCounts += iter.next()
        }

        // 清空状态：及时清空状态，可以节省内存空间
        itemViewCountListState.clear()

        // 按照count大小排序，取前n个
        val sortedItemViewCounts: ListBuffer[ItemViewCount] = allItemViewCounts.sortBy(_.count)(Ordering.Long.reverse).take(topSize)

        // 将排名信息格式化成String，便于打印输出可视化展示
        val result: StringBuilder = new StringBuilder
        result.append("窗口结束时间：").append(new Timestamp(timestamp - 1)).append("\n")

        // 遍历结果列表中的每个ItemViewCount，输出到一行
        for(i <- sortedItemViewCounts.indices) {
            val currentItemViewCount: ItemViewCount = sortedItemViewCounts(i)
            result.append("NO").append(i + 1).append("：\t")
                    .append("商品ID = ").append(currentItemViewCount.itemId).append("\t")
                    .append("热门度 = ").append(currentItemViewCount.count).append("\n")
        }

        // 每次for循环结束后，为不同窗口间设置间隔符
        result.append("\n==========================================\n\n")

        // 控制（每个窗口数据）输出显示频率：因为是从文件中读取数据过程太快（处理、输出结果也很快）
        Thread.sleep(1000)

        // 将result写出到输出缓冲中
        out.collect(result.toString())

    }
}