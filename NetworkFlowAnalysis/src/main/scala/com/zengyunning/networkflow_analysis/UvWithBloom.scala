package com.zengyunning.networkflow_analysis

import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.scala.function.ProcessWindowFunction
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.triggers.{Trigger, TriggerResult}
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector
import redis.clients.jedis.Jedis

import java.lang
import java.net.URL

/**
 * Copyright (c) 2021 zengyunning All Rights Reserved
 *
 * Project: UserBehaviorAnalysis
 * Package: com.zengyunning.networkflow_analysis
 * Version: V 1.0
 *
 * Create by zengyunning on 2021/04/21 21:29
 */
object UvWithBloom {

    def main(args: Array[String]): Unit = {

        val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
        env.setParallelism(1)

        // 从文件中读取数据
        val resource: URL = getClass.getResource("/datas/UserBehavior.csv")
        val inputStream: DataStream[String] = env.readTextFile(resource.getPath)

        // 转换成样例类类型，并提取时间戳和Watermark
        val dataStream: DataStream[UserBehavior] = inputStream
                .map(
                    data => {
                        val arr: Array[String] = data.split(",")
                        UserBehavior(arr(0).toLong, arr(1).toLong, arr(2).toLong, arr(3), arr(4).toLong)
                    }
                )
                .assignAscendingTimestamps(_.timestamp * 1000L) // 用户行为数据timestamp字段单位是秒

        val uvStream: DataStream[UvCount] = dataStream
                .filter(_.behavior == "uv")
                .map(data => ("uv", data.userId))  // 这里为了方便选择使用dummy key“哑key”——用于分组开窗（聚合操作）
                .keyBy(_._1)  // TODO 所有数据全部分到同一组【这里keyBy(...)+ .timeWindow(...) 类似于/= .timeWindowAll(...)，实际开发禁用】
                .timeWindow(Time.hours(1))  // 开1小时窗口
                .trigger(new MyTrigger())   // 自定义触发器  // TODO trigger()为可选API
                .process(new UvCountWithBloom())

        uvStream.print()

        env.execute("uv with bloom job")

    }

}

// 自定义触发器：每来一条数据，直接触发窗口计算并清空窗口状态
// TODO onXxx(...)类似于onTimer(...)，意为定时触发执行【某个事件来了之后要进行的操作】，类似于前端js的onClick(...)事件等
class MyTrigger() extends Trigger[(String, Long), TimeWindow] {

    // TODO onElement(...)：每来一个数据/元素时要执行的操作
    // 当前需求下要做的操作：每来一个数据时，清空状态、触发窗口计算
    override def onElement(element: (String, Long), timestamp: Long, window: TimeWindow, ctx: Trigger.TriggerContext): TriggerResult = {
        TriggerResult.FIRE_AND_PURGE
    }

    // TODO onProcessingTime(...)：当前系统时间【处理时间】有“进展”时要执行的操作
    // 当前是事件时间，因此onProcessingTime(...)不需要做“什么”操作
    override def onProcessingTime(time: Long, window: TimeWindow, ctx: Trigger.TriggerContext): TriggerResult = TriggerResult.CONTINUE

    // TODO onEventTime(...)：收到Watermark或有Watermark改变时要执行的操作
    // 当前不“关心”Watermark，只“关心”数据——数据来了之后，直接触发（窗口）计算、清空（状态），即onElement(...)方法
    override def onEventTime(time: Long, window: TimeWindow, ctx: Trigger.TriggerContext): TriggerResult = TriggerResult.CONTINUE

    // TODO clear(...)：在自定义Trigger中也可以定义状态或上下文相关内容，在该方法中进行“收尾”/清理工作
    // 当前自定义Trigger中未定义任何内容，因此不需要执行任何操作；（并且该方法无返回值——Unit）
    override def clear(window: TimeWindow, ctx: Trigger.TriggerContext): Unit = {}
}

// 自定义一个布隆过滤器，主要就是一个Bitmap位图和hash函数【优化算法：从众多hash函数中选择最优的】
class MyBloomFilter(bitmapSize: Long) extends Serializable {
    /**
     * TODO 说明：
     *      1.此处不能直接new创建Bitmap位图实例，否则占用的仍是内存空间，因此这里我们只定义当前Bitmap位图的大小，而“具体的Bitmap位图放到Redis中（进行定义）”；
     *      2.真实（UV）优化场景下，Bitmap位图大小（bitmapSize）一般为2的N次方幂，这样方便换算为MB/GB等储存单位和内存管理；这里我们为了方便，不进行“算法调整”（直接从外部参数传入）；
     */
    // 定义Bitmap位图大小
    private val cap: Long = bitmapSize

    /**
     * TODO 说明：
     *      1.本身系统底层有hashCode()函数，这里我们为了教学演示，选择自定义hash()函数；
     *      2.参数：
     *          value：对外部传入的参数进行hash计算；当前对userId进行hash计算，这里我们定义为String类型（比Long类型涵盖范围更广）
     *          seed：随机数种子——进行随机化处理（将hash“打散”） Long：hash()函数返回值类型
     */
    // hash函数
    def hash(value: String, seed: Int): Long = {
        // 预定义返回值
        var result: Long = 0L

        for(i <- 0 until value.length) {
            result = result * seed + value.charAt(i)
        }

        // 返回hash值，要映射到cap范围内
        (cap - 1) & result
    }
}

/**
 * TODO 说明：
 *      1.ProcessWindowFunction：是一个全窗口函数——和WindowFunction的类型定义相同；
 *      2.process(...)：本身该方法调用时机 不是每来一条数据就执行该方法【KeyedProcessFunction-processElement(...)】，而是所有数据都“收集齐了”之后、触发窗口计算的时候才会调用该方法【同WidowFunction】；但当前
 *  定义Trigger触发器后——每来一条数据，.trigger(new MyTrigger())就会触发一次.process(new UvCountWithBloom())窗口函数的调用；因此，相当于每来一条数据，都会调用一次process(...)方法——只不过elements: Iterable[(String, Long)]
 *  只有“一个数”而已；
 *      3.定义redis连接：本来常见的方式是在open(...)生命周期中创建连接，这里我们为了方便采用lazy（成员变量）的方式进行定义；
 */
// 实现自定义的窗口处理函数
class UvCountWithBloom() extends ProcessWindowFunction[(String, Long), UvCount, String, TimeWindow] {
    // 定义redis连接以及布隆过滤器
//    lazy private val jedis: Jedis = new Jedis("hadoop202", 6379)
//    private lazy val jedis: Jedis = new Jedis("hadoop202", 6379)
    lazy val jedis: Jedis = new Jedis("hadoop202", 6379)
//    private val bloomFilter: MyBloomFilter = new MyBloomFilter(1 << 29)
    lazy val bloomFilter: MyBloomFilter = new MyBloomFilter(1 << 29)  // 位的个数：2^6(64) * 2^20(1M) * 2^6(8bit)，64MB

    // 本来是收集齐所有数据、窗口触发计算的时候才会调用；现在每来一条数据都调用一次
    override def process(key: String, context: Context, elements: Iterable[(String, Long)], out: Collector[UvCount]): Unit = {
        // 先定义redis中存储位图的key
        val storedBitmapKey: String = context.window.getEnd.toString

        // 另外将当前窗口的UV count值，作为状态保存到redis中，用一个叫作uvcount的hash表来保存 (windowEnd, count)
        val uvCountMap: String = "uvcount"
        val currentKey: String = context.window.getEnd.toString
        var count: Long = 0L

        // 从redis中取出当前窗口的UV count值
        val jedisStr: String = jedis.hget(uvCountMap, currentKey)
        if(jedisStr != null) {
            count = jedisStr.toLong
        }

        // 去重：判断当前userId的hash值对应的位图位置，是否为0（1：存在；0：不存在）
        val userId: String = elements.last._2.toString
        // 计算hash值，就对应着位图中的偏移量
        val offset: Long = bloomFilter.hash(userId, 61)
        // 用redis的位操作命令，取Bitmap中对应的值
        val isExist: lang.Boolean = jedis.getbit(storedBitmapKey, offset)
        if(!isExist) {
            // 如果不存在，那么位图对应位置 置1，并且将count值加1
            jedis.setbit(storedBitmapKey, offset, true)
            jedis.hset(uvCountMap, currentKey, (count + 1).toString)
        }

    }
}