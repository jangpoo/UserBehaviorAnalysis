package com.zengyunning.networkflow_analysis

import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.scala.function.AllWindowFunction
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector

import java.net.URL

/**
 * Copyright (c) 2021 zengyunning All Rights Reserved
 *
 * Project: UserBehaviorAnalysis
 * Package: com.zengyunning.networkflow_analysis
 * Version: V 1.0
 *
 * Create by zengyunning on 2021/04/21 14:47
 */

// TODO 定义输入数据样例类
case class UserBehavior(userId: Long, itemId: Long, categorId: Long, behavior: String, timestamp: Long)

// TODO 定义输出pv统计的样例类
case class UvCount(windowEnd: Long, count: Long)

object UniqueVistor {

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
                .timeWindowAll(Time.hours(1)) // 直接不分组，基于DataStream开1小时滚动窗口
                .apply(new UvCountResult())     // TODO 参数：全窗口函数——而不采用之前的预聚合操作

        uvStream.print()

        env.execute("uv job")

    }

}

// 自定义实现全窗口函数；用一个Set结构来保存所有的userId，进行自动去重【for循环遍历、逐个元素/数据比较userId的方式比较麻烦】
class UvCountResult() extends AllWindowFunction[UserBehavior, UvCount, TimeWindow] {
    override def apply(window: TimeWindow, input: Iterable[UserBehavior], out: Collector[UvCount]): Unit = {
        // 定义一个Set
        var userIdSet: Set[Long] = Set[Long]()

        // 遍历窗口中的所有数据，把userId添加到Set中，自动去重
        for(userBehavior <- input) {
            userIdSet += userBehavior.userId
        }

        // 将set的size作为去重后的uv值输出
        out.collect(UvCount(window.getEnd, userIdSet.size))

    }
}
