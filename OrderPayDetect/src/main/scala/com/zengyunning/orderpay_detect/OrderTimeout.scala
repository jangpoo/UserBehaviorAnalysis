package com.zengyunning.orderpay_detect

import org.apache.flink.cep.{PatternSelectFunction, PatternTimeoutFunction}
import org.apache.flink.cep.scala.{CEP, PatternStream}
import org.apache.flink.cep.scala.pattern.Pattern
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.time.Time

import java.net.URL
import java.util

/**
 * Copyright (c) 2021 zengyunning All Rights Reserved
 *
 * Project: UserBehaviorAnalysis
 * Package: com.zengyunning.orderpay_detect
 * Version: V 1.0
 *
 * Create by zengyunning on 2021/04/27 21:04
 */

// 定义输入数据样例类类型
case class OrderEvent(orderId: Long, eventType: String, txId: String, timestamp: Long)
// 定义输出数据样例类类型  TODO 若要分担业务系统压力，应定义为直接更改业务数据库中的“订单状态”【如果订单超时，直接连接到业务系统、修改对应订单的状态为timeout超时状态，这样业务系统就不用做任何操作】
case class OrderResult(orderId: Long, resultMsg: String)

object OrderTimeout {

    def main(args: Array[String]): Unit = {

        val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)   // TODO 事件时间：数据源中带有时间戳——判断订单是否超时，应按照订单的下单时间，而不是系统处理时间
        env.setParallelism(1)

        // 0.从文件中读取数据
//        val url: URL = getClass.getResource("/datas/OrderLog.csv")
//        val inputStream: DataStream[String] = env.readTextFile(url.getPath)
        val inputStream: DataStream[String] = env.socketTextStream("hadoop202", 7777)

        val orderEventStream: KeyedStream[OrderEvent, Long] = inputStream
                .map(
                    data => {
                        val arr: Array[String] = data.split(",")
                        OrderEvent(arr(0).toLong, arr(1), arr(2), arr(3).toLong)
                    }
                )
                .assignAscendingTimestamps(_.timestamp * 1000L)
                .keyBy(_.orderId)

        // 1.定义一个Pattern
        val orderPayPattern: Pattern[OrderEvent, OrderEvent] = Pattern
                .begin[OrderEvent]("create").where(_.eventType == "create")
                .followedBy("pay").where(_.eventType == "pay")
                .within(Time.minutes(15))

        // 2.将Pattern应用到数据流上，进行模式检测/匹配
        val patternStream: PatternStream[OrderEvent] = CEP.pattern(orderEventStream, orderPayPattern)

        // 3.定义侧输出流标签，用于处理超时事件
        val orderTimeoutOutputTag: OutputTag[OrderResult] = new OutputTag[OrderResult]("orderTimeout")

        // 4.调用select方法，提取并处理匹配的成功支付事件以及超时事件
        val resultStream: DataStream[OrderResult] = patternStream.select(orderTimeoutOutputTag, new OrderTimeoutSelect(), new OrderPaySelect())

        resultStream.print("paid")
        resultStream.getSideOutput(orderTimeoutOutputTag).print("timeout")

        env.execute("order timeout job")

    }
}

// 实现自定义的PatternTimeoutFunction以及PatternSelectFunction
class OrderTimeoutSelect() extends PatternTimeoutFunction[OrderEvent, OrderResult] {
    override def timeout(pattern: util.Map[String, util.List[OrderEvent]], timeoutTimestamp: Long): OrderResult = {
//        val timeoutOrderId: Long = pattern.get("create").get(0).orderId
        val timeoutOrderId: Long = pattern.get("create").iterator().next().orderId
        OrderResult(timeoutOrderId, "timeout：" + timeoutTimestamp)
    }
}

class OrderPaySelect() extends PatternSelectFunction[OrderEvent, OrderResult] {
    override def select(pattern: util.Map[String, util.List[OrderEvent]]): OrderResult = {
//        val payOrderId: Long = pattern.get("create").get(0).orderId
        val payOrderId: Long = pattern.get("pay").iterator().next().orderId
        OrderResult(payOrderId, "paid successfully")
    }
}