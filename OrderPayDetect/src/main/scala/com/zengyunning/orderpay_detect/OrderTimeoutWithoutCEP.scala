package com.zengyunning.orderpay_detect

import org.apache.flink.api.common.state.{ValueState, ValueStateDescriptor}
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.streaming.api.scala._
import org.apache.flink.util.Collector

import java.net.URL

/**
 * Copyright (c) 2021 zengyunning All Rights Reserved
 *
 * Project: UserBehaviorAnalysis
 * Package: com.zengyunning.orderpay_detect
 * Version: V 1.0
 *
 * Create by zengyunning on 2021/04/28 21:29
 */
object OrderTimeoutWithoutCEP {

    def main(args: Array[String]): Unit = {

        val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
        env.setParallelism(1)

        // 0.从文件中读取数据
        val url: URL = getClass.getResource("/datas/OrderLog.csv")
        val inputStream: DataStream[String] = env.readTextFile(url.getPath)
//        val inputStream: DataStream[String] = env.socketTextStream("hadoop202", 7777)

        val orderEventStream: DataStream[OrderEvent] = inputStream
                .map(
                    data => {
                        val arr: Array[String] = data.split(",")
                        OrderEvent(arr(0).toLong, arr(1), arr(2), arr(3).toLong)
                    }
                )
                .assignAscendingTimestamps(_.timestamp * 1000L)


        // 自定义ProcessFunction进行复杂事件的检测
        val orderResultStream: DataStream[OrderResult] = orderEventStream
                .keyBy(_.orderId)
                .process(new OrderPayMatchResult())

        orderResultStream.print("paid")
        orderResultStream.getSideOutput(new OutputTag[OrderResult]("timeout")).print("timeout")

        env.execute("order timeout without CEP")
    }
}

// 自定义实现KeyedProcessFunction
class OrderPayMatchResult() extends KeyedProcessFunction[Long, OrderEvent, OrderResult] {

    // 定义状态：标识位表示create、pay是否已经来过（create、pay事件/数据可能乱序），保存定时器时间戳
    lazy val isCreatedState: ValueState[Boolean] = getRuntimeContext.getState(new ValueStateDescriptor[Boolean]("is-created", classOf[Boolean]))
    lazy val isPaidState: ValueState[Boolean] = getRuntimeContext.getState(new ValueStateDescriptor[Boolean]("is-paid", classOf[Boolean]))
    lazy val timerTsState: ValueState[Long] = getRuntimeContext.getState(new ValueStateDescriptor[Long]("timer-ts", classOf[Long]))
    // 定义侧输出流标签
    lazy val orderTimeoutOutputTag: OutputTag[OrderResult] = new OutputTag[OrderResult]("timeout")


    /**
     * TODO 说明：
     *      1.processElement(...)：每来一条数据都会调用/执行该方法；
     *          在该方法中可以：
     *              * （定义状态后）获取状态；
     *              * ctx：获取当前上下文环境；
     *              * 注册定时器；
     *              * 输出侧输出流；
     */
    override def processElement(value: OrderEvent, ctx: KeyedProcessFunction[Long, OrderEvent, OrderResult]#Context, out: Collector[OrderResult]): Unit = {
        // 先拿到当前状态
        val isCreated: Boolean = isCreatedState.value()
        val isPaid: Boolean = isPaidState.value()
        val timerTs: Long = timerTsState.value()

        // 判断当前事件类型，看是create还是pay
        // 1. 来的是create，（因为数据-create、pay事件可能乱序）要继续判断是否pay过
        if(value.eventType == "create") {
            // 1.1 如果已经支付过，正常支付，输出匹配成功的结果
            if(isPaid) {
                out.collect(OrderResult(value.orderId, "paid successfully"))
                // 已经处理完毕，清空状态和定时器
                isCreatedState.clear()
                isPaidState.clear()
                timerTsState.clear()
                ctx.timerService().deleteEventTimeTimer(timerTs)
            }else {
                // 1.2 如果尚未pay支付过，注册定时器，等待15分钟
                val ts: Long = value.timestamp * 1000L + 15 * 60 * 1000L
                ctx.timerService().registerEventTimeTimer(ts)
                // 更新状态
                timerTsState.update(ts)
                isCreatedState.update(true)
            }
        // 2. 如果当前来的是pay，要判断是否create过（因为数据-create、pay事件可能乱序——似乎没有必要，pay操作必定有create操作，只不过数据先后到达而已）
        }else if(value.eventType == "pay") {
            if(isCreated) {
                // 2.1 如果已经create过，匹配成功，还要判断一下pay时间是否超过了定时器时间（15min）——TODO 问题：似乎不用判断当前逻辑，既然是value.eventType == "pay"，肯定没有超时15min【付款倒计时、超时关闭】
                if(value.timestamp * 1000L < timerTs) {
                    // 2.1.1 没有超时，正常输出
                    out.collect(OrderResult(value.orderId, "paid successfully"))
                }else {
                    // 2.1.2 已经超时，输出超时（侧输出流）
                    ctx.output(orderTimeoutOutputTag, OrderResult(value.orderId, "paid but already timeout"))
                }

                // 只要输出结果，当前order处理已经结束，清空状态和定时器
                isCreatedState.clear()  // TODO 似乎没有update(...)过，会否NPE
                isPaidState.clear()
                timerTsState.clear()
                ctx.timerService().deleteEventTimeTimer(timerTs)
            }else {
                // 2.2 如果create没来，注册定时器，等到pay的时间就可以
                ctx.timerService().registerEventTimeTimer(value.timestamp * 1000L)  // TODO 不是立即触发定时器——事件时间语义EventTimeTimer触发的条件是Watermark涨到当前时间戳；乱序数据......
                // 更新状态
                timerTsState.update(value.timestamp * 1000L)
                isPaidState.update(true)
            }
        }
    }

    override def onTimer(timestamp: Long, ctx: KeyedProcessFunction[Long, OrderEvent, OrderResult]#OnTimerContext, out: Collector[OrderResult]): Unit = {
        // 定时器触发
        // 1. pay来了，没等到create
        if(isPaidState.value()) {
            ctx.output(orderTimeoutOutputTag, OrderResult(ctx.getCurrentKey, "paid but create not found log"))
        }else {
            // 2. create来了，没有pay
            ctx.output(orderTimeoutOutputTag, OrderResult(ctx.getCurrentKey, "order timeout"))
        }

        // 清空状态
        isCreatedState.clear()
        isPaidState.clear()
        timerTsState.clear()
    }
}
