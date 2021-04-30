package com.zengyunning.orderpay_detect

import org.apache.flink.api.common.state.{ValueState, ValueStateDescriptor}
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.co.CoProcessFunction
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
 * Create by zengyunning on 2021/04/29 21:21
 */

/**
 *  说明：
 *      实时对账两条流：订单的事件流OrderLog.csv、对账时查询平台账户到账信息的流ReceiptLog.csv
 */
// 定义到账事件样例类
case class ReceiptEvent(txId: String, payChannel: String, timestamp: Long)

object TxMatch {

    def main(args: Array[String]): Unit = {

        val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
        env.setParallelism(1)

        // 1.读取订单事件数据
        val orderUrl: URL = getClass.getResource("/datas/OrderLog.csv")
        val orderInputStream: DataStream[String] = env.readTextFile(orderUrl.getPath)
//        val inputStream: DataStream[String] = env.socketTextStream("hadoop202", 7777)

        val orderEventStream: DataStream[OrderEvent] = orderInputStream
                .map(
                    data => {
                        val arr: Array[String] = data.split(",")
                        OrderEvent(arr(0).toLong, arr(1), arr(2), arr(3).toLong)
                    }
                )
                .assignAscendingTimestamps(_.timestamp * 1000L)
                .filter(_.eventType == "pay")
                .filter(_.txId != "")
                .keyBy(_.txId)

        // 2.读取到账事件数据
        val receiptUrl: URL = getClass.getResource("/datas/ReceiptLog.csv")
        val receiptInputStream: DataStream[String] = env.readTextFile(receiptUrl.getPath)

        val receiptEventStream: DataStream[ReceiptEvent] = receiptInputStream
                .map(
                    data => {
                        val arr: Array[String] = data.split(",")
                        ReceiptEvent(arr(0), arr(1), arr(2).toLong)
                    }
                )
                .assignAscendingTimestamps(_.timestamp * 1000L)  // Watermark默认延迟1ms
                .keyBy(_.txId)

        // 3.合并两条流，进行处理
        /**
         *  合流两种方式：
         *      1.Connect：“一国两制”——两条流的数据类型可以不同；
         *      2.Union：两条流的数据类型必须相同；
         */
        val resultStream: DataStream[(OrderEvent, ReceiptEvent)] = orderEventStream.connect(receiptEventStream)
                .process(new TxPayMatchResult())

        resultStream.print("matched")
        resultStream.getSideOutput(new OutputTag[OrderEvent]("unmatched-pay")).print("unmatched pays")
        resultStream.getSideOutput(new OutputTag[ReceiptEvent]("unmatched-receipt")).print("unmatched receipts")

        env.execute("tx match job")

    }
}

class TxPayMatchResult() extends CoProcessFunction[OrderEvent, ReceiptEvent, (OrderEvent, ReceiptEvent)] {
    // 定义状态：保存当前交易对应的 订单支付事件 和 到账事件
    lazy val payEventState: ValueState[OrderEvent] = getRuntimeContext.getState(new ValueStateDescriptor[OrderEvent]("pay", classOf[OrderEvent]))
    lazy val receiptEventState: ValueState[ReceiptEvent] = getRuntimeContext.getState(new ValueStateDescriptor[ReceiptEvent]("receipt", classOf[ReceiptEvent]))
    /**
     *  TODO 说明：
     *      不需要删除定时器【偷懒儿】——不需要定义timerTsState保存定时器时间戳 以为删除定时器使用；......定时器“到点儿啥都不干”
     */

    // 侧输出流标签
    lazy val unmatchedPayEventOutputTag: OutputTag[OrderEvent] = new OutputTag[OrderEvent]("unmatched-pay")
    lazy val unmatchedReceiptEventOutputTag: OutputTag[ReceiptEvent] = new OutputTag[ReceiptEvent]("unmatched-receipt")

    override def processElement1(pay: OrderEvent, ctx: CoProcessFunction[OrderEvent, ReceiptEvent, (OrderEvent, ReceiptEvent)]#Context, out: Collector[(OrderEvent, ReceiptEvent)]): Unit = {
        // 订单支付来了，要判断之前是否有到账事件
        val receipt: ReceiptEvent = receiptEventState.value()
        if(receipt != null) {
            // 如果已经有receipt，正常输出匹配，清空状态
            out.collect((pay, receipt))
            receiptEventState.clear()
            payEventState.clear()  // TODO 可以不必清空————参数pay: OrderEvent事件首次到达，payEventState中没有数据（元素）/事件
        }else {
            // 如果（receipt）还没来【若提前/先于pay到达无所谓】，注册定时器开始等待5秒【具体等待时长取决于ReceiptLog.csv中数据】
            ctx.timerService().registerEventTimeTimer(pay.timestamp * 1000L + 5000L)
            // 更新状态
            payEventState.update(pay)
        }
    }

    override def processElement2(receipt: ReceiptEvent, ctx: CoProcessFunction[OrderEvent, ReceiptEvent, (OrderEvent, ReceiptEvent)]#Context, out: Collector[(OrderEvent, ReceiptEvent)]): Unit = {
        // 到账事件来了，要判断之前是否有pay事件
        val pay: OrderEvent = payEventState.value()
        if(pay != null) {
            // 如果已经有pay，正常输出匹配，清空状态
            out.collect((pay, receipt))
            receiptEventState.clear()// TODO 可以不必清空————参数receipt: ReceiptEvent事件首次到达，receiptEventState中没有数据（元素）/事件
            payEventState.clear()
        }else {
            // 如果（pay）还没来【若提前/先于receipt到达无所谓】，注册定时器开始等待3秒【具体等待时长取决于OrderLog.csv中数据】
            ctx.timerService().registerEventTimeTimer(receipt.timestamp * 1000L + 3000L)
            // 更新状态
            receiptEventState.update(receipt)
        }
    }

    /**
     *  说明：
     *      1.定时器触发时机：由于我们前面没有定义删除定时器，因此定时器在哪种情况下都可能触发（只要注册了定时器）；
     *      2.定时器触发时的几种情况：
     *          1>、假如成功“匹配”（if条件满足），则状态（payEventState、receiptEventState）都会被清空——>定时器“到点儿啥都不干”；
     *          2>、假如“匹配”失败（if条件不满足），则状态（payEventState、receiptEventState）中会有一个没被清空【不会存在两个状态都没被清空的情况——因为pay、receipt必定有先后顺序，“第二个”来了之后就会
     *              out.collect((pay, receipt))输出、清空状态】；
     *              状态（payEventState、receiptEventState）中有一个没被清空：代表另外一个（pay或receipt事件）没来【即没被清空的状态（代表的事件）来了、一直在等，等到定时器触发，“另一个”还没来】；
     *
     */
    override def onTimer(timestamp: Long, ctx: CoProcessFunction[OrderEvent, ReceiptEvent, (OrderEvent, ReceiptEvent)]#OnTimerContext, out: Collector[(OrderEvent, ReceiptEvent)]): Unit = {
        // 定时器触发，判断状态中哪个还存在，就代表另一个没来，输出到侧输出流
        if(payEventState.value() != null) {
            // pay来了，receipt没来
            ctx.output(unmatchedPayEventOutputTag, payEventState.value())
        }
        if(receiptEventState.value() != null) {
            // receipt来了，pay没来
            ctx.output(unmatchedReceiptEventOutputTag, receiptEventState.value())
        }

        // 清空状态————无论前面的两个if逻辑是否执行（输出侧输出流），都清空状态【“就算啥都没做”，最多白清空一次】
        receiptEventState.clear()
        payEventState.clear()
    }
}