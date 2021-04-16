package com.zengyunning.hotitems_analysis.utils

import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}

import java.util.Properties
import scala.io.BufferedSource

/**
 * Copyright (c) 2021 zengyunning All Rights Reserved
 *
 * Project: UserBehaviorAnalysis
 * Package: com.zengyunning.hotitems_analysis.utils
 * Version: V 1.0
 *
 * Create by zengyunning on 2021/04/15 22:21
 */
object KafkaProducerUtil {

    def main(args: Array[String]): Unit = {
        writeToKafka("hotitems")
    }

    def writeToKafka(topic: String): Unit = {
        val properties: Properties = new Properties()
        properties.setProperty("bootstrap.servers", "hadoop202:9092")
        properties.setProperty("key.serializer", "org.apache.kafka.common.serialization.StringSerializer")
        properties.setProperty("value.serializer", "org.apache.kafka.common.serialization.StringSerializer")

        // 创建Kafka生产者Producer
        val producer: KafkaProducer[String, String] = new KafkaProducer[String, String](properties)

        // 从文件读取数据，逐行写入Kafka
        val bufferedSource: BufferedSource = io.Source.fromFile("E:\\IDEAWorkspace\\UserBehaviorAnalysis\\HotItemsAnalysis\\src\\main\\resources\\datas\\UserBehavior.csv")
        for(line <- bufferedSource.getLines()) {
            val record: ProducerRecord[String, String] = new ProducerRecord[String, String](topic, line)
            producer.send(record)
        }

        bufferedSource.close()
        producer.close()

    }
}
