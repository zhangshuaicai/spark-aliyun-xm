package com.atguigu.sparkaliyun.producer

import java.util.Properties

import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}
import org.apache.spark.{SparkConf, SparkContext}

object PageLogProducer {
    def main(args: Array[String]): Unit = {
        val conf: SparkConf = new SparkConf().setAppName("PageLogProducer").setMaster("local[*]")
        System.setProperty("hadoop.home.dir", "C:\\Program Files\\hadoop-common-2.2.0-bin-master")
        val sc = new SparkContext(conf)
        sc.textFile(this.getClass.getResource("/input/page.log").getPath,10)
            .foreachPartition(partition=>{
                val props = new Properties()
                props.put("bootstrap.servers", "hadoop102:9092,hadoop103:9092,hadoop104:9092")
                props.put("acks", "1")
                props.put("batch.size", "16384")
                props.put("linger.ms", "10")
                props.put("buffer.memory", "33554432")
                props.put("key.serializer",
                    "org.apache.kafka.common.serialization.StringSerializer")
                props.put("value.serializer",
                    "org.apache.kafka.common.serialization.StringSerializer")
                val producer = new KafkaProducer[String, String](props)
                partition.foreach(item=>{
                    val msg = new ProducerRecord[String, String]("page_topic", item)
                    producer.send(msg)
                })
                producer.flush()
                producer.close()
            })
    }
}
