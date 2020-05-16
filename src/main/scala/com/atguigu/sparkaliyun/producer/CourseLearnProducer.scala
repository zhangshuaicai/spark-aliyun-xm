package com.atguigu.sparkaliyun.producer

import java.util.Properties

import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}
import org.apache.spark.{SparkConf, SparkContext}

object CourseLearnProducer {
    def main(args: Array[String]): Unit = {
        val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("CourseLearnProducer")
        System.setProperty("hadoop.home.dir", "C:\\Program Files\\hadoop-common-2.2.0-bin-master")
        val sc = new SparkContext(conf)
        sc.textFile(this.getClass.getResource("/input/course_learn.log").getPath,10)
            .foreachPartition(partitions=>{
                val props = new Properties()
                props.put("bootstrap.servers", "hadoop001:9092,hadoop002:9092,hadoop003:9092")
                props.put("acks", "1")
                props.put("batch.size", "16384")
                props.put("linger.ms", "10")
                props.put("buffer.memory", "33554432")
                props.put("key.serializer",
                    "org.apache.kafka.common.serialization.StringSerializer")
                props.put("value.serializer",
                    "org.apache.kafka.common.serialization.StringSerializer")
                val producer = new KafkaProducer[String,String](props)
                partitions.foreach(item=>{
                    val record = new ProducerRecord[String,String]("course_learn",item)
                    producer.send(record)
                })
                producer.flush()
                producer.close()
            })
    }
}
