package com.atguigu.sparkaliyun.stream

import com.atguigu.sparkaliyun.constant.TopicConstants
import com.atguigu.sparkaliyun.util.MyKafkaUtil
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.{SparkConf, SparkContext}

/**
 * 需求1：实时统计注册人数，批次为3秒一批，
 * 使用updateStateBykey算子计算历史数据和当前批次的数据总数，
 * 仅此需求使用updateStateBykey，后续需求不使用updateStateBykey。
 */
object RegisterChartRealTime {
    def main(args: Array[String]): Unit = {
        val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("RegisterChartRealTime")
        val sc = new SparkContext(conf)
        sc.hadoopConfiguration.set("fs.defaultFS", "hdfs://nameservice1")
        sc.hadoopConfiguration.set("dfs.nameservices", "nameservice1")
        val ssc = new StreamingContext(sc,Seconds(3))
        val registerStream: InputDStream[ConsumerRecord[String, String]]
                                        = MyKafkaUtil.getKafkaStream(TopicConstants.REGISTER_TOPIC,ssc)

        val mapStream: DStream[(String, Int)] = registerStream.map(record => {
            val line: Array[String] = record.value().split("\t")
            //((line(0).toLong,line(1).toLong,line(2).toString),1)
            ("register", 1)
        })

        def updateFunc(newValue:Seq[Int],runningCount:Option[Int]):Option[Int] = {
            val newCount:Int = (0 /: newValue)(_ + _) + runningCount.getOrElse(0)
            Some(newCount)
        }

        sc.setCheckpointDir("/streamcheckpoint")
        val resultDs: DStream[(String, Int)] = mapStream.updateStateByKey[Int](updateFunc _)

        resultDs.print()

        ssc.start()
        ssc.awaitTermination()
    }
}
