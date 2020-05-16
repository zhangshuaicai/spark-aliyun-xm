package com.atguigu.sparkaliyun.controllor

import com.atguigu.sparkaliyun.service.{AdsDataEtlService, DwsDataEtlService}
import com.atguigu.sparkaliyun.util.HiveUtil
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SparkSession

object AdsDataEtlControllor {
    def main(args: Array[String]): Unit = {
        val conf: SparkConf = new SparkConf().setAppName("AdsDataEtl")

        val spark: SparkSession = SparkSession.builder().config(conf).enableHiveSupport().getOrCreate()

        val sc: SparkContext = spark.sparkContext

        sc.hadoopConfiguration.set("fs.defaultFS","hdfs://nameservice1")
        sc.hadoopConfiguration.set("dfs.nameservices","nameservice1  ")

        HiveUtil.openDynamicPartition(spark)

        //dws  ----->   ads
        AdsDataEtlService.queryDetailApi(spark, "20190722")
        AdsDataEtlService.queryDetailSql(spark, "20190722")

    }
}
