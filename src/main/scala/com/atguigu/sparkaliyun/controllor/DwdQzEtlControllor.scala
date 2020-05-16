package com.atguigu.sparkaliyun.controllor

import com.atguigu.sparkaliyun.service.DwdQzEtlService
import com.atguigu.sparkaliyun.util.HiveUtil
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SparkSession

object DwdQzEtlControllor {
    def main(args: Array[String]): Unit = {
        val conf: SparkConf = new SparkConf().setAppName("DwdQzEtl")

        val spark: SparkSession = SparkSession.builder().config(conf).enableHiveSupport().getOrCreate()

        val sc: SparkContext = spark.sparkContext

        sc.hadoopConfiguration.set("fs.defaultFS","hdfs://nameservice1")
        sc.hadoopConfiguration.set("dfs.nameservices","nameservice1  ")

        HiveUtil.setMaxpartitions(spark)
        HiveUtil.openCompression(spark)

        DwdQzEtlService.dwdQz(sc,spark)

    }
}
