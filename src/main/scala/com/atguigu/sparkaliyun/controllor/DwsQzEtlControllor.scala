package com.atguigu.sparkaliyun.controllor

import com.atguigu.sparkaliyun.service.DwsQzEtlService
import com.atguigu.sparkaliyun.util.HiveUtil
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SparkSession

object DwsQzEtlControllor {
    def main(args: Array[String]): Unit = {
        val conf: SparkConf = new SparkConf().setAppName("DwsQzEtl")

        val spark: SparkSession = SparkSession.builder().config(conf).enableHiveSupport().getOrCreate()

        val sc: SparkContext = spark.sparkContext

        sc.hadoopConfiguration.set("fs.defaultFS","hdfs://nameservice1")
        sc.hadoopConfiguration.set("dfs.nameservices","nameservice1  ")

        HiveUtil.setMaxpartitions(spark)
        HiveUtil.openCompression(spark)

        val dt = "20190722"
        DwsQzEtlService.saveDwsQzChapter(spark,dt)
        DwsQzEtlService.saveDwsQzCourse(spark,dt)
        DwsQzEtlService.saveDwsQzMajor(spark,dt)
        DwsQzEtlService.saveDwsQzPaper(spark,dt)
        DwsQzEtlService.saveDwsQzQuestionTpe(spark,dt)
        DwsQzEtlService.saveDwsUserPaperDetail(spark,dt)
    }
}
