package com.atguigu.sparkaliyun.controllor

import com.atguigu.sparkaliyun.service.{DwdDateEtlService, DwsDataEtlService}
import com.atguigu.sparkaliyun.util.HiveUtil
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SparkSession

object DwsDataEtlControllor {
    def main(args: Array[String]): Unit = {
        val conf: SparkConf = new SparkConf().setAppName("DwsDataEtl")

        val spark: SparkSession = SparkSession.builder().config(conf).enableHiveSupport().getOrCreate()

        val sc: SparkContext = spark.sparkContext

        sc.hadoopConfiguration.set("fs.defaultFS","hdfs://nameservice1")
        sc.hadoopConfiguration.set("dfs.nameservices","nameservice1  ")

        HiveUtil.setMaxpartitions(spark)
        HiveUtil.openCompression(spark)

        //dwd  ----->   dws 数据清洗过滤
        DwsDataEtlService.importMember(spark, "20190722") //根据用户信息聚合用户表数据
        DwsDataEtlService.importMemberUseApi(spark, "20190722")

    }
}
