package com.atguigu.sparkaliyun.controllor

import com.atguigu.sparkaliyun.service.DwdDateEtlService
import com.atguigu.sparkaliyun.util.HiveUtil
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SparkSession

object DwdDateEtlControllor {
    def main(args: Array[String]): Unit = {
        val conf: SparkConf = new SparkConf().setAppName("DwdDateEtl")

        val spark: SparkSession = SparkSession.builder().config(conf).enableHiveSupport().getOrCreate()

        val sc: SparkContext = spark.sparkContext

        sc.hadoopConfiguration.set("fs.defaultFS","hdfs://nameservice1")
        sc.hadoopConfiguration.set("dfs.nameservices","nameservice1  ")

        HiveUtil.setMaxpartitions(spark)
        HiveUtil.openCompression(spark)

        //ods  ----->   dwd 数据清洗过滤
        DwdDateEtlService.dwdBaseAd(sc,spark)
        DwdDateEtlService.dwdBaseWebsite(sc,spark)
        DwdDateEtlService.dwdMember(sc,spark)
        DwdDateEtlService.dwdMemberRegtype(sc,spark)
        DwdDateEtlService.dwdPcenterMemPayMoney(sc,spark)
        DwdDateEtlService.dwdVipLevel(sc,spark)

    }
}
