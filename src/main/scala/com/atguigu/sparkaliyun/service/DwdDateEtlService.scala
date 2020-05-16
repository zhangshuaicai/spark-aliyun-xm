package com.atguigu.sparkaliyun.service

import com.alibaba.fastjson.JSONObject
import com.atguigu.sparkaliyun.bean.Member
import com.atguigu.sparkaliyun.util.ParseJsonData
import org.apache.commons.lang3.StringUtils
import org.apache.spark.SparkContext
import org.apache.spark.sql.{SaveMode, SparkSession}

object DwdDateEtlService {
    /**
     * 将广告表进行数据清洗
     *
     * @param sc
     * @param spark
     */
    def dwdBaseAd(sc: SparkContext, spark: SparkSession): Unit = {
        import spark.implicits._
        sc.textFile("/user/atguigu/ods/baseadlog.log")
            .filter {
                data => {
                    val obj: JSONObject = ParseJsonData.getJsonData(data)
                    obj.isInstanceOf[JSONObject]
                }
            }.mapPartitions {
            partitions => {
                partitions.map {
                    data => {
                        val obj: JSONObject = ParseJsonData.getJsonData(data)
                        val adid = obj.getInteger("adid")
                        val adname = obj.getString("adname")
                        val dn = obj.getString("dn")
                        (adid, adname, dn)
                    }
                }
            }
        }.toDF().coalesce(1).write.mode(SaveMode.Overwrite).insertInto("dwd.dwd_base_ad")
    }

    /**
     * 网站访问数据导入dwd层
     *
     * @param sc
     * @param spark
     */
    def dwdBaseWebsite(sc: SparkContext, spark: SparkSession): Unit = {
        import spark.implicits._
        sc.textFile("/user/atguigu/ods/baswewebsite.log")
            .filter {
                data => {
                    val obj: JSONObject = ParseJsonData.getJsonData(data)
                    obj.isInstanceOf[JSONObject]
                }
            }.mapPartitions {
            partitions => {
                partitions.map {
                    data => {
                        val obj: JSONObject = ParseJsonData.getJsonData(data)
                        val siteid: Int = obj.getInteger("siteid")
                        val sitename: String = obj.getString("sitename")
                        val siteurl: String = obj.getString("siteurl")
                        val delete: Int = obj.getInteger("delete")
                        val createtime: String = obj.getString("createtime")
                        val creator: String = obj.getString("creator")
                        val dn: String = obj.getString("dn")
                        (siteid, sitename, siteurl, delete, createtime, creator, dn)
                    }
                }
            }
        }.toDF().coalesce(1).write.mode(SaveMode.Overwrite).insertInto("dwd.dwd_base_website")
    }

    /**
     * 用户信息
     *
     * @param sc
     * @param spark
     */
    def dwdMember(sc: SparkContext, spark: SparkSession): Unit = {
        import spark.implicits._
        sc.textFile("/user/atguigu/ods/member.log")
            .filter {
                data => {
                    val obj: JSONObject = ParseJsonData.getJsonData(data)
                    obj.isInstanceOf[JSONObject]
                }
            }.mapPartitions {
            partitions => {
                partitions.map {
                    data => {
                        val obj: JSONObject = ParseJsonData.getJsonData(data)
                        val uid: Int = obj.getInteger("uid")
                        val ad_id: Int = obj.getInteger("ad_id")
                        val birthday: String = obj.getString("birthday")
                        val email: String = obj.getString("email")
                        //姓名脱敏
                        val fullname: String = obj.getString("fullname")
                        val name = StringUtils.left(fullname,1)
                        val resultName = StringUtils.rightPad(name,StringUtils.length(fullname),"*")
                        val iconurl: String = obj.getString("iconurl")
                        val lastlogin: String = obj.getString("lastlogin")
                        val mailaddr: String = obj.getString("mailaddr")
                        val memberlevel: String = obj.getString("memberlevel")

                        //密码脱敏
                        //val password: String = obj.getString("password")
                        val password: String = "******"
                        val paymoney: String = obj.getString("paymoney")

                        //电话脱敏
                        val phone: String = obj.getString("phone")
                        var phonetm = phone.substring(0, 3) + "*****" + phone.substring(7,11)

                        val qq: String = obj.getString("qq")
                        val register: String = obj.getString("register")
                        val regupdatetime: String = obj.getString("regupdatetime")
                        val unitname: String = obj.getString("unitname")
                        val userip: String = obj.getString("userip")
                        val zipcode: String = obj.getString("zipcode")
                        val dt: String = obj.getString("dt")
                        val dn: String = obj.getString("dn")
                        Member(uid,ad_id,birthday,email,resultName,iconurl,lastlogin,mailaddr,memberlevel,password,
                            paymoney,phonetm,qq,register,regupdatetime,unitname,userip,zipcode,dt,dn)
                    }
                }
            }
        }.toDF().coalesce(2).write.mode(SaveMode.Append).insertInto("dwd.dwd_member")
    }


    /**
     *
     * @param sc
     * @param spark
     */
    def dwdMemberRegtype(sc: SparkContext, spark: SparkSession): Unit = {
        import spark.implicits._
        sc.textFile("/user/atguigu/ods/memberRegtype.log")
            .filter{
                data => {
                    val obj: JSONObject = ParseJsonData.getJsonData(data)
                    obj.isInstanceOf[JSONObject]
                }
            }.mapPartitions{
                partitions => {
                    partitions.map {
                        data => {
                            val obj: JSONObject = ParseJsonData.getJsonData(data)
                            val uid = obj.getInteger("uid")
                            val appkey = obj.getString("appkey")
                            val appregurl = obj.getString("appregurl")
                            val bdp_uuid = obj.getString("bdp_uuid")
                            val createtime = obj.getString("createtime")
                            val isranreg = obj.getString("isranreg")
                            val regsource = obj.getString("regsource")
                            val regsourcename = obj.getString("regsourcename")
                            val websiteid = obj.getInteger("websiteid")
                            val dt = obj.getString("dt")
                            val dn = obj.getString("dn")
                            (uid,appkey,appregurl,bdp_uuid,createtime,isranreg,regsource,regsourcename,websiteid,dt,dn)
                        }
                    }
                }
            }.toDF().coalesce(1).write.mode(SaveMode.Append).insertInto("dwd.dwd_member_regtype")
    }

    /**
     * 用户支付信息
     * @param sc
     * @param spark
     */
    def dwdPcenterMemPayMoney(sc: SparkContext, spark: SparkSession): Unit = {
        import spark.implicits._
        sc.textFile("/user/atguigu/ods/pcentermempaymoney.log")
            .filter {
                data => {
                    val obj: JSONObject = ParseJsonData.getJsonData(data)
                    obj.isInstanceOf[JSONObject]
                }
            }.mapPartitions {
                partitions => {
                    partitions.map {
                        data => {
                            val obj: JSONObject = ParseJsonData.getJsonData(data)
                            val uid = obj.getInteger("uid")
                            val paymoney = obj.getString("paymoney")
                            val siteid = obj.getInteger("siteid")
                            val vip_id = obj.getInteger("vip_id")
                            val dt = obj.getString("dt")
                            val dn = obj.getString("dn")
                            (uid,paymoney,siteid,vip_id,dt,dn)
                        }
                    }
                }
            }.toDF().coalesce(1).write.mode(SaveMode.Overwrite).insertInto("dwd.dwd_pcentermempaymoney")
    }

    /**
     * 用户VIP信息
     * @param sc
     * @param spark
     */
    def dwdVipLevel(sc: SparkContext, spark: SparkSession): Unit = {
        import spark.implicits._
        sc.textFile("/user/atguigu/ods/pcenterMemViplevel.log")
            .filter {
                data => {
                    val obj: JSONObject = ParseJsonData.getJsonData(data)
                    obj.isInstanceOf[JSONObject]
                }
            }.mapPartitions {
            partitions => {
                partitions.map {
                    data => {
                        val obj: JSONObject = ParseJsonData.getJsonData(data)
                        val vip_id = obj.getInteger("vip_id")
                        val vip_level = obj.getString("vip_level")
                        val start_time = obj.getString("start_time")
                        val end_time = obj.getString("end_time")
                        val last_modify_time = obj.getString("last_modify_time")
                        val max_free = obj.getString("max_free")
                        val min_free = obj.getString("min_free")
                        val next_level = obj.getString("next_level")
                        val operator = obj.getString("operator")
                        val dn = obj.getString("dn")
                        (vip_id,vip_level,start_time,end_time,last_modify_time,max_free,min_free,next_level,operator,dn)
                    }
                }
            }
        }.toDF().coalesce(1).write.mode(SaveMode.Overwrite).insertInto("dwd.dwd_vip_level")
    }
}
