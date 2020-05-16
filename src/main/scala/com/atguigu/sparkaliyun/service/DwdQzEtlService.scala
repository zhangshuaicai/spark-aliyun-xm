package com.atguigu.sparkaliyun.service

import com.alibaba.fastjson.JSONObject
import com.atguigu.sparkaliyun.bean.{DwdQzPaperView, DwdQzPoint, DwdQzQuestion}
import com.atguigu.sparkaliyun.util.ParseJsonData
import org.apache.spark.SparkContext
import org.apache.spark.sql.{SaveMode, SparkSession}

import scala.math.BigDecimal.RoundingMode

object DwdQzEtlService {

    def dwdQz(sc:SparkContext,spark:SparkSession):Unit= {
        import spark.implicits._
        sc.textFile("/user/atguigu/ods/QzBusiness.log")
            .filter(data => {
                val obj: JSONObject = ParseJsonData.getJsonData(data)
                obj.isInstanceOf[JSONObject]
            }).mapPartitions(partitions=>{
                partitions.map(data=>{
                    val obj: JSONObject = ParseJsonData.getJsonData(data)
                    val businessid = obj.getInteger("businessid")
                    val businessname = obj.getString("businessname")
                    val sequence = obj.getString("sequence")
                    val status = obj.getString("status")
                    val creator = obj.getString("creator")
                    val createtime = obj.getString("createtime")
                    val siteid = obj.getInteger("siteid")
                    val dt = obj.getString("dt")
                    val dn = obj.getString("dn")
                    (businessid,businessname,sequence,status,creator,createtime,siteid,dt,dn)
                })
            }).toDF().coalesce(1).write.mode(SaveMode.Append).insertInto("dwd.dwd_qz_business")

        sc.textFile("/user/atguigu/ods/QzCenter.log")
            .filter(data => {
                val obj: JSONObject = ParseJsonData.getJsonData(data)
                obj.isInstanceOf[JSONObject]
            }).mapPartitions(partitions=>{
            partitions.map(data=>{
                val obj: JSONObject = ParseJsonData.getJsonData(data)
                val centerid = obj.getInteger("centerid")
                val centername = obj.getString("centername")
                val centeryear = obj.getString("centeryear")
                val centertype = obj.getString("centertype")
                val openstatus = obj.getString("openstatus")
                val centerparam = obj.getString("centerparam")
                val description = obj.getString("description")
                val creator = obj.getString("creator")
                val createtime = obj.getString("createtime")
                val sequence = obj.getString("sequence")
                val provideuser = obj.getString("provideuser")
                val centerviewtype = obj.getString("centerviewtype")
                val stage = obj.getString("stage")
                val dt = obj.getString("dt")
                val dn = obj.getString("dn")
                (centerid,centername,centeryear,centertype,openstatus,centerparam,description,creator,createtime,sequence,provideuser,centerviewtype,stage,dt,dn)
            })
        }).toDF().coalesce(1).write.mode(SaveMode.Append).insertInto("dwd.dwd_qz_center")

        sc.textFile("/user/atguigu/ods/QzCenterPaper.log")
            .filter(data => {
                val obj: JSONObject = ParseJsonData.getJsonData(data)
                obj.isInstanceOf[JSONObject]
            }).mapPartitions(partitions=>{
            partitions.map(data=>{
                val obj: JSONObject = ParseJsonData.getJsonData(data)
                val paperviewid = obj.getInteger("paperviewid")
                val centerid = obj.getInteger("centerid")
                val openstatus = obj.getString("openstatus")
                val sequence = obj.getString("sequence")
                val creator = obj.getString("creator")
                val createtime = obj.getString("createtime")
                val dt = obj.getString("dt")
                val dn = obj.getString("dn")
                (paperviewid,centerid,openstatus,sequence,creator,createtime,dt,dn)
            })
        }).toDF().coalesce(1).write.mode(SaveMode.Append).insertInto("dwd.dwd_qz_center_paper")

        sc.textFile("/user/atguigu/ods/QzChapter.log")
            .filter(data => {
                val obj: JSONObject = ParseJsonData.getJsonData(data)
                obj.isInstanceOf[JSONObject]
            }).mapPartitions(partitions=>{
            partitions.map(data=>{
                val obj: JSONObject = ParseJsonData.getJsonData(data)
                val chapterid = obj.getInteger("chapterid")
                val chapterlistid = obj.getInteger("chapterlistid")
                val chaptername = obj.getString("chaptername")
                val sequence = obj.getString("sequence")
                val showstatus = obj.getString("showstatus")
                val creator = obj.getString("creator")
                val createtime = obj.getString("createtime")
                val courseid = obj.getInteger("courseid")
                val chapternum = obj.getInteger("chapternum")
                val outchapterid = obj.getInteger("outchapterid")
                val dt = obj.getString("dt")
                val dn = obj.getString("dn")
                (chapterid,chapterlistid,chaptername,sequence,showstatus,creator,createtime,courseid,chapternum,outchapterid,dt,dn)
            })
        }).toDF().coalesce(1).write.mode(SaveMode.Append).insertInto("dwd.dwd_qz_chapter")

        sc.textFile("/user/atguigu/ods/QzChapterList.log")
            .filter(data => {
                val obj: JSONObject = ParseJsonData.getJsonData(data)
                obj.isInstanceOf[JSONObject]
            }).mapPartitions(partitions=>{
            partitions.map(data=>{
                val obj: JSONObject = ParseJsonData.getJsonData(data)
                val chapterlistid = obj.getInteger("chapterlistid")
                val chapterlistname = obj.getString("chapterlistname")
                val courseid = obj.getInteger("courseid")
                val chapterallnum = obj.getInteger("chapterallnum")
                val sequence = obj.getString("sequence")
                val status = obj.getString("status")
                val creator = obj.getString("creator")
                val createtime = obj.getString("createtime")
                val dt = obj.getString("dt")
                val dn = obj.getString("dn")
                (chapterlistid,chapterlistname,courseid,chapterallnum,sequence,status,creator,createtime,dt,dn)
            })
        }).toDF().coalesce(1).write.mode(SaveMode.Append).insertInto("dwd.dwd_qz_chapter_list")

        sc.textFile("/user/atguigu/ods/QzCourse.log")
            .filter(data => {
                val obj: JSONObject = ParseJsonData.getJsonData(data)
                obj.isInstanceOf[JSONObject]
            }).mapPartitions(partitions=>{
            partitions.map(data=>{
                val obj: JSONObject = ParseJsonData.getJsonData(data)
                val courseid = obj.getInteger("courseid")
                val majorid = obj.getInteger("majorid")
                val coursename = obj.getString("coursename")
                val coursechapter = obj.getString("coursechapter")
                val sequence = obj.getString("sequence")
                val isadvc = obj.getString("isadvc")
                val creator = obj.getString("creator")
                val createtime = obj.getString("createtime")
                val status = obj.getString("status")
                val chapterlistid = obj.getInteger("chapterlistid")
                val pointlistid = obj.getInteger("pointlistid")
                val dt = obj.getString("dt")
                val dn = obj.getString("dn")
                (courseid,majorid,coursename,coursechapter,sequence,isadvc,creator,createtime,status,chapterlistid,pointlistid,dt,dn)
            })
        }).toDF().coalesce(1).write.mode(SaveMode.Append).insertInto("dwd.dwd_qz_course")

        sc.textFile("/user/atguigu/ods/QzCourseEduSubject.log")
            .filter(data => {
                val obj: JSONObject = ParseJsonData.getJsonData(data)
                obj.isInstanceOf[JSONObject]
            }).mapPartitions(partitions=>{
            partitions.map(data=>{
                val obj: JSONObject = ParseJsonData.getJsonData(data)
                val courseeduid = obj.getInteger("courseeduid")
                val edusubjectid = obj.getInteger("edusubjectid")
                val courseid = obj.getInteger("courseid")
                val creator = obj.getString("creator")
                val createtime = obj.getString("createtime")
                val majorid = obj.getInteger("majorid")
                val dt = obj.getString("dt")
                val dn = obj.getString("dn")
                (courseeduid,edusubjectid,courseid,creator,createtime,majorid,dt,dn)
            })
        }).toDF().coalesce(1).write.mode(SaveMode.Append).insertInto("dwd.dwd_qz_course_edusubject")

        sc.textFile("/user/atguigu/ods/QzMajor.log")
            .filter(data => {
                val obj: JSONObject = ParseJsonData.getJsonData(data)
                obj.isInstanceOf[JSONObject]
            }).mapPartitions(partitions=>{
            partitions.map(data=>{
                val obj: JSONObject = ParseJsonData.getJsonData(data)
                val majorid = obj.getInteger("majorid")
                val businessid = obj.getInteger("businessid")
                val siteid = obj.getInteger("siteid")
                val majorname = obj.getString("majorname")
                val shortname = obj.getString("shortname")
                val status = obj.getString("status")
                val sequence = obj.getString("sequence")
                val creator = obj.getString("creator")
                val createtime = obj.getString("createtime")
                val column_sitetype = obj.getString("column_sitetype")
                val dt = obj.getString("dt")
                val dn = obj.getString("dn")
                (majorid,businessid,siteid,majorname,shortname,status,sequence,creator,createtime,column_sitetype,dt,dn)
            })
        }).toDF().coalesce(1).write.mode(SaveMode.Append).insertInto("dwd.dwd_qz_major")

        sc.textFile("/user/atguigu/ods/QzMemberPaperQuestion.log")
            .filter(data => {
                val obj: JSONObject = ParseJsonData.getJsonData(data)
                obj.isInstanceOf[JSONObject]
            }).mapPartitions(partitions=>{
            partitions.map(data=>{
                val obj: JSONObject = ParseJsonData.getJsonData(data)
                val userid = obj.getInteger("userid")
                val paperviewid = obj.getInteger("paperviewid")
                val chapterid = obj.getInteger("chapterid")
                val sitecourseid = obj.getInteger("sitecourseid")
                val questionid = obj.getInteger("questionid")
                val majorid = obj.getInteger("majorid")
                val useranswer = obj.getString("useranswer")
                val istrue = obj.getString("istrue")
                val lasttime = obj.getString("lasttime")
                val opertype = obj.getString("opertype")
                val paperid = obj.getInteger("paperid")
                val spendtime = obj.getInteger("spendtime")

                val scoredata = obj.getString("score")
                val score: Double = BigDecimal(scoredata).setScale(1,RoundingMode.HALF_UP).doubleValue()

                val question_answer = obj.getInteger("question_answer")
                val dt = obj.getString("dt")
                val dn = obj.getString("dn")
                (userid,paperviewid,chapterid,sitecourseid,questionid,majorid,useranswer,istrue,lasttime,opertype,paperid,spendtime,score,question_answer,dt,dn)
            })
        }).toDF().coalesce(1).write.mode(SaveMode.Append).insertInto("dwd.dwd_qz_member_paper_question")

        sc.textFile("/user/atguigu/ods/QzPaper.log")
            .filter(data => {
                val obj: JSONObject = ParseJsonData.getJsonData(data)
                obj.isInstanceOf[JSONObject]
            }).mapPartitions(partitions=>{
            partitions.map(data=>{
                val obj: JSONObject = ParseJsonData.getJsonData(data)
                val paperid = obj.getInteger("paperid")
                val papercatid = obj.getInteger("papercatid")
                val courseid = obj.getInteger("courseid")
                val paperyear = obj.getString("paperyear")
                val chapter = obj.getString("chapter")
                val suitnum = obj.getString("suitnum")
                val papername = obj.getString("papername")
                val status = obj.getString("status")
                val creator = obj.getString("creator")
                val createtime = obj.getString("createtime")

                val scoredata = obj.getString("totalscore")
                val totalscore: Double = BigDecimal(scoredata).setScale(1,RoundingMode.HALF_UP).doubleValue()

                val chapterid = obj.getInteger("chapterid")
                val chapterlistid = obj.getInteger("chapterlistid")
                val dt = obj.getString("dt")
                val dn = obj.getString("dn")
                (paperid,papercatid,courseid,paperyear,chapter,suitnum,papername,status,creator,createtime,totalscore,chapterid,chapterlistid,dt,dn)
            })
        }).toDF().coalesce(1).write.mode(SaveMode.Append).insertInto("dwd.dwd_qz_paper")

        sc.textFile("/user/atguigu/ods/QzPaperView.log")
            .filter(data => {
                val obj: JSONObject = ParseJsonData.getJsonData(data)
                obj.isInstanceOf[JSONObject]
            }).mapPartitions(partitions=>{
            partitions.map(data=>{
                val obj: JSONObject = ParseJsonData.getJsonData(data)
                val paperviewid = obj.getInteger("paperviewid")
                val paperid = obj.getInteger("paperid")
                val paperviewname = obj.getString("paperviewname")
                val paperparam = obj.getString("paperparam")
                val openstatus = obj.getString("openstatus")
                val explainurl = obj.getString("explainurl")
                val iscontest = obj.getString("iscontest")
                val contesttime = obj.getString("contesttime")
                val conteststarttime = obj.getString("conteststarttime")
                val contestendtime = obj.getString("contestendtime")
                val contesttimelimit = obj.getString("contesttimelimit")
                val dayiid = obj.getInteger("dayiid")
                val status = obj.getString("status")
                val creator = obj.getString("creator")
                val createtime = obj.getString("createtime")
                val paperviewcatid = obj.getInteger("paperviewcatid")
                val modifystatus = obj.getString("modifystatus")
                val description = obj.getString("description")
                val papertype = obj.getString("papertype")
                val downurl = obj.getString("downurl")
                val paperuse = obj.getString("paperuse")
                val paperdifficult = obj.getString("paperdifficult")
                val testreport = obj.getString("testreport")
                val paperuseshow = obj.getString("paperuseshow")
                val dt = obj.getString("dt")
                val dn = obj.getString("dn")
                DwdQzPaperView(paperviewid,paperid,paperviewname,paperparam,openstatus,explainurl,iscontest,contesttime,
                    conteststarttime,contestendtime,contesttimelimit,dayiid,status,creator,createtime,paperviewcatid,
                    modifystatus,description,papertype,downurl,paperuse,paperdifficult,testreport,paperuseshow,dt,dn)
            })
        }).toDF().coalesce(1).write.mode(SaveMode.Append).insertInto("dwd.dwd_qz_paper_view")

        sc.textFile("/user/atguigu/ods/QzPoint.log")
            .filter(data => {
                val obj: JSONObject = ParseJsonData.getJsonData(data)
                obj.isInstanceOf[JSONObject]
            }).mapPartitions(partitions=>{
            partitions.map(data=>{
                val obj: JSONObject = ParseJsonData.getJsonData(data)
                val pointid = obj.getInteger("pointid")
                val courseid = obj.getInteger("courseid")
                val pointname = obj.getString("pointname")
                val pointyear = obj.getString("pointyear")
                val chapter = obj.getString("chapter")
                val creator = obj.getString("creator")
                val createtime = obj.getString("createtime")
                val status = obj.getString("status")
                val modifystatus = obj.getString("modifystatus")
                val excisenum = obj.getString("excisenum")
                val pointlistid = obj.getInteger("pointlistid")
                val chapterid = obj.getInteger("chapterid")
                val sequece = obj.getString("sequece")
                val pointdescribe = obj.getString("pointdescribe")
                val pointlevel = obj.getString("pointlevel")
                val typelist = obj.getString("typelist")
                val scoredata = obj.getString("score")
                val score: Double = BigDecimal(scoredata).setScale(1,RoundingMode.HALF_UP).doubleValue()
                val thought = obj.getString("thought")
                val remid = obj.getString("remid")
                val pointnamelist = obj.getString("pointnamelist")
                val typelistids = obj.getString("typelistids")
                val pointlist = obj.getString("pointlist")
                val dt = obj.getString("dt")
                val dn = obj.getString("dn")
                DwdQzPoint(pointid,courseid,pointname,pointyear,chapter,creator,createtime,status,modifystatus,excisenum,pointlistid,chapterid,sequece,pointdescribe,pointlevel,typelist,score,thought,remid,pointnamelist,typelistids,pointlist,dt,dn)
            })
        }).toDF().coalesce(1).write.mode(SaveMode.Append).insertInto("dwd.dwd_qz_point")

        sc.textFile("/user/atguigu/ods/QzPointQuestion.log")
            .filter(data => {
                val obj: JSONObject = ParseJsonData.getJsonData(data)
                obj.isInstanceOf[JSONObject]
            }).mapPartitions(partitions=>{
            partitions.map(data=>{
                val obj: JSONObject = ParseJsonData.getJsonData(data)
                val pointid = obj.getInteger("pointid")
                val courseid = obj.getInteger("courseid")
                val pointname = obj.getString("pointname")
                val pointyear = obj.getString("pointyear")
                val chapter = obj.getString("chapter")
                val creator = obj.getString("creator")
                val createtime = obj.getString("createtime")
                val status = obj.getString("status")
                val modifystatus = obj.getString("modifystatus")
                val excisenum = obj.getString("excisenum")
                val pointlistid = obj.getInteger("pointlistid")
                val chapterid = obj.getInteger("chapterid")
                val sequece = obj.getString("sequece")
                val pointdescribe = obj.getString("pointdescribe")
                val pointlevel = obj.getString("pointlevel")
                val typelist = obj.getString("typelist")
                val scoredata = obj.getString("score")
                val score: Double = BigDecimal(scoredata).setScale(1,RoundingMode.HALF_UP).doubleValue()
                val thought = obj.getString("thought")
                val remid = obj.getString("remid")
                val pointnamelist = obj.getString("pointnamelist")
                val typelistids = obj.getString("typelistids")
                val pointlist = obj.getString("pointlist")
                val dt = obj.getString("dt")
                val dn = obj.getString("dn")
                DwdQzPoint(pointid,courseid,pointname,pointyear,chapter,creator,createtime,status,modifystatus,excisenum,
                    pointlistid,chapterid,sequece,pointdescribe,pointlevel,typelist,score,thought,remid,pointnamelist,typelistids,pointlist,dt,dn)
            })
        }).toDF().coalesce(1).write.mode(SaveMode.Append).insertInto("dwd.dwd_qz_point_question")

        sc.textFile("/user/atguigu/ods/QzQuestion.log")
            .filter(data => {
                val obj: JSONObject = ParseJsonData.getJsonData(data)
                obj.isInstanceOf[JSONObject]
            }).mapPartitions(partitions=>{
            partitions.map(data=>{
                val obj: JSONObject = ParseJsonData.getJsonData(data)
                val questionid = obj.getInteger("questionid")
                val parentid = obj.getInteger("parentid")
                val questypeid = obj.getInteger("questypeid")
                val quesviewtype = obj.getInteger("quesviewtype")
                val content = obj.getString("content")
                val answer = obj.getString("answer")
                val analysis = obj.getString("analysis")
                val limitminute = obj.getString("limitminute")
                val scoredata = obj.getString("score")
                val score: Double = BigDecimal(scoredata).setScale(1,RoundingMode.HALF_UP).doubleValue()
                val splitscoredata = obj.getString("splitscore")
                val splitscore: Double = BigDecimal(splitscoredata).setScale(1,RoundingMode.HALF_UP).doubleValue()
                val status = obj.getString("status")
                val optnum = obj.getInteger("optnum")
                val lecture = obj.getString("lecture")
                val creator = obj.getString("creator")
                val createtime = obj.getString("createtime")
                val modifystatus = obj.getString("modifystatus")
                val attanswer = obj.getString("attanswer")
                val questag = obj.getString("questag")
                val vanalysisaddr = obj.getString("vanalysisaddr")
                val difficulty = obj.getString("difficulty")
                val quesskill = obj.getString("quesskill")
                val vdeoaddr = obj.getString("vdeoaddr")
                val dt = obj.getString("dt")
                val dn = obj.getString("dn")
                DwdQzQuestion(questionid,parentid,questypeid,quesviewtype,content,answer,analysis,limitminute,score,splitscore,
                    status,optnum,lecture,creator,createtime,modifystatus,attanswer,questag,vanalysisaddr,difficulty,quesskill,vdeoaddr,dt,dn)
            })
        }).toDF().coalesce(1).write.mode(SaveMode.Append).insertInto("dwd.dwd_qz_question")

        sc.textFile("/user/atguigu/ods/QzQuestionType.log")
            .filter(data => {
                val obj: JSONObject = ParseJsonData.getJsonData(data)
                obj.isInstanceOf[JSONObject]
            }).mapPartitions(partitions=>{
            partitions.map(data=>{
                val obj: JSONObject = ParseJsonData.getJsonData(data)
                val quesviewtype = obj.getInteger("quesviewtype")
                val viewtypename = obj.getString("viewtypename")
                val questypeid = obj.getInteger("questypeid")
                val description = obj.getString("description")
                val status = obj.getString("status")
                val creator = obj.getString("creator")
                val createtime = obj.getString("createtime")
                val papertypename = obj.getString("papertypename")
                val sequence = obj.getString("sequence")
                val remark = obj.getString("remark")
                val splitscoretype = obj.getString("splitscoretype")
                val dt = obj.getString("dt")
                val dn = obj.getString("dn")
                (quesviewtype,viewtypename,questypeid,description,status,creator,createtime,papertypename,sequence,remark,
                    splitscoretype,dt,dn)
            })
        }).toDF().coalesce(1).write.mode(SaveMode.Append).insertInto("dwd.dwd_qz_question_type")

        sc.textFile("/user/atguigu/ods/QzSiteCourse.log")
            .filter(data => {
                val obj: JSONObject = ParseJsonData.getJsonData(data)
                obj.isInstanceOf[JSONObject]
            }).mapPartitions(partitions=>{
            partitions.map(data=>{
                val obj: JSONObject = ParseJsonData.getJsonData(data)
                val sitecourseid = obj.getInteger("sitecourseid")
                val siteid = obj.getInteger("siteid")
                val courseid = obj.getInteger("courseid")
                val sitecoursename = obj.getString("sitecoursename")
                val coursechapter = obj.getString("coursechapter")
                val sequence = obj.getString("sequence")
                val status = obj.getString("status")
                val creator = obj.getString("creator")
                val createtime = obj.getString("createtime")
                val helppaperstatus = obj.getString("helppaperstatus")
                val servertype = obj.getString("servertype")
                val boardid = obj.getInteger("boardid")
                val showstatus = obj.getString("showstatus")
                val dt = obj.getString("dt")
                val dn = obj.getString("dn")
                (sitecourseid,siteid,courseid,sitecoursename,coursechapter,sequence,status,creator,createtime,helppaperstatus,
                    servertype,boardid,showstatus,dt,dn)
            })
        }).toDF().coalesce(1).write.mode(SaveMode.Append).insertInto("dwd.dwd_qz_site_course")

        sc.textFile("/user/atguigu/ods/QzWebsite.log")
            .filter(data => {
                val obj: JSONObject = ParseJsonData.getJsonData(data)
                obj.isInstanceOf[JSONObject]
            }).mapPartitions(partitions=>{
            partitions.map(data=>{
                val obj: JSONObject = ParseJsonData.getJsonData(data)
                val siteid = obj.getInteger("siteid")
                val sitename = obj.getString("sitename")
                val domain = obj.getString("domain")
                val sequence = obj.getString("sequence")
                val multicastserver = obj.getString("multicastserver")
                val templateserver = obj.getString("templateserver")
                val status = obj.getString("status")
                val creator = obj.getString("creator")
                val createtime = obj.getString("createtime")
                val multicastgateway = obj.getString("multicastgateway")
                val multicastport = obj.getString("multicastport")
                val dt = obj.getString("dt")
                val dn = obj.getString("dn")
                (siteid,sitename,domain,sequence,multicastserver,templateserver,status,creator,createtime,multicastgateway,
                    multicastport,dt,dn)
            })
        }).toDF().coalesce(1).write.mode(SaveMode.Append).insertInto("dwd.dwd_qz_website")

    }




}
/*





       */