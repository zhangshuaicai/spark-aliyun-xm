package com.atguigu.sparkaliyun.dao

import org.apache.spark.sql.SparkSession

object DwsQzDao {
    def dwdQzbusiness(spark:SparkSession,dt:String)={
        spark.sql(s"select businessid,businessname,dn from dwd.dwd_qz_business where dt='$dt'")
    }
    def dwdQzcenter(spark:SparkSession,dt:String)={
        spark.sql("select centerid,centername,centeryear,centertype,centerparam,provideuser," +
            s"centerviewtype,stage,dn from dwd.dwd_qz_center where dt='$dt'")
    }
    def dwdQzcenter_paper(spark:SparkSession,dt:String)={
        spark.sql(s"select paperviewid,sequence,centerid,dn from dwd.dwd_qz_center_paper where dt='$dt'")
    }
    def dwdQzchapter(spark:SparkSession,dt:String)={
        spark.sql("select chapterid,chapterlistid,chaptername,sequence,showstatus,creator as " +
            "chapter_creator,createtime as chapter_createtime,courseid as chapter_courseid,chapternum,outchapterid,dt,dn from dwd.dwd_qz_chapter where " +
            s"dt='$dt'")
    }
    def dwdQzchapter_list(spark:SparkSession,dt:String)={
        spark.sql("select chapterlistid,chapterlistname,chapterallnum,dn from dwd.dwd_qz_chapter_list " +
            s"where dt='$dt'")
    }
    def dwdQzcourse(spark:SparkSession,dt:String)={
        spark.sql("select courseid,majorid,coursename,isadvc,chapterlistid,pointlistid,dn from " +
            s"dwd.dwd_qz_course where dt='${dt}'")
    }
    def dwdQzcourse_edusubject(spark:SparkSession,dt:String)={
        spark.sql("select courseeduid,edusubjectid,courseid,dn from dwd.dwd_qz_course_edusubject " +
            s"where dt='${dt}'")
    }
    def dwdQzmajor(spark:SparkSession,dt:String)={
        spark.sql("select majorid,businessid,siteid,majorname,shortname,status,sequence,creator as major_creator," +
            s"createtime as major_createtime,dt,dn from dwd.dwd_qz_major where dt='$dt'")
    }
    def dwdQzmember_paper_question(spark:SparkSession,dt:String)={
        spark.sql("select userid,paperviewid,chapterid,sitecourseid,questionid,majorid,useranswer,istrue,lasttime,opertype," +
            s"paperid,spendtime,score,question_answer,dt,dn from dwd.dwd_qz_member_paper_question where dt='$dt'")
    }
    def dwdQzpaper(spark:SparkSession,dt:String)={
        spark.sql("select paperid,papercatid,courseid,paperyear,chapter,suitnum,papername,totalscore,chapterid," +
            s"chapterlistid,dn from dwd.dwd_qz_paper where dt='$dt'")
    }
    def dwdQzpaper_view(spark:SparkSession,dt:String)={
        spark.sql("select paperviewid,paperid,paperviewname,paperparam,openstatus,explainurl,iscontest," +
            "contesttime,conteststarttime,contestendtime,contesttimelimit,dayiid,status,creator as paper_view_creator," +
            "createtime as paper_view_createtime,paperviewcatid,modifystatus,description,papertype,downurl,paperuse," +
            s"paperdifficult,testreport,paperuseshow,dt,dn from dwd.dwd_qz_paper_view where dt='$dt'")
    }
    def dwdQzpoint(spark:SparkSession,dt:String)={
        spark.sql("select pointid,pointname,pointyear,chapter,excisenum,pointlistid,chapterid," +
            "pointdescribe,pointlevel,typelist,score as point_score,thought,remid,pointnamelist,typelistids,pointlist,dn from " +
            s"dwd.dwd_qz_point where dt='$dt'")
    }
    def dwdQzpoint_question(spark:SparkSession,dt:String)={
        spark.sql(s"select pointid,questionid,questype,dn from dwd.dwd_qz_point_question where dt='$dt'")
    }
    def dwdQzquestion(spark:SparkSession,dt:String)={
        spark.sql("select questionid,parentid,questypeid,quesviewtype,content,answer,analysis,limitminute," +
            "score,splitscore,status,optnum,lecture,creator,createtime,modifystatus,attanswer,questag,vanalysisaddr,difficulty," +
            s"quesskill,vdeoaddr,dt,dn from  dwd.dwd_qz_question where dt='$dt'")
    }
    def dwdQzquestion_type(spark:SparkSession,dt:String)={
        spark.sql("select questypeid,viewtypename,description,papertypename,remark,splitscoretype,dn from " +
            s"dwd.dwd_qz_question_type where dt='$dt'")
    }
    def dwdQzsite_course(spark:SparkSession,dt:String)={
        spark.sql("select sitecourseid,siteid,courseid,sitecoursename,coursechapter,sequence,status," +
            "creator as sitecourse_creator,createtime as sitecourse_createtime,helppaperstatus,servertype,boardid,showstatus,dt,dn " +
            s"from dwd.dwd_qz_site_course where dt='${dt}'")
    }
    def dwdQzwebsite(spark:SparkSession,dt:String)={
        spark.sql("select siteid,sitename,domain,multicastserver,templateserver,creator," +
            s"createtime,multicastgateway,multicastport,dn from dwd.dwd_qz_website where dt='$dt'")
    }
    /**
     * 统计各试卷平均耗时 平均分
     *
     * @param sparkSession
     * @param dt
     * @return
     */
    def getAvgSPendTimeAndScore(sparkSession: SparkSession, dt: String) = {
        sparkSession.sql(s"select paperviewid,paperviewname,cast(avg(score) as decimal(4,1)) score,cast(avg(spendtime) as decimal(10,2))" +
            s" spendtime,dt,dn from dws.dws_user_paper_detail where dt='$dt' group by " +
            "paperviewid,paperviewname,dt,dn order by score desc,spendtime desc");
    }

    /**
     * 统计试卷 最高分 最低分
     *
     * @param sparkSession
     * @param dt
     */
    def getTopScore(sparkSession: SparkSession, dt: String) = {
        sparkSession.sql("select paperviewid,paperviewname,cast(max(score) as decimal(4,1)),cast(min(score) as decimal(4,1)) " +
            s",dt,dn from dws.dws_user_paper_detail where dt=$dt group by paperviewid,paperviewname,dt,dn ")
    }

    /**
     * 按试卷分组获取每份试卷的分数前三用户详情
     *
     * @param sparkSession
     * @param dt
     */
    def getTop3UserDetail(sparkSession: SparkSession, dt: String) = {
        sparkSession.sql("select *from (select userid,paperviewname,chaptername,pointname,sitecoursename,coursename,majorname,shortname," +
            "sitename,papername,score,dense_rank() over (partition by paperviewid order by score desc) as rk,dt,dn from dws.dws_user_paper_detail) " +
            "where rk<4")
    }

    /**
     * 按试卷分组获取每份试卷的分数倒数三的用户详情
     *
     * @param sparkSession
     * @param dt
     * @return
     */
    def getLow3UserDetail(sparkSession: SparkSession, dt: String) = {
        sparkSession.sql("select *from (select userid,paperviewname,chaptername,pointname,sitecoursename,coursename,majorname,shortname," +
            s"sitename,papername,score,dense_rank() over (partition by paperviewid order by score asc) as rk,dt,dn from dws.dws_user_paper_detail where dt='$dt') where rk<4")
    }


    /**
     * 统计各试卷 各分段学员名称
     */
    def getPaperScoreSegmentUser(sparkSession: SparkSession, dt: String) = {
        sparkSession.sql("select paperviewid,paperviewname,score_segment,concat_ws(',',collect_list(cast(userid as string))),dt,dn" +
            " from (select paperviewid,paperviewname,userid," +
            " case  when score >=0  and score <=20 then '0-20'" +
            "       when score >20 and score <=40 then '20-40' " +
            "       when score >40 and score <=60 then '40-60' " +
            "       when score >60 and score <=80 then '60-80' " +
            "       when score >80 and score <=100 then '80-100' end  as score_segment" +
            s",dt,dn from  dws.dws_user_paper_detail where dt='$dt') group by paperviewid,paperviewname,score_segment,dt,dn order by paperviewid,score_segment")
    }

    /**
     * 统计各试卷未及格人数 及格人数 及格率
     *
     * @param sparkSession
     * @param dt
     */
    def getPaperPassDetail(sparkSession: SparkSession, dt: String) = {
        sparkSession.sql("select t.*,cast(t.passcount/(t.passcount+t.countdetail) as decimal(4,2)) as rate,dt,dn" +
            "   from(select a.paperviewid,a.paperviewname,a.countdetail,a.dt,a.dn,b.passcount from " +
            s"(select paperviewid,paperviewname,count(*) countdetail,dt,dn from dws.dws_user_paper_detail where dt='$dt' and score between 0 and 60 group by" +
            s" paperviewid,paperviewname,dt,dn) a join (select paperviewid,count(*) passcount,dn from  dws.dws_user_paper_detail  where dt='$dt' and score >60  " +
            "group by paperviewid,dn) b on a.paperviewid=b.paperviewid and a.dn=b.dn)t")

    }

    /**
     * 统计各题 正确人数 错误人数 错题率 top3错误题数多的questionid
     *
     * @param sparkSession
     * @param dt
     */
    def getQuestionDetail(sparkSession: SparkSession, dt: String) = {
        sparkSession.sql(s"select t.*,cast(t.errcount/(t.errcount+t.rightcount) as decimal(4,2))as rate" +
            s" from((select questionid,count(*) errcount,dt,dn from dws.dws_user_paper_detail where dt='$dt' and user_question_answer='0' " +
            s"group by questionid,dt,dn) a join(select questionid,count(*) rightcount,dt,dn from dws.dws_user_paper_detail where dt='$dt' and user_question_answer='1' " +
            s"group by questionid,dt,dn) b on a.questionid=b.questionid and a.dn=b.dn)t order by errcount desc")
    }
}
