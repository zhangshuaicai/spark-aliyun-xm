package com.atguigu.sparkaliyun.service

import com.atguigu.sparkaliyun.dao.DwsQzDao
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}

object DwsQzEtlService {


    def saveDwsQzChapter(spark: SparkSession, dt: String) = {
        val dwdQzChapter = DwsQzDao.dwdQzchapter(spark, dt)
        val dwdQzChapterlist = DwsQzDao.dwdQzchapter_list(spark, dt)
        val dwdQzPoint = DwsQzDao.dwdQzpoint(spark, dt)
        val dwdQzPointQuestion = DwsQzDao.dwdQzpoint_question(spark, dt)
        val result: DataFrame = dwdQzChapter.join(dwdQzChapterlist, Seq("chapterlistid", "dn"))
            .join(dwdQzPoint, Seq("chapterid", "dn"))
            .join(dwdQzPointQuestion, Seq("pointid", "dn"))
        result.select("chapterid", "chapterlistid", "chaptername", "sequence", "showstatus","showstatus",
            "chapter_creator", "chapter_createtime", "chapter_courseid", "chapternum", "chapterallnum", "outchapterid", "chapterlistname",
               "pointid", "questionid", "questype", "pointname", "pointyear", "chapter", "excisenum", "pointlistid", "pointdescribe","pointlevel",
            "typelist", "point_score", "thought", "remid", "pointnamelist", "typelistids", "pointlist", "dt", "dn")
            .coalesce(1).write.mode(SaveMode.Append).insertInto("dws.dws_qz_chapter")
    }

    def saveDwsQzCourse(sparkSession: SparkSession, dt: String) = {
        val dwdQzSiteCourse = DwsQzDao.dwdQzsite_course(sparkSession, dt)
        val dwdQzCourse = DwsQzDao.dwdQzcourse(sparkSession, dt)
        val dwdQzCourseEdusubject = DwsQzDao.dwdQzcourse_edusubject(sparkSession, dt)
        val result = dwdQzSiteCourse.join(dwdQzCourse, Seq("courseid", "dn"))
            .join(dwdQzCourseEdusubject, Seq("courseid", "dn"))
            .select("sitecourseid", "siteid", "courseid", "sitecoursename", "coursechapter",
                "sequence", "status", "sitecourse_creator", "sitecourse_createtime", "helppaperstatus", "servertype", "boardid",
                "showstatus", "majorid", "coursename", "isadvc", "chapterlistid", "pointlistid", "courseeduid", "edusubjectid"
                , "dt", "dn")
        result.coalesce(1).write.mode(SaveMode.Append).insertInto("dws.dws_qz_course")
    }

    def saveDwsQzMajor(sparkSession: SparkSession, dt: String) = {
        val dwdQzMajor = DwsQzDao.dwdQzmajor(sparkSession, dt)
        val dwdQzWebsite = DwsQzDao.dwdQzwebsite(sparkSession, dt)
        val dwdQzBusiness = DwsQzDao.dwdQzbusiness(sparkSession, dt)
        val result = dwdQzMajor.join(dwdQzWebsite, Seq("siteid", "dn"))
            .join(dwdQzBusiness, Seq("businessid", "dn"))
            .select("majorid", "businessid", "siteid", "majorname", "shortname", "status", "sequence",
                "major_creator", "major_createtime", "businessname", "sitename", "domain", "multicastserver", "templateserver",
                "multicastgateway", "multicastport", "dt", "dn")
        result.coalesce(1).write.mode(SaveMode.Append).insertInto("dws.dws_qz_major")
    }

    def saveDwsQzPaper(sparkSession: SparkSession, dt: String) = {
        val dwdQzPaperView = DwsQzDao.dwdQzpaper_view(sparkSession, dt)
        val dwdQzCenterPaper = DwsQzDao.dwdQzcenter_paper(sparkSession, dt)
        val dwdQzCenter = DwsQzDao.dwdQzcenter(sparkSession, dt)
        val dwdQzPaper = DwsQzDao.dwdQzpaper(sparkSession, dt)
        val result = dwdQzPaperView.join(dwdQzCenterPaper, Seq("paperviewid", "dn"), "left")
            .join(dwdQzCenter, Seq("centerid", "dn"), "left")
            .join(dwdQzPaper, Seq("paperid", "dn"))
            .select("paperviewid", "paperid", "paperviewname", "paperparam", "openstatus", "explainurl", "iscontest"
                , "contesttime", "conteststarttime", "contestendtime", "contesttimelimit", "dayiid", "status", "paper_view_creator",
                "paper_view_createtime", "paperviewcatid", "modifystatus", "description", "paperuse", "paperdifficult", "testreport",
                "paperuseshow", "centerid", "sequence", "centername", "centeryear", "centertype", "provideuser", "centerviewtype",
                "stage", "papercatid", "courseid", "paperyear", "suitnum", "papername", "totalscore", "chapterid", "chapterlistid",
                "dt", "dn")
        result.coalesce(1).write.mode(SaveMode.Append).insertInto("dws.dws_qz_paper")
    }

    def saveDwsQzQuestionTpe(sparkSession: SparkSession, dt: String) = {
        val dwdQzQuestion = DwsQzDao.dwdQzquestion(sparkSession, dt)
        val dwdQzQuestionType = DwsQzDao.dwdQzquestion_type(sparkSession, dt)
        val result = dwdQzQuestion.join(dwdQzQuestionType, Seq("questypeid", "dn"))
            .select("questionid", "parentid", "questypeid", "quesviewtype", "content", "answer", "analysis"
                , "limitminute", "score", "splitscore", "status", "optnum", "lecture", "creator", "createtime", "modifystatus"
                , "attanswer", "questag", "vanalysisaddr", "difficulty", "quesskill", "vdeoaddr", "viewtypename", "papertypename",
                "remark", "splitscoretype", "dt", "dn")
        result.coalesce(1).write.mode(SaveMode.Append).insertInto("dws.dws_qz_question")
    }

    def saveDwsUserPaperDetail(sparkSession: SparkSession, dt: String) = {
        val dwdQzMemberPaperQuestion = DwsQzDao.dwdQzmember_paper_question(sparkSession, dt).drop("paperid")
            .withColumnRenamed("question_answer", "user_question_answer")
        val dwsQzChapter = DwsQzDao.dwdQzchapter(sparkSession, dt).drop("courseid")
        val dwsQzCourse = DwsQzDao.dwdQzcourse(sparkSession, dt).withColumnRenamed("sitecourse_creator", "course_creator")
            .withColumnRenamed("sitecourse_createtime", "course_createtime").drop("majorid")
            .drop("chapterlistid").drop("pointlistid")
        val dwsQzMajor = DwsQzDao.dwdQzmajor(sparkSession, dt)
        val dwsQzPaper = DwsQzDao.dwdQzpaper(sparkSession, dt).drop("courseid")
        val dwsQzQuestion = DwsQzDao.dwdQzquestion(sparkSession, dt)
        dwdQzMemberPaperQuestion.join(dwsQzCourse, Seq("sitecourseid", "dn")).
            join(dwsQzChapter, Seq("chapterid", "dn")).join(dwsQzMajor, Seq("majorid", "dn"))
            .join(dwsQzPaper, Seq("paperviewid", "dn")).join(dwsQzQuestion, Seq("questionid", "dn"))
            .select("userid", "courseid", "questionid", "useranswer", "istrue", "lasttime", "opertype",
                "paperid", "spendtime", "chapterid", "chaptername", "chapternum",
                "chapterallnum", "outchapterid", "chapterlistname", "pointid", "questype", "pointyear", "chapter", "pointname"
                , "excisenum", "pointdescribe", "pointlevel", "typelist", "point_score", "thought", "remid", "pointnamelist",
                "typelistids", "pointlist", "sitecourseid", "siteid", "sitecoursename", "coursechapter", "course_sequence", "course_status"
                , "course_creator", "course_createtime", "servertype", "helppaperstatus", "boardid", "showstatus", "majorid", "coursename",
                "isadvc", "chapterlistid", "pointlistid", "courseeduid", "edusubjectid", "businessid", "majorname", "shortname",
                "major_status", "major_sequence", "major_creator", "major_createtime", "businessname", "sitename",
                "domain", "multicastserver", "templateserver", "multicastgateway", "multicastport", "paperviewid", "paperviewname", "paperparam",
                "openstatus", "explainurl", "iscontest", "contesttime", "conteststarttime", "contestendtime", "contesttimelimit",
                "dayiid", "paper_status", "paper_view_creator", "paper_view_createtime", "paperviewcatid", "modifystatus", "description", "paperuse",
                "testreport", "centerid", "paper_sequence", "centername", "centeryear", "centertype", "provideuser", "centerviewtype",
                "paper_stage", "papercatid", "paperyear", "suitnum", "papername", "totalscore", "question_parentid", "questypeid",
                "quesviewtype", "question_content", "question_answer", "question_analysis", "question_limitminute", "score",
                "splitscore", "lecture", "question_creator", "question_createtime", "question_modifystatus", "question_attanswer",
                "question_questag", "question_vanalysisaddr", "question_difficulty", "quesskill", "vdeoaddr", "question_description",
                "question_splitscoretype", "user_question_answer", "dt", "dn").coalesce(1)
            .write.mode(SaveMode.Append).insertInto("dws.dws_user_paper_detail")
    }

}
