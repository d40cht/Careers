package controllers

import java.sql.{Timestamp, Blob}

import org.scalaquery.session._
import org.scalaquery.ql.basic.{BasicTable => Table}
import org.scalaquery.session.Database.threadLocalSession
import org.scalaquery.ql.extended.H2Driver.Implicit._
import org.scalaquery.ql._
import org.scalaquery.ql.TypeMapper._

// To interrogate the H2 DB: java -jar ../../../play-1.2.2RC2/framework/lib/h2-1.3.149.jar
package models
{
    object Users extends Table[(Long, Timestamp, String, String, String, Boolean)]("Users")
    {
        def id          = column[Long]("id")
        def added       = column[Timestamp]("added")
        def email       = column[String]("email")
        def password    = column[String]("password")
        def fullName    = column[String]("fullName")
        def isAdmin     = column[Boolean]("isAdmin")
        
        def insertIdSeq = Sequence[Long]("seq_Users_id")
        
        def * = id ~ added ~ email ~ password ~ fullName ~ isAdmin
    }
    
    object CVs extends Table[(Long, Timestamp, String, Long, Blob, Blob, Blob)]("CVs")
    {
        def id              = column[Long]("id")
        def added           = column[Timestamp]("added")
        def description     = column[String]("description")
        def userId          = column[Long]("userId")
        def pdf             = column[Blob]("pdf")
        def text            = column[Blob]("text")
        def documentDigest  = column[Blob]("documentDigest")
        
        def insertIdSeq     = Sequence[Long]("seq_CVs_id")
        
        def * = id ~ added ~ description ~ userId ~ pdf ~ text ~ documentDigest
    }
    
    object MatchVectors extends Table[(Long, Long, Blob)]("MatchVector")
    {
        def id              = column[Long]("id")
        def cvId            = column[Long]("cvId")
        def topicVector     = column[Blob]("topicVector")
        
        def insertIdSeq     = Sequence[Long]("seq_MatchVector_id")
        
        def * = id ~ cvId ~ topicVector
    }
    
    object Matches extends Table[(Long, Long, Long, Double, Blob, java.sql.Timestamp)]("Matches")
    {
        def id              = column[Long]("id")
        def fromMatchId     = column[Long]("fromMatchId")
        def toMatchId       = column[Long]("toMatchId")
        def similarity      = column[Double]("similarity")
        def matchVector     = column[Blob]("matchVector")
        def added           = column[java.sql.Timestamp]("added")
        
        def insertIdSeq     = Sequence[Long]("seq_Matches_id")
        
        def * = id ~ fromMatchId ~ toMatchId ~ similarity ~ matchVector ~ added
    }
    
    object Companies extends Table[(Long, String, String, String, String, String)]("Companies")
    {
        def id              = column[Long]("id")
        def name            = column[String]("name")
        def url             = column[String]("url")
        def description     = column[String]("description")
        def nameMatch1      = column[String]("nameMatch1")
        def nameMatch2      = column[String]("nameMatch2")
        
        def insertIdSeq     = Sequence[Long]("seq_Companies_id")
        
        def * = id ~ name ~ url ~ description ~ nameMatch1 ~ nameMatch2
    }
    
    object Position extends Table[(Long, Long, Long, String, String, Int, Int, Int, String, Double, Double, Long)]("Position")
    {
        def id              = column[Long]("id")
        def userId          = column[Long]("userId")
        def companyId       = column[Long]("companyId")
        def department      = column[String]("department")
        def jobTitle        = column[String]("jobTitle")
        def yearsExperience = column[Int]("yearsExperience")
        def startYear       = column[Int]("startYear")
        def endYear         = column[Int]("endYear")
        def address         = column[String]("address")
        def longitude       = column[Double]("longitude")
        def latitude        = column[Double]("latitude")
        def matchVectorId   = column[Long]("matchVectorId")
        
        def insertIdSeq     = Sequence[Long]("seq_Positions_id")
        
        def * = id ~ userId ~ companyId ~ department ~ jobTitle ~ yearsExperience ~ startYear ~ endYear ~ address ~ longitude ~ latitude ~ matchVectorId
    }
    
    object Searches extends Table[(Long, Long, String, String, Double, Double, Double, Long)]("Searches")
    {
        def id              = column[Long]("id")
        def userId          = column[Long]("userId")
        def description     = column[String]("description")
        def address         = column[String]("address")
        def longitude       = column[Double]("longitude")
        def latitude        = column[Double]("latitude")
        def radius          = column[Double]("radius")
        def matchVectorId   = column[Long]("matchVectorId")
        
        def insertIdSeq     = Sequence[Long]("seq_Searches_id")
        
        def * = id ~ userId ~ description ~ address ~ longitude ~ latitude ~ radius ~ matchVectorId
    }
    
    object Logs extends Table[(Long, java.sql.Timestamp, Long, String)]("Logs")
    {
        def id              = column[Long]("id")
        def time            = column[java.sql.Timestamp]("time")
        def userId          = column[Long]("userId")
        def event           = column[String]("event")
        
        def insertIdSeq     = Sequence[Long]("seq_Logs_id")
        
        def * = id ~ time ~ userId ~ event
    }
  
    object AuthenticatedSessions extends Table[(String, Long, java.sql.Timestamp)]("AuthenticatedSessions")
    {
        def token           = column[String]("token")
        def userId          = column[Long]("userId")
        def expiry          = column[java.sql.Timestamp]("expiry")
        
        def * = token ~ userId ~ expiry
    }
}

object Utilities
{
    def eventLog( userId : Long, event : String )
    {
        val cols = models.Logs.userId ~ models.Logs.event
        cols.insert( userId, event )
    }
    
    def getCurr( seq : Sequence[Long] ) : Long = Query(seq.curr).first
}

