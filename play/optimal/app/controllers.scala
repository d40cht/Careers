package controllers

import play._
import play.libs._
import play.mvc._
import play.cache._
import play.data.validation._

import scala.io.Source._

import java.io.{File, FileInputStream, BufferedInputStream}
import java.sql.{Timestamp, Blob}
import javax.sql.rowset.serial.SerialBlob
import java.security.MessageDigest

import org.apache.commons.io.IOUtils
import org.apache.commons.io.FileUtils.copyFile
import org.apache.commons.mail.SimpleEmail

import org.scalaquery.session._
import org.scalaquery.ql.basic.{BasicTable => Table}
import org.scalaquery.session.Database.threadLocalSession
import org.scalaquery.ql.extended.H2Driver.Implicit._
import org.scalaquery.ql._
import org.scalaquery.ql.TypeMapper._

import sbinary.Operations._
import sbt.Process._

import org.seacourt.htmlrender._
import org.seacourt.utility._
import org.seacourt.disambiguator.{DocumentDigest, TopicVector}
import org.seacourt.serialization.SerializationProtocol._

import net.liftweb.json.JsonAST
import net.liftweb.json.JsonDSL._
import net.liftweb.json.Printer._

import org.apache.commons.codec.language.DoubleMetaphone

import scala.collection.JavaConversions._
import org.scala_tools.time.Imports._



object WorkTracker
{
    def cvAnalysis( cvId : Long ) = "JOB_cvAnalysis" + cvId
    def searchAnalysis( searchId : Long ) = "JOB_search" + searchId
    
    def setSubmitted( workItem : String )
    {
        Cache.set( workItem, "running", "10mn" )
    }
    
    def setComplete( workItem : String )
    {
        Cache.delete( workItem )
    }
    
    def checkComplete( workItem : String ) =
    {
        Cache.get( workItem ) == None
    }
}

class AuthenticatedController extends Controller
{
    // TODO: Run regular clearouts of the AuthenticatedSessions table
    
    import models._
    
    def authenticated =
    {
        session( "userId" ) match
        {
            case None => None
            case Some( text : String ) =>
            {
                // Do we have the auth token and the user id in the session?
                val userId = text.toLong
                val authToken = session("authToken")
                
                // Is the auth token cached and matching?
                if ( authToken != None && authToken == Cache.get("authToken" + userId) )
                {
                    Some( userId )
                }
                else
                {
                    // Not cached: is it in the database and in date?
                    
                    val db = Database.forDataSource(play.db.DB.datasource)
            
                    db withSession
                    {
                        val res = (for ( r <- AuthenticatedSessions if r.token === authToken && r.userId === userId ) yield r.expiry).list
                        
                        if ( res != Nil )
                        {
                            val now = new Timestamp( (new DateTime() + 3.days).millis )
                            if ( res.head.after( now ) )
                            {
                                None
                            }
                            else
                            {
                                Some( userId )
                            }
                        }
                        else
                        {
                            None
                        }
                    }
                }
            }
        }
    }
    
    def makeSession( userId : Long, userName : String )
    {
        val token = scala.util.Random.nextString(20)
        val expiry = new java.sql.Timestamp((DateTime.now + 5.days).millis)

        AuthenticatedSessions.insert( token, userId, expiry )
        session += ("user" -> userName)
        session += ("userId" -> userId.toString)
        session += ("authToken" -> token)
        
        Cache.set("authToken" + userId, token, "600mn")
    }
    
    def clearSession()
    {
        session.remove("user")
        session.remove("userId")
        session.remove("authToken")
    }
}


