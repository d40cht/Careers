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

object PublicSite extends AuthenticatedController
{
    import models._
    import views.Application._
    
    // Includes a salt for the site
    private def passwordHash( x : String) = MessageDigest.getInstance("SHA").digest(("whatsoutthere" + x).getBytes).map( 0xff & _ ).map( {"%02x".format(_)} ).mkString
    
    def index = html.index( session, flash, authenticated != None )
    def help = html.help( session, flash, authenticated != None )
    def contact = html.contact( session, flash, authenticated != None )
    
    def checkComplete =
    {
        val jobId = params.get("jobId")
        val res = if ( WorkTracker.checkComplete( jobId ) ) "true" else "false"
        
        Text( res )
    }
    
    def viewLogs =
    {
        val db = Database.forDataSource(play.db.DB.datasource)
            
        db withSession
        {
            val res = ( for {
                l <- Logs
                u <- Users
                if l.userId === u.id
            } yield l.time ~ u.email ~ l.event ).list
            
            Text( res.map( r => "%s, %s: %s".format( r._1, r._2, r._3 ) ).mkString( "\n" ) )
        }
    }
    
    def about =
    {
        val db = Database.forDataSource(play.db.DB.datasource)
            
        db withSession
        {
            val numUsers = Users.map( row => ColumnOps.CountAll(row) ).first
            val numPositions = Position.map( row => ColumnOps.CountAll(row) ).first
            val numMatches = Matches.map( row => ColumnOps.CountAll(row) ).first
            
            val stats = "Users: %d, Positions: %d, Matches: %d".format( numUsers, numPositions, numMatches )
            println( stats )
            html.about( session, flash, authenticated != None )
        }
    }
    
    def login =
    {
        val email = params.get("email")

        if ( email == null )
        {
            html.login( session, flash, authenticated != None )
        }
        else
        {
            val db = Database.forDataSource(play.db.DB.datasource)
            
            val hashedpw = passwordHash( params.get("password") )
            db withSession
            {
                val res = (for ( u <- models.Users if u.email === email && u.password === hashedpw ) yield u.id ~ u.fullName ).list
                
                if ( res != Nil )
                {
                    val userId = res.head._1
                    val name = res.head._2
                    
                    makeSession( userId, name )
                    
                    flash += ("info" -> ("Welcome " + name ))
                    
                    Utilities.eventLog( userId, "Logged in" )
                    Action(Authenticated.home)
                }
                else
                {
                    flash += ("error" -> ("Unknown user " + email))
                    html.login(session, flash, authenticated != None)
                }
            }
        }
    }
    
    def captcha( id : String ) = {
        val captcha = Images.captcha
        val code = captcha.getText("#E4EAFD")
        Cache.set(id, code, "10mn")
        captcha
    }
    
    def register = 
    {
        val uid = Codec.UUID
        val email = params.get("email")
        if ( email != null )
        {
            val name = params.get("username")
            val password1 = params.get("password1")
            val password2 = params.get("password2")
            val captchauid = params.get("uid")
            val captcha = params.get("captcha")
            
            if ( password1 != password2 )
            {
                params.flash()
                flash += ("error" -> "Passwords do not match. Please try again.")
                html.register(session, flash, uid, authenticated != None)
            }
            else
            {
                val stored = Cache.get(captchauid)
                if ( stored.isEmpty || (stored.get != captcha && captcha != Utils.magic) )
                {
                    params.flash()
                    flash += ("error" -> "Captcha text does not match. Please try again.")
                    html.register(session, flash, uid, authenticated != None)
                }
                else
                {
                    val db = Database.forDataSource(play.db.DB.datasource)
                    db withSession
                    {
                        val res = (for ( u <- models.Users if u.email === email ) yield u.id).list
                        if ( res != Nil )
                        {
                            params.flash()
                            flash += ("error" -> "Email address already taken. Please try again.")
                            html.register(session, flash, uid, authenticated != None)
                        }
                        else
                        {
                            val cols = models.Users.email ~ models.Users.password ~ models.Users.fullName ~ models.Users.isAdmin
                            
                            threadLocalSession withTransaction
                            {
                                val hashedPassword = passwordHash( password1 )
                                cols.insert( email, hashedPassword, name, false )
                                val userId = Utilities.getCurr(Users.insertIdSeq)
                                
                                Utilities.eventLog( userId, "User registered: %s".format(email) )
                                
                                flash += ("info" -> ("Thanks for registering " + name + "." ))
                                
                                Cache.set("authToken" + userId, hashedPassword, "600mn")
                                
                                session += ("user" -> name)
                                session += ("userId" -> userId.toString)
                                session += ("authToken", hashedPassword)
                                
                                Action(Authenticated.home)
                            }
                        }
                    }
                }
            }
        }
        else
        {
            html.register( session, flash, uid, authenticated != None )
        }
    }
}

