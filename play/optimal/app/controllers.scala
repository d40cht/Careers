package controllers

import play._
import play.libs._
import play.mvc._
import play.cache._

import java.sql.{Timestamp, Blob}
import java.security.MessageDigest

import org.scalaquery.session._
import org.scalaquery.ql.basic.{BasicTable => Table}

import org.scalaquery.session.Database.threadLocalSession
import org.scalaquery.ql.extended.H2Driver.Implicit._

// Import the query language
import org.scalaquery.ql._

// Import the standard SQL types
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
        
        def * = id ~ added ~ email ~ password ~ fullName ~ isAdmin
    }
    
    object CVs extends Table[(Long, Timestamp, Long, Blob, Blob)]("CVs")
    {
        def id          = column[Long]("id")
        def added       = column[Timestamp]("added")
        def userId      = column[Long]("userId")
        def pdf         = column[Blob]("pdf")
        def text        = column[Blob]("text")
        
        def * = id ~ added ~ userId ~ pdf ~ text
    }
}

object Application extends Controller {
    
    import views.Application._
    
    private def passwordHash( x : String) = MessageDigest.getInstance("SHA").digest(x.getBytes).map( 0xff & _ ).map( {"%02x".format(_)} ).mkString
    
    def index = html.index( session, flash )
    def addPosition = html.addPosition( session, flash )
    
    def uploadCV =
    {
        val pdfData = params.get("pdf")
        val textData = params.get("text")
        
        val isUpload = pdfData != null || textData != null
        
        if ( isUpload )
        {
            val db = Database.forDataSource(play.db.DB.datasource)
            
            if ( textData == null )
            {
                // Convert the pdf to plain text here.
            }
            
            db withSession
            {
                
            }            
        }
        else
        {
            html.uploadCV( session, flash )
        }
    }
    
    def manageCVs = html.manageCVs( session, flash )
    def manageSearches = html.manageSearches( session, flash )
    def about = html.about( session, flash )
    def help = html.help( session, flash )
    def contact = html.contact( session, flash )
    
    def login =
    {
        val email = params.get("email")

        if ( email == null )
        {
            html.login( session, flash )
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
                    
                    flash += ("info" -> ("Welcome " + name ))
                    session += ("user" -> name)
                    Action(index)
                }
                else
                {
                    flash += ("error" -> ("Unknown user " + email))
                    html.login(session, flash)
                }
            }
        }
    }
    
    def captcha(id:String) = {
        val captcha = Images.captcha
        val code = captcha.getText("#E4EAFD")
        Cache.set(id, code, "10mn")
        captcha
    }
    
    def logout =
    {
        val name = session.get("user")
        session.remove("user")
        flash += ("info" -> ("Goodbye: " + name) )
        Action(index)
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
                flash += ("error" -> "Passwords do not match. Please try again.")
                html.register(session, flash, uid)
            }
            else
            {
                val stored = Cache.get(captchauid)
                if ( stored.isEmpty || stored.get != captcha )
                {
                    flash += ("error" -> "Captcha text does not match. Please try again.")
                    html.register(session, flash, uid)
                }
                else
                {
                    val db = Database.forDataSource(play.db.DB.datasource)
                    db withSession
                    {
                        val res = (for ( u <- models.Users if u.email === name ) yield u.id).list
                        if ( res != Nil )
                        {
                            flash += ("error" -> "Email address already taken. Please try again.")
                            html.register(session, flash, uid)
                        }
                        else
                        {
                            val cols = models.Users.email ~ models.Users.password ~ models.Users.fullName ~ models.Users.isAdmin
                            cols.insert( email, passwordHash( password1 ), name, false )
                            
                            flash += ("info" -> ("Thanks for registering " + name + ". Please login." ))
                            Action(login)
                        }
                    }
                }
            }
        }
        else
        {
            html.register( session, flash, uid )
        }
    }
       
}
