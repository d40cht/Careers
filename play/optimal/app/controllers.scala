package controllers

import play._
import play.libs._
import play.mvc._
import play.cache._

import scala.io.Source._

import java.io.{File, FileInputStream, BufferedInputStream}
import java.sql.{Timestamp, Blob}
import javax.sql.rowset.serial.SerialBlob
import java.security.MessageDigest

import org.apache.commons.io.IOUtils
import org.apache.commons.io.FileUtils.copyFile

import org.scalaquery.session._
import org.scalaquery.ql.basic.{BasicTable => Table}

import org.scalaquery.session.Database.threadLocalSession
import org.scalaquery.ql.extended.H2Driver.Implicit._

// Import the query language
import org.scalaquery.ql._

// Import the standard SQL types
import org.scalaquery.ql.TypeMapper._

import sbinary.Operations._
import sbt.Process._

import org.seacourt.htmlrender._
import org.seacourt.utility._
import org.seacourt.disambiguator.{DocumentDigest, TopicVector}
import org.seacourt.serialization.SerializationProtocol._

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
    
    object CVs extends Table[(Long, Timestamp, String, Long, Blob, Blob, Blob)]("CVs")
    {
        def id              = column[Long]("id")
        def added           = column[Timestamp]("added")
        def description     = column[String]("description")
        def userId          = column[Long]("userId")
        def pdf             = column[Blob]("pdf")
        def text            = column[Blob]("text")
        def documentDigest  = column[Blob]("documentDigest")
        
        def * = id ~ added ~ description ~ userId ~ pdf ~ text ~ documentDigest
    }
    
    object MatchVectors extends Table[(Long, Long, Blob)]("MatchVector")
    {
        def id              = column[Long]("id")
        def cvId            = column[Long]("cvId")
        def topicVector     = column[Blob]("topicVector")
        
        def * = id ~ cvId ~ topicVector
    }
    
    object CVMatches extends Table[(Long, Long, Long, Double, Blob)]("CVMatches")
    {
        def id              = column[Long]("id")
        def fromMatchId     = column[Long]("fromCVId")
        def toMatchId       = column[Long]("toCVId")
        def distance        = column[Double]("distance")
        def matchVector     = column[Blob]("matchVector")
        
        def * = id ~ fromCVId ~ toCVId ~ distance ~ matchVector
    }
    
    object Companies extends Table[(Long, String, String, String, String)]("Companies")
    {
        def id              = column[Long]("id")
        def name            = column[String]("name")
        def url             = column[String]("url")
        def nameMatch1      = column[String]("nameMatch1")
        def nameMatch2      = column[String]("nameMatch2")
        
        def * = id ~ name ~ url ~ nameMatch1 ~ nameMatch2
    }
    
    object Positions extends Table[(Long, Long, Long, String, String, Int, Int, Int, Double, Double, Int)]("Positions")
    {
        def id              = column[Long]("id")
        def userId          = column[Long]("userId")
        def companyId       = column[Long]("companyId")
        def department      = column[String]("department")
        def jobTitle        = column[String]("jobTitle")
        def yearsExperience = column[Int]("yearsExperience")
        def startYear       = column[Int]("startYear")
        def endYear         = column[Int]("endYear")
        def longitude       = column[Double]("longitude")
        def latitude        = column[Double]("latitude")
        def matchVectorId   = column[Long]("matchVectorId")
        
        def * = id ~ userId ~ companyId ~ department ~ jobTitle ~ yearsExperience ~ startYear ~ endYear ~ longitude ~ latitude ~ matchVectorId
    }
}

object PublicSite extends Controller
{    
    import views.Application._
    
    private def passwordHash( x : String) = MessageDigest.getInstance("SHA").digest(x.getBytes).map( 0xff & _ ).map( {"%02x".format(_)} ).mkString
    
    def index = html.index( session, flash )
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
                    session += ("userId" -> userId.toString)
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


object Batch extends Controller
{
    import views.Application._
    
    private def authRequired[T]( handler : => T ) =
    {
        if ( params.get("magic") == Utils.magic )
        {
            handler
        }
        else
        {
            Forbidden
        }
    }
    
    def uploadDocumentDigest = authRequired
    {
        val id = params.get("id").toLong
        
        val reqType = request.contentType
        val data = IOUtils.toByteArray( request.body )
        val dd = fromByteArray[DocumentDigest](data)
        
        val db = Database.forDataSource(play.db.DB.datasource)
        db withSession
        {
            val toUpdate = for ( cv <- models.CVs if cv.id === id ) yield cv.documentDigest
            toUpdate.update( new SerialBlob(data) )
        }
    }
    
    def uploadCVDistance = authRequired
    {
        val fromId = params.get("fromId").toLong
        val toId = params.get("toId").toLong
        val distance = params.get("distance").toLong
        
        val reqType = request.contentType
        val data = IOUtils.toByteArray( request.body )
        val dd = fromByteArray[TopicVector](data)
        
        val db = Database.forDataSource(play.db.DB.datasource)
        db withSession
        {
            // For both the from id and the to id we need to get the existing min 
            // SELECT MIN(matchMetric) AS minMatch, SUM(1) AS count FROM CVMatches WHERE fromId=?
            // Then if count < 10 OR matchMetric > minMatch add/replace
        }
    }
    
    // TODO: Add validation to these data classes. Encryption? IP address restrictions?
    def listCVs = authRequired
    {
        val db = Database.forDataSource(play.db.DB.datasource)
        db withSession
        {
            /*val unprocessedCVs = for {
                Join(cv, md) <- models.CVs leftJoin models.CVMetaData on (_.id is _.cvId)
            } yield cv.id ~ cv.added.?*/
            val unprocessedCVs = (for ( cv <- models.CVs ) yield cv.id ~ cv.documentDigest.isNull).list
            
            <cvs>
            {
                for ( (id, dd) <-unprocessedCVs ) yield <cv><id>{id}</id><dd>{dd.toString}</dd></cv>
            }
            </cvs>
        }
    }
    
    def rpcCVText = authRequired
    {
        val cvId = params.get("id").toLong
        
        val db = Database.forDataSource(play.db.DB.datasource)
        db withSession
        {
            val text = ( for ( u <- models.CVs if u.id === cvId ) yield u.text ).list
            val blob = text.head
            val data = blob.getBytes(1, blob.length().toInt)
            
            Text( new String(data) )
        }
    }
}


object Authenticated extends Controller
{
    import views.Application._
    
    def addPosition =
    {
        val userId = session("userId").get.toLong
        
        val db = Database.forDataSource(play.db.DB.datasource)
        db withSession
        {
            val cvs = ( for ( u <- models.CVs if u.userId === userId ) yield u.description ).list
            
            html.addPosition( session, flash, cvs )
            
            // Break down to add company first (index on DoubleMetaphone)
            // Then when adding the position, also add a MatchVector based
            // on the chosen CV.
        }
    }
    
    private def readFileToBytes( path : File ) =
    {
        val fis = new FileInputStream( path )
        val length = fis.available()
        val ba = new Array[Byte]( length )
        fis.read( ba )
        
        ba
    }
    
    def uploadCV =
    {
        // I don't know why this is required. It appears to be because of a bug in the framework.
        params.checkAndParse()
        val description = params.get("description")
        val pdfArg : File = params.get("pdf", classOf[File])
        val textArg : File = params.get("text", classOf[File])
                
        val isUpload = pdfArg != null || textArg != null
        if ( isUpload )
        {
            val db = Database.forDataSource(play.db.DB.datasource)
            
            // Pdf data can be null but if so text data must exist
            println( pdfArg )
            
            
            var pdfData = if ( pdfArg == null ) null else readFileToBytes(pdfArg)
            
            var textData : Array[Byte] = null
            if ( textArg == null )
            {
                Utils.withTemporaryDirectory( dirName =>
                {
                    copyFile( pdfArg, new File( "./%s/tmp.pdf".format( dirName ) ) )
                    val pdfToTextCmd = "pdftotext ./%s/tmp.pdf ./%s/tmp.txt".format( dirName, dirName )
                    
                    println( pdfToTextCmd )
                    
                    val res = pdfToTextCmd !
                    
                    textData = readFileToBytes( new File(dirName, "tmp.txt") )
                } )
            }
            else
            {
                textData = readFileToBytes(textArg)
            }
            
            db withSession
            {
                // TODO: The session returns an Option. Use pattern matching
                val userId = session("userId").get.toLong
                val cols = models.CVs.userId ~ models.CVs.description ~ models.CVs.pdf ~ models.CVs.text
                
                cols.insert( userId, description, if (pdfData == null) null else new SerialBlob( pdfData ), new SerialBlob( textData ) )
                flash += ("info" -> ("CV uploaded and added to the processing queue. You'll get an email when it is ready.") )
                Action(manageCVs)
            }            
        }
        else
        {
            html.uploadCV( session, flash )
        }
    }
    
    def logout =
    {
        val name = session.get("user")
        session.remove("user")
        flash += ("info" -> ("Goodbye: " + name) )
        Action(PublicSite.index)
    }
    
    def manageCVs =
    {
        val userId = session("userId").get.toLong
        
        val db = Database.forDataSource(play.db.DB.datasource)
        db withSession
        {
            val cvs = ( for ( u <- models.CVs if u.userId === userId ) yield u.id ~ u.added ~ u.description ~ !u.pdf.isNull ~ !u.text.isNull ~ !u.documentDigest.isNull ).list
            
            html.manageCVs( session, flash, cvs )
        }
    }
    
    def manageSearches = html.manageSearches( session, flash )
    
    def cvPdf =
    {
        val userId = session("userId").get.toLong
        val cvId = params.get("id").toLong
        
        val db = Database.forDataSource(play.db.DB.datasource)
        db withSession
        {
            val text = ( for ( u <- models.CVs if u.id === cvId && u.userId === userId ) yield u.pdf ).list
            
            if ( text != Nil )
            {
                val blob = text.head
                val data = blob.getBytes(1, blob.length().toInt)
                val stream = new java.io.ByteArrayInputStream(data)
                new play.mvc.results.RenderBinary(stream, "cv.pdf", "application/pdf", false )
            }
            else Forbidden
        }
    }
    
    def cvAnalysis =
    {
        val userId = session("userId").get.toLong
        val cvId = params.get("id").toLong
        
        val db = Database.forDataSource(play.db.DB.datasource)
        db withSession
        {
            val query = ( for ( u <-models.CVs if u.id === cvId && u.userId === userId ) yield u.documentDigest ).list
            
            if ( query != Nil )
            {
                val blob = query.head
                val data = blob.getBytes(1, blob.length().toInt)
                val dd = fromByteArray[DocumentDigest](data)
                
                val groupedSkills = dd.topicVector.rankedAndGrouped
                
                val theTable = new play.templates.Html( HTMLRender.skillsTable( groupedSkills ).toString )
                html.cvAnalysis( session, flash, theTable )
            }
            else Forbidden
        }
    }
    
    def cvText =
    {
        val userId = session("userId").get.toLong
        val cvId = params.get("id").toLong
        
        val db = Database.forDataSource(play.db.DB.datasource)
        db withSession
        {
            val text = ( for ( u <- models.CVs if u.id === cvId && u.userId === userId ) yield u.text ).list
            
            if ( text != Nil )
            {
                val blob = text.head
                val data = blob.getBytes(1, blob.length().toInt)
                
                Text( new String(data) )
            }
            else Forbidden                
        }
    }
}

