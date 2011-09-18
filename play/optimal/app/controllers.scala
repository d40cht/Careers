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

import net.liftweb.json.JsonAST
import net.liftweb.json.JsonDSL._
import net.liftweb.json.Printer._

import org.apache.commons.codec.language.DoubleMetaphone

import scala.collection.JavaConversions._

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
    
    object Matches extends Table[(Long, Long, Long, Double, Blob)]("Matches")
    {
        def id              = column[Long]("id")
        def fromMatchId     = column[Long]("fromMatchId")
        def toMatchId       = column[Long]("toMatchId")
        def similarity      = column[Double]("similarity")
        def matchVector     = column[Blob]("matchVector")
        
        def * = id ~ fromMatchId ~ toMatchId ~ similarity ~ matchVector
    }
    
    object Companies extends Table[(Long, String, String, String, String, String)]("Companies")
    {
        def id              = column[Long]("id")
        def name            = column[String]("name")
        def url             = column[String]("url")
        def description     = column[String]("description")
        def nameMatch1      = column[String]("nameMatch1")
        def nameMatch2      = column[String]("nameMatch2")
        
        def * = id ~ name ~ url ~ description ~ nameMatch1 ~ nameMatch2
    }
    
    object Position extends Table[(Long, Long, Long, String, String, Int, Int, Int, Double, Double, Long)]("Position")
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
    
    object Searches extends Table[(Long, Long, String, Double, Double, Double, Long)]("Searches")
    {
        def id              = column[Long]("id")
        def userId          = column[Long]("userId")
        def description     = column[String]("description")
        def longitude       = column[Double]("longitude")
        def latitude        = column[Double]("latitude")
        def radius          = column[Double]("radius")
        def matchVectorId   = column[Long]("matchVectorId")
        
        def * = id ~ userId ~ description ~ longitude ~ latitude ~ radius ~ matchVectorId
    }
    
    object Logs extends Table[(Long, java.sql.Timestamp, Long, String)]("Logs")
    {
        def id              = column[Long]("id")
        def time            = column[java.sql.Timestamp]("time")
        def userId          = column[Long]("userId")
        def event           = column[String]("event")
        
        def * = id ~ time ~ userId ~ event
    }
}

object Utilities
{
    def eventLog( userId : Long, event : String )
    {
        val cols = models.Logs.userId ~ models.Logs.event
        cols.insert( userId, event )
    }
}

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
    def authenticated =
    {
        session( "userId" ) match
        {
            case None => None
            case Some( text : String ) =>
            {
                val userId = text.toLong
                val authToken = session("authToken")
                
                if ( authToken != None && authToken == Cache.get("authToken" + userId ) )
                {
                    Some( userId )
                }
                else
                {
                    None
                }
            }
        }
    }
}

object PublicSite extends AuthenticatedController
{
    import models._
    import views.Application._
    
    private def passwordHash( x : String) = MessageDigest.getInstance("SHA").digest(x.getBytes).map( 0xff & _ ).map( {"%02x".format(_)} ).mkString
    
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
                    
                    val authenticationToken = hashedpw
                    Cache.set("authToken" + userId, authenticationToken, "600mn")
                    
                    flash += ("info" -> ("Welcome " + name ))
                    session += ("user" -> name)
                    session += ("userId" -> userId.toString)
                    session += ("authToken", authenticationToken)
                    
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
                            cols.insert( email, passwordHash( password1 ), name, false )
                            
                            val scopeIdentity = SimpleScalarFunction.nullary[Long]("scope_identity")
                            val userId = Query(scopeIdentity).first
                            
                            Utilities.eventLog( userId, "User registered: %s".format(email) )
                            
                            flash += ("info" -> ("Thanks for registering " + name + ". Please login." ))
                            
                            Action(login)
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

object Batch extends Controller
{
    import models._
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
            val toUpdate = for ( cv <- CVs if cv.id === id ) yield cv.documentDigest
            toUpdate.update( new SerialBlob(data) )
            
            WorkTracker.setComplete( WorkTracker.cvAnalysis(id) )
        }
    }
    
    
    def listCVs = authRequired
    {
        val db = Database.forDataSource(play.db.DB.datasource)
        db withSession
        {
            val unprocessedCVs = (for ( cv <- CVs if cv.documentDigest.isNull ) yield cv.id ~ cv.documentDigest.isNull).list
            
            <cvs>
            {
                for ( (id, dd) <-unprocessedCVs ) yield <cv><id>{id}</id><dd>{dd.toString}</dd></cv>
            }
            </cvs>
        }
    }
    
    def listSearches = authRequired
    {
        val minId = params.get("minId").toLong
        val db = Database.forDataSource(play.db.DB.datasource)
        db withSession
        {
            val searches = (for (s <- Searches if s.id >= minId) yield s.id ~ s.userId ~ s.longitude ~ s.latitude ~ s.radius ~ s.matchVectorId).list
            
            <searches>
            {
                for ( (id, userId, lon, lat, radius, mvId) <- searches ) yield
                {
                    <search>
                        <id>{id}</id>
                        <userId>{userId}</userId>
                        <lon>{lon}</lon>
                        <lat>{lat}</lat>
                        <radius>{radius}</radius>
                        <mvId>{mvId}</mvId>
                    </search>
                }
            }
            </searches>
        }
    }
    
    def listPositions = authRequired
    {
        val minId = params.get("minId").toLong
        val db = Database.forDataSource(play.db.DB.datasource)
        db withSession
        {
            val positions = (for (p <- Position if p.id >= minId) yield p.id ~ p.userId ~ p.longitude ~ p.latitude ~ p.matchVectorId).list
            
            <positions>
            {
                for ( (id, userId, lon, lat, mvId) <- positions ) yield
                {
                    <position>
                        <id>{id}</id>
                        <userId>{userId}</userId>
                        <lon>{lon}</lon>
                        <lat>{lat}</lat>
                        <mvId>{mvId}</mvId>
                    </position>
                }
            }
            </positions>
        }
    }
    
    def matchVector = authRequired
    {
        val mvId = params.get("id").toLong
        
        val db = Database.forDataSource(play.db.DB.datasource)
        db withSession
        {
            val blob = ( for ( m <- MatchVectors if m.id === mvId ) yield m.topicVector ).first
            val data = blob.getBytes(1, blob.length().toInt)
            val stream = new java.io.ByteArrayInputStream(data)
            new play.mvc.results.RenderBinary(stream, "mv.bin", "application/octet-stream", false )
        }
    }
    
    def addMatch = authRequired
    {
        val fromId = params.get("fromId").toLong
        val toId = params.get("toId").toLong
        val similarity = params.get("similarity").toDouble
        
        val reqType = request.contentType
        val data = IOUtils.toByteArray( request.body )
        
        val db = Database.forDataSource(play.db.DB.datasource)
        db withSession
        {
            val cols = Matches.fromMatchId ~ Matches.toMatchId ~ Matches.similarity ~ Matches.matchVector
            cols.insert( fromId, toId, similarity, new SerialBlob(data) )
        }
    }
    
    def searchCompleted = authRequired
    {
        WorkTracker.setComplete( WorkTracker.searchAnalysis(params.get("id").toLong) )
    }
    
    def rpcCVText = authRequired
    {
        val cvId = params.get("id").toLong
        
        val db = Database.forDataSource(play.db.DB.datasource)
        db withSession
        {
            val text = ( for ( u <- CVs if u.id === cvId ) yield u.text ).list
            val blob = text.head
            val data = blob.getBytes(1, blob.length().toInt)
            
            Text( new String(data) )
        }
    }
    
    def clearMatches = authRequired
    {
        val db = Database.forDataSource(play.db.DB.datasource)
        db withSession
        {
            Matches.filter( x => true ).mutate( m => m.delete() )
        }
        Ok
    }
}


object Authenticated extends AuthenticatedController
{
    import views.Application._
    import models._
    
    private def parseLocation(loc : String) =
    {
        val llList = params.get("location").replace("(","").replace(")","").split(",").map( _.trim.toDouble )
        (llList(0), llList(1))
    }
    
    private def makeMatchVectorFromCV( cvId : Long ) =
    {
        val cvId = params.get("chosenCV").toLong
        val blob = (for (row <- CVs if row.id === cvId) yield row.documentDigest).first
        
        val data = blob.getBytes(1, blob.length().toInt)
        val dd = fromByteArray[DocumentDigest](data)
        
        val tvData = toByteArray(dd.topicVector)
        
        val rows = MatchVectors.cvId ~ MatchVectors.topicVector
        rows.insert( cvId, new SerialBlob(tvData) )
        
        val scopeIdentity = SimpleScalarFunction.nullary[Long]("scope_identity")
        Query(scopeIdentity).first
    }
    
    class LLPoint( val longitude : Double, val latitude : Double )
    {
    }
    
    private def distance( p1 : LLPoint, p2 : LLPoint ) =
    {
        def deg2rad( deg : Double ) = deg * Math.Pi / 180.0
        def rad2deg( rad : Double ) = rad * 180.0 / Math.Pi

        val theta = p1.longitude - p2.longitude
        var dist = Math.sin(deg2rad(p1.latitude)) * Math.sin(deg2rad(p2.latitude)) + Math.cos(deg2rad(p1.latitude)) * Math.cos(deg2rad(p2.latitude)) * Math.cos(deg2rad(theta))
        dist = Math.acos(dist)
        dist = rad2deg(dist)
        dist = dist * 60 * 1.1515
        
        dist
    }

    private def requiredLoggedIn[T]( handler : Long => T ) =
    {
        authenticated match
        {
            case None => Action(PublicSite.index)
            case Some( userId ) => handler( userId )
        }
    }
    
    def home = requiredLoggedIn
    {
        userId =>   

        val db = Database.forDataSource(play.db.DB.datasource)
        db withSession
        {
            val matchData = ( for
            {
                s <- Searches
                m <- Matches
                if s.matchVectorId === m.fromMatchId && s.userId === userId
                p <- Position
                if p.matchVectorId === m.toMatchId 
                c <- Companies
                if c.id === p.companyId
                u <- Users
                if u.id === p.userId
                _ <- Query orderBy( m.similarity desc )
            } yield u.fullName ~ c.id ~ c.name ~ c.url ~ c.description ~ p.department ~ p.jobTitle ~ p.yearsExperience ~ m.similarity ~ s.latitude ~ s.longitude ~ p.latitude ~ p.longitude ~ m.id ).list
            
            
            
            val matches = matchData.map( row =>
                (row._1, row._2, row._3, row._4, row._5, row._6, row._7, row._8, row._9,
                distance( new LLPoint( row._10, row._11 ), new LLPoint( row._12, row._13 ) ), row._14) ).groupBy( r => (r._2, r._3, r._4, r._5) )
                
            val sorted = matches.toList.sortWith( (x, y) => x._2.map( _._9 ).max > y._2.map( _._9 ).max )
                
            html.home( session, flash, sorted, authenticated != None )
        }
    }
    
    def addSearch = requiredLoggedIn
    {
        userId =>
        
        val db = Database.forDataSource(play.db.DB.datasource)
        db withSession
        {
            val cvs = ( for ( u <- CVs if u.userId === userId ) yield u.id ~ u.description ).list
            
            html.addSearch( session, flash, cvs, authenticated != None )
        }
    }
    
    def acceptSearch = requiredLoggedIn
    {
        userId =>
        
        Validation.required( "Description", params.get("description") )
        Validation.required( "Location", params.get("location") )
        Validation.required( "Search radius", params.get("radius") )
        Validation.required( "CV", params.get("chosenCV") )

        if ( Validation.hasErrors )
        {
            params.flash()
            Validation.errors.foreach( error => flash += ("error" -> error.message()) )
            addSearch
        }
        else
        {
            val db = Database.forDataSource(play.db.DB.datasource)
            db withSession
            {
                val description = params.get("description")
                val (latitude, longitude) = parseLocation( params.get("location") )
                val radius = params.get("radius").toDouble
                val matchVectorId = makeMatchVectorFromCV( params.get("chosenCV").toLong )
                
                val cols = Searches.userId ~ Searches.description ~ Searches.longitude ~ Searches.latitude ~ Searches.radius ~ Searches.matchVectorId
                
                cols.insert( userId, description, longitude, latitude, radius, matchVectorId )
                Utilities.eventLog( userId, "Added a search: %s".format(description) )
                
                
                val scopeIdentity = SimpleScalarFunction.nullary[Long]("scope_identity")
                val searchId = Query(scopeIdentity).first
                val jobDetails = WorkTracker.searchAnalysis( searchId )
                WorkTracker.setSubmitted( jobDetails )
                
                flash += ("pend" -> (jobDetails) )
                Action(Authenticated.manageSearches)
            }
        }
    }
    
    def addPosition = requiredLoggedIn
    {
        userId =>
        
        val db = Database.forDataSource(play.db.DB.datasource)
        db withSession
        {
            val cvs = ( for ( u <- models.CVs if u.userId === userId ) yield u.id ~ u.description ).list
            
            html.addPosition( session, flash, cvs, authenticated != None )
        }
    }
    
    def acceptPosition = requiredLoggedIn
    {
        userId =>
        
        Validation.required( "Company name", params.get("companyName") )
        Validation.required( "Company url", params.get("companyUrl") )
        Validation.required( "Company description", params.get("companyDescription") )
        Validation.required( "Department", params.get("department") )
        Validation.required( "Job title", params.get("jobTitle") )
        Validation.required( "Experience", params.get("experience") )
        Validation.required( "Start year", params.get("startYear") )
        Validation.required( "Start year", params.get("endYear") )
        Validation.required( "Address", params.get("address") )
        Validation.required( "Location", params.get("location") )
        Validation.required( "CV for position", params.get("chosenCV") )

        if ( Validation.hasErrors )
        {
            params.flash()
            Validation.errors.foreach( error => flash += ("error" -> error.message()) )
            addPosition
        }
        else
        {
            val db = Database.forDataSource(play.db.DB.datasource)
            db withSession
            {
                // Retrieve company id or make new row and get last insert id
                val companyId =
                {
                    val submittedCompanyId = params.get("companyId")
                    if ( submittedCompanyId != "" )
                    {
                        submittedCompanyId.toInt
                    }
                    else
                    {
                        val name = params.get("companyName")
                        val url = params.get("companyUrl")
                        val description = params.get("companyDescription")
                        val rows = Companies.name ~ Companies.url ~ Companies.description ~ Companies.nameMatch1 ~ Companies.nameMatch2
                        
                        val encoder = new DoubleMetaphone()
                        rows.insert( name, url, description, encoder.encode( name ), encoder.encode( name ) )
                        
                        val scopeIdentity = SimpleScalarFunction.nullary[Long]("scope_identity")
                        
                        Utilities.eventLog( userId, "Added a company: %s, %s".format(name, url) )
                        
                        Query(scopeIdentity).first
                    }
                }
                
                // Make the MatchVectors from the CV
                val matchVectorId = makeMatchVectorFromCV( params.get("chosenCV").toLong )
                
                val (latitude, longitude) = parseLocation( params.get("location") )


                // Add the position itself
                val rows =
                    Position.userId ~ Position.companyId ~ Position.department ~ Position.jobTitle ~
                    Position.yearsExperience ~ Position.startYear ~ Position.endYear ~
                    Position.longitude ~ Position.latitude ~ Position.matchVectorId
                    
                rows.insert( userId, companyId, params.get("department"), params.get("jobTitle"),
                    params.get("experience").toInt, params.get("startYear").toInt, params.get("endYear").toInt,
                    longitude, latitude, matchVectorId )
                
                
                Utilities.eventLog( userId, "Added a position: %s".format(params.get("jobTitle")) )
                Action(Authenticated.managePositions)
            }
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
    
    def uploadCV = requiredLoggedIn
    {
        userId =>
        
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
                val cols = models.CVs.userId ~ models.CVs.description ~ models.CVs.pdf ~ models.CVs.text
                
                cols.insert( userId, description, if (pdfData == null) null else new SerialBlob( pdfData ), new SerialBlob( textData ) )
                
                val scopeIdentity = SimpleScalarFunction.nullary[Long]("scope_identity")
                val cvId = Query(scopeIdentity).first
                WorkTracker.setSubmitted( WorkTracker.cvAnalysis( cvId ) )
                
                Utilities.eventLog( userId, "Uploaded a CV" )
                
                flash += ("pend" -> ( WorkTracker.cvAnalysis(cvId) ) )
                Action(manageCVs)
            }            
        }
        else
        {
            html.uploadCV( session, flash, authenticated != None )
        }
    }
    
    def logout = requiredLoggedIn
    {
        userId =>
        
        val name = session.get("user")
        session.remove("user")
        session.remove("userId")
        flash += ("info" -> ("Goodbye: " + name) )
        Action(PublicSite.index)
    }
    
    def manageCVs = requiredLoggedIn
    {
        userId =>
        
        val db = Database.forDataSource(play.db.DB.datasource)
        db withSession
        {
            val cvs = ( for ( u <- models.CVs if u.userId === userId ) yield u.id ~ u.added ~ u.description ~ !u.pdf.isNull ~ !u.text.isNull ~ !u.documentDigest.isNull ).list
            
            html.manageCVs( session, flash, cvs, authenticated != None )
        }
    }
    
    def managePositions = requiredLoggedIn
    {
        userId =>
        
        val db = Database.forDataSource(play.db.DB.datasource)
        db withSession
        {
            val positions = ( for
            {
                Join(p, c) <- Position leftJoin Companies on (_.companyId is _.id) if p.userId === userId
                _ <- Query orderBy( p.startYear desc )
            } yield c.name ~ c.url ~ p.department ~ p.jobTitle ~ p.startYear ~ p.endYear ).list
            
            html.managePositions( session, flash, positions, authenticated != None )
        }
    }
    
    
    def manageSearches = requiredLoggedIn
    {
        userId =>
        
        val db = Database.forDataSource(play.db.DB.datasource)
        db withSession
        {
            val searches = ( for
            {
                s <- Searches if s.userId === userId
            } yield s.description ~ s.radius ).list
            
            html.manageSearches( session, flash, searches, authenticated != None )
        }
    }
    
    def cvPdf = requiredLoggedIn
    {
        userId =>
        
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
    
    def cvAnalysis = requiredLoggedIn
    {
        userId =>
        
        val cvId = params.get("id").toLong
        
        val db = Database.forDataSource(play.db.DB.datasource)
        db withSession
        {
            val res = ( for ( u <-models.CVs if u.id === cvId && u.userId === userId ) yield u.documentDigest ).firstOption
            
            res match
            {
                case Some( blob ) =>
                {
                    val data = blob.getBytes(1, blob.length().toInt)
                    val dd = fromByteArray[DocumentDigest](data)
                    
                    val groupedSkills = dd.topicVector.rankedAndGrouped
                    
                    val theTable = new play.templates.Html( HTMLRender.skillsTable( groupedSkills ).toString )
                    html.cvAnalysis( session, flash, theTable, authenticated != None )
                }
                case None => Forbidden
            }
        }
    }
    
    def matchAnalysis = requiredLoggedIn
    {
        userId =>
        
        val matchId = params.get("id").toLong
        
        val db = Database.forDataSource(play.db.DB.datasource)
        db withSession
        {
            val res = Matches.filter( row => row.id === matchId ).map( row => row.matchVector ).firstOption
            
            res match
            {
                case Some( blob ) =>
                {
                    val data = blob.getBytes(1, blob.length().toInt)
                    val tv = fromByteArray[TopicVector](data)
                    
                    val groupedSkills = tv.rankedAndGrouped
                    
                    val theTable = new play.templates.Html( HTMLRender.skillsTable( groupedSkills ).toString )
                    html.matchAnalysis( session, flash, theTable, authenticated != None )
                }
                case None => Forbidden
            }
        }
    }
    
    def cvText = requiredLoggedIn
    {
        userId =>
        
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
    
    def validateCompany =
    {
        val companyName = params.get("name")
        
        val encoder = new DoubleMetaphone()
        val asMetaphone = encoder.encode( companyName )
        
        val db = Database.forDataSource(play.db.DB.datasource)
        db withSession
        {
            val res = ( for ( c <- models.Companies if c.nameMatch1 === asMetaphone ) yield c.name ~ c.url ~ c.id ~ c.description ).list.map( x => List(x._1, x._2, x._3.toString, x._4) )
            val toStr = compact(JsonAST.render( res ))
            Json( toStr )
        }
    }
}

