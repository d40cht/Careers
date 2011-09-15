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
                    Action(Authenticated.home)
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
        }
    }
    
    
    def listCVs = authRequired
    {
        val db = Database.forDataSource(play.db.DB.datasource)
        db withSession
        {
            val unprocessedCVs = (for ( cv <- CVs ) yield cv.id ~ cv.documentDigest.isNull).list
            
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


object Authenticated extends Controller
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

    
    def home =
    {
        val userId = session("userId").get.toLong
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
            } yield u.fullName ~ c.name ~ c.url ~ c.description ~ p.department ~ p.jobTitle ~ p.yearsExperience ~ m.similarity ~ s.latitude ~ s.longitude ~ p.latitude ~ p.longitude ~ m.id ).list
            
            
            
            val matches = matchData.map( row =>
                (row._1, row._2, row._3, row._4, row._5, row._6, row._7, row._8,
                distance( new LLPoint( row._9, row._10 ), new LLPoint( row._11, row._12 ) ), row._13) ).groupBy( r => (r._2, r._3, r._4) )
                
            html.home( session, flash, matches )
        }
    }
    
    def addSearch =
    {
        val userId = session("userId").get.toLong
        val db = Database.forDataSource(play.db.DB.datasource)
        db withSession
        {
            val cvs = ( for ( u <- CVs if u.userId === userId ) yield u.id ~ u.description ).list
            
            html.addSearch( session, flash, cvs )
        }
    }
    
    def acceptSearch =
    {
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
            val userId = session("userId").get.toLong
            val db = Database.forDataSource(play.db.DB.datasource)
            db withSession
            {
                val description = params.get("description")
                val (latitude, longitude) = parseLocation( params.get("location") )
                val radius = params.get("radius").toDouble
                val matchVectorId = makeMatchVectorFromCV( params.get("chosenCV").toLong )
                
                val cols = Searches.userId ~ Searches.description ~ Searches.longitude ~ Searches.latitude ~ Searches.radius ~ Searches.matchVectorId
                
                cols.insert( userId, description, longitude, latitude, radius, matchVectorId )
                Action(Authenticated.manageSearches)
            }
        }
    }
    
    def addPosition =
    {
        val userId = session("userId").get.toLong
        val db = Database.forDataSource(play.db.DB.datasource)
        db withSession
        {
            val cvs = ( for ( u <- models.CVs if u.userId === userId ) yield u.id ~ u.description ).list
            
            html.addPosition( session, flash, cvs )
        }
    }
    
    def acceptPosition =
    {
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
            val userId = session("userId").get.toLong
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
    
    def managePositions =
    {
        val userId = session("userId").get.toLong
        
        val db = Database.forDataSource(play.db.DB.datasource)
        db withSession
        {
            val positions = ( for
            {
                Join(p, c) <- Position leftJoin Companies on (_.companyId is _.id) if p.userId === userId
                _ <- Query orderBy( p.startYear desc )
            } yield c.name ~ c.url ~ p.department ~ p.jobTitle ~ p.startYear ~ p.endYear ).list
            
            html.managePositions( session, flash, positions )
        }
    }
    
    
    def manageSearches =
    {
        val userId = session("userId").get.toLong
        
        val db = Database.forDataSource(play.db.DB.datasource)
        db withSession
        {
            val searches = ( for
            {
                s <- Searches if s.userId === userId
            } yield s.description ~ s.radius ).list
            
            html.manageSearches( session, flash, searches )
        }
    }
    
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
            val res = ( for ( u <-models.CVs if u.id === cvId && u.userId === userId ) yield u.documentDigest ).firstOption
            
            res match
            {
                case Some( blob ) =>
                {
                    val data = blob.getBytes(1, blob.length().toInt)
                    val dd = fromByteArray[DocumentDigest](data)
                    
                    val groupedSkills = dd.topicVector.rankedAndGrouped
                    
                    val theTable = new play.templates.Html( HTMLRender.skillsTable( groupedSkills ).toString )
                    html.cvAnalysis( session, flash, theTable )
                }
                case None => Forbidden
            }
        }
    }
    
    def matchAnalysis =
    {
        val userId = session("userId").get.toLong
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
                    html.matchAnalysis( session, flash, theTable )
                }
                case None => Forbidden
            }
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

