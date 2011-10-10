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

class SearchMatch(
    val userName : String,
    val companyId : Long,
    val companyName : String,
    val companyUrl : String,
    val companyDescription : String,
    val companyDepartment : String,
    val jobTitle : String,
    val experience : Int,
    val similarity : Double,
    val address : String,
    val distance : Double,
    val matchId : Long,
    val added : java.sql.Timestamp )
{
    def isNew =
    {
        val now = new DateTime()
        
        (new DateTime(added) + 3.days) < now
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
        
        threadLocalSession withTransaction
        {
            rows.insert( cvId, new SerialBlob(tvData) )
            Utilities.getCurr(MatchVectors.insertIdSeq)
        }
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
            } yield u.fullName ~ c.id ~ c.name ~ c.url ~ c.description ~ p.department ~ p.jobTitle ~ p.yearsExperience ~ m.similarity ~ s.address ~ s.latitude ~ s.longitude ~ p.latitude ~ p.longitude ~ m.id ~ m.added ).list
            
            val matches = matchData.map( row => new SearchMatch(
                row._1, row._2, row._3, row._4, row._5, row._6, row._7, row._8, row._9, row._10,
                distance( new LLPoint( row._11, row._12 ), new LLPoint( row._13, row._14 ) ), row._15, row._16 ) ).
                groupBy( m => (m.companyId, m.companyName, m.companyUrl, m.companyDescription) )
            
                
            val sorted = matches.toList.sortWith( (x, y) => x._2.map( _.similarity ).max > y._2.map( _.similarity ).max )
                
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
        Validation.required( "Address", params.get("address") )
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
                val address = params.get("address")
                val (latitude, longitude) = parseLocation( params.get("location") )
                val radius = params.get("radius").toDouble
                val matchVectorId = makeMatchVectorFromCV( params.get("chosenCV").toLong )
                
                val cols = Searches.userId ~ Searches.description ~ Searches.address ~ Searches.longitude ~ Searches.latitude ~ Searches.radius ~ Searches.matchVectorId
                
                threadLocalSession withTransaction
                {
                    cols.insert( userId, description, address, longitude, latitude, radius, matchVectorId )
                    val searchId = Utilities.getCurr(Searches.insertIdSeq)
                    
                    Utilities.eventLog( userId, "Added a search: %s (%d)".format(description, searchId) )
                    val jobDetails = WorkTracker.searchAnalysis( searchId )
                    WorkTracker.setSubmitted( jobDetails )
                    
                    flash += ("pend" -> (jobDetails) )
                    Action(Authenticated.home)
                }
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
                        threadLocalSession withTransaction
                        {
                            rows.insert( name, url, description, encoder.encode( name ), encoder.encode( name ) )
                            val companyId = Utilities.getCurr(Companies.insertIdSeq)
                            
                            Utilities.eventLog( userId, "Added a company: %s, %s".format(name, url) )
                            
                            companyId
                        }
                    }
                }
                
                // Make the MatchVectors from the CV
                val matchVectorId = makeMatchVectorFromCV( params.get("chosenCV").toLong )
                
                val (latitude, longitude) = parseLocation( params.get("location") )


                // Add the position itself
                val rows =
                    Position.userId ~ Position.companyId ~ Position.department ~ Position.jobTitle ~
                    Position.yearsExperience ~ Position.startYear ~ Position.endYear ~ Position.address ~
                    Position.longitude ~ Position.latitude ~ Position.matchVectorId
                    
                rows.insert( userId, companyId, params.get("department"), params.get("jobTitle"),
                    params.get("experience").toInt, params.get("startYear").toInt, params.get("endYear").toInt,
                    params.get("address"), longitude, latitude, matchVectorId )
                
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
                
                threadLocalSession withTransaction
                {
                    cols.insert( userId, description, if (pdfData == null) null else new SerialBlob( pdfData ), new SerialBlob( textData ) )
                    val cvId = Utilities.getCurr(models.CVs.insertIdSeq)
                
                    WorkTracker.setSubmitted( WorkTracker.cvAnalysis( cvId ) )
                    
                    Utilities.eventLog( userId, "Uploaded a CV" )
                    
                    flash += ("pend" -> ( WorkTracker.cvAnalysis(cvId) ) )
                    Action(manageCVs)
                }
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
        clearSession()
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
            } yield s.description ~ s.address ~ s.radius ).list
            
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

