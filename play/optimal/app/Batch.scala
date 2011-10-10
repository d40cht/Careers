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

