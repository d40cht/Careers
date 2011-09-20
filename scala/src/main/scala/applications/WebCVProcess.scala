import java.io._
import org.apache.http.client._
import org.apache.http.impl.client._
import org.apache.http.client.methods._
import org.apache.commons.io.IOUtils
import org.apache.http.entity.InputStreamEntity

import scala.collection.immutable.HashSet
import scala.xml._

import org.seacourt.utility._
import org.seacourt.disambiguator._

import org.scalaquery.session._
import org.scalaquery.session.Database.threadLocalSession

import org.scalaquery.ql.extended.ExtendedColumnOption.AutoInc
import org.scalaquery.ql.basic.{BasicTable => Table}
import org.scalaquery.ql.basic.BasicDriver.Implicit._
import org.scalaquery.ql.TypeMapper._
import org.scalaquery.ql._

import java.sql.{Timestamp, Blob}
import javax.sql.rowset.serial.SerialBlob

import org.seacourt.serialization.SerializationProtocol._

import sbinary._
import sbinary.Operations._

class HttpUtil
{
    def fetchRaw( url : String )  =
    {
        val httpclient = new DefaultHttpClient()
        val httpget = new HttpGet( url )
        val response = httpclient.execute(httpget)
        val entity = response.getEntity()
        if (entity != null)
        {
            entity.getContent()
        }
        else
        {
            null
        }
    }
    
    def fetch( url : String ) : String =
    {
        val raw = fetchRaw(url)
        if ( raw == null )
        {
            null
        }
        else
        {
            val instream = raw
            val writer = new StringWriter()
            IOUtils.copy(instream, writer, "UTF-8")
            
            writer.toString()
        }
    }
    
    def post( url : String, inputStream : java.io.InputStream )
    {
        val httpClient = new DefaultHttpClient()
        val httpPost = new HttpPost( url )
        
        val reqEntity = new InputStreamEntity( inputStream, -1 )
        reqEntity.setContentType( "binary/octet-stream" )
        httpPost.setEntity( reqEntity )
        
        System.out.println("executing request " + httpPost.getRequestLine());
        val response = httpClient.execute(httpPost);
        val resEntity = response.getEntity();
        
        System.out.println(response.getStatusLine());
        if (resEntity != null)
        {
            System.out.println("Response content length: " + resEntity.getContentLength() )
        }
    }
}

object Positions extends Table[(Long, Long, Double, Double, Long)]("Positions")
{
    def id              = column[Long]("id", O PrimaryKey)
    def userId          = column[Long]("userId")
    def longitude       = column[Double]("longitude")
    def latitude        = column[Double]("latitude")
    def matchVectorId   = column[Long]("matchVectorId")
    
    def * = id ~ userId ~ longitude ~ latitude ~ matchVectorId
}

object Searches extends Table[(Long, Long, Double, Double, Double, Long)]("Searches")
{
    def id              = column[Long]("id", O PrimaryKey)
    def userId          = column[Long]("userId")
    def longitude       = column[Double]("longitude")
    def latitude        = column[Double]("latitude")
    def radius          = column[Double]("radius")
    def matchVectorId   = column[Long]("matchVectorId")
    
    def * = id ~ userId ~ longitude ~ latitude ~ radius ~ matchVectorId
}

object MatchVectors extends Table[(Long, Blob)]("MatchVectors")
{
    def id              = column[Long]("id", O PrimaryKey)
    def topicVector     = column[Blob]("topicVector")
    
    def * = id ~ topicVector
}



class WebCVProcess( val baseUrl : String, val localDb : String )
{
    val d = new Disambiguator( "./DisambigData/phraseMap.bin", "./DisambigData/dbout.sqlite", "./DisambigData/categoryHierarchy.bin" )
    val p = new HttpUtil()
    
    def processCVs()
    {
        val res = p.fetch( "%s/Batch/listcvs?magic=%s".format( baseUrl, Utils.magic ) )
        
        val cvs = XML.loadString(res)
        for ( cv <- cvs \\ "cv" )
        {
            val id = (cv \ "id").text.trim.toInt
            val toProcess = (cv \ "dd").text.trim.toBoolean
            
            println( id, toProcess )
            if ( toProcess )
            {
                println( "Fetching: " + id )
                val text = p.fetch( "%s/Batch/rpcCVText?id=%d&magic=%s".format( baseUrl, id, Utils.magic ) )
                
                val b = new d.Builder(text)
                val forest = b.build()
                
                val dd = forest.getDocumentDigest( id )
                
                Utils.withTemporaryDirectory( dirName =>
                {
                    val ddFile = new java.io.File( dirName, "documentDigest" + id + ".bin" )
                    
                    sbinary.Operations.toFile( dd )( ddFile )
                    p.post( "%s/Batch/uploadDocumentDigest?id=%d&magic=%s".format( baseUrl, id, Utils.magic ), new FileInputStream( ddFile ) )
                } )

            }
        }
    }
    
    private def getMatchVector( matchId : Long ) =
    {
        val res = ( for ( u <- MatchVectors if u.id === matchId ) yield u.topicVector ).firstOption
            
        res match
        {
            case Some( blob ) =>
            {
                val data = blob.getBytes(1, blob.length().toInt)
                val tv = fromByteArray[TopicVector](data)
                
                tv
            }
            case None => null
        }
    }
    
    private def tvDistance( tv1 : TopicVector, tv2 : TopicVector ) =
    {
        val minSize = (tv1.size min tv2.size)
        val tv1cp = tv1.prunedToTop( minSize )
        val tv2cp = tv2.prunedToTop( minSize )
        
        tv1cp.pruneSolitaryContexts( tv2cp, true )
        tv2cp.pruneSolitaryContexts( tv1cp, false )
        
        assert( tv1cp.size <= minSize )
        assert( tv2cp.size <= minSize )
        val (dist, why) = tv1cp.distance(tv2cp)
        
        (scala.math.sqrt(dist), why)
    }
    
    private def checkMatch( searchMatchId : Long, positionMatchId : Long )
    {
        val searchTV = getMatchVector( searchMatchId )
        val posTV = getMatchVector( positionMatchId )
        
        val (similarity, why) = tvDistance( searchTV, posTV )
        
        println( "Match score: ", searchMatchId, positionMatchId, similarity )
        
        if ( similarity > 0.1 )
        {
            val newTV = new TopicVector()
            for ( (priorityWeight, id, te) <- why )
            {
                newTV.addTopic( id, priorityWeight, te.name, te.groupId, te.primaryTopic )
            }
            
            p.post( "%s/Batch/addMatch?fromId=%d&toId=%d&similarity=%.4f&magic=%s".format( baseUrl, searchMatchId, positionMatchId, similarity, Utils.magic ),
                new java.io.ByteArrayInputStream( toByteArray( newTV ) ) )
        }
    }
    
    def processMatches()
    {
        val nextPositionId = Positions.map( row => row.id.max ).first.getOrElse[Long](-1) + 1
        val nextSearchId = Searches.map( row => row.id.max ).first.getOrElse[Long](-1) + 1
        
        var maxPositionId = nextPositionId
        var maxSearchId = nextSearchId
        
        // Fetch all the match data
        var searchIds = HashSet[Long]()
        
        {
            var allMatchVectorIds = HashSet[Long]()
            val positions = XML.loadString( p.fetch( "%s/Batch/listPositions?magic=%s&minId=%d".format( baseUrl, Utils.magic, nextPositionId ) ) )
            for ( pos <- positions \\ "position" )
            {
                val posId = (pos \ "id").text.trim.toLong
                println( "Fetching position: ", posId )
                maxPositionId = maxPositionId max posId
                
                val mvId = (pos \ "mvId").text.trim.toLong
                Positions.insert(
                    posId,
                    (pos \ "userId").text.trim.toLong,
                    (pos \ "lon").text.trim.toDouble,
                    (pos \ "lat").text.trim.toDouble,
                    mvId )
                    
                allMatchVectorIds += mvId
            }

            val searches = XML.loadString( p.fetch( "%s/Batch/listSearches?magic=%s&minId=%d".format( baseUrl, Utils.magic, nextSearchId ) ) )
            
            
            for ( search <- searches \\ "search" )
            {
                val searchId = (search \ "id").text.trim.toLong
                println( "Fetching search: ", searchId )
                maxSearchId = maxSearchId max searchId
                
                val mvId = (search \ "mvId").text.trim.toLong
                Searches.insert(
                    searchId,
                    (search \ "userId").text.trim.toLong,
                    (search \ "lon").text.trim.toDouble,
                    (search \ "lat").text.trim.toDouble,
                    (search \ "radius").text.trim.toDouble,
                    mvId )
                    
                allMatchVectorIds += mvId
                searchIds += searchId
            }
            
            for ( mvId <- allMatchVectorIds )
            {
                val exists = MatchVectors.filter( row => row.id === mvId ).list != Nil
                
                if ( !exists )
                {
                    val instream = p.fetchRaw( "%s/Batch/matchVector?id=%d&magic=%s".format( baseUrl, mvId, Utils.magic ) )
                    println( "Fetching match vector: ", mvId )
                    MatchVectors.insert( mvId, new SerialBlob( IOUtils.toByteArray(instream) ) )
                }
            }
        }
        
        // Process all the match data
        {
            // Get all old searches against new positions, and all new searches against all positions
            val updateQuery = ( for
            {
                s <- Searches
                p <- Positions
                if (s.userId =!= p.userId) && ((s.id < nextSearchId && p.id >= nextPositionId) || (s.id >= nextSearchId))
            } yield s.matchVectorId ~ p.matchVectorId ).list
            
            for ( (sid, pid) <- updateQuery )
            {
                checkMatch( sid, pid )
            }
        }
        
        for ( searchId <- searchIds )
        {
            p.fetchRaw( "%s/Batch/searchCompleted?id=%d&magic=%s".format( baseUrl, searchId, Utils.magic ) )
        }
    }
    
    def run()
    { 
        val makeTables = !(new File( localDb + ".h2.db" )).exists()
        val db = Database.forURL("jdbc:h2:file:%s;DB_CLOSE_DELAY=-1".format( localDb ), driver = "org.h2.Driver")
        db withSession
        {
            if ( makeTables )
            {
                Positions.ddl.create
                Searches.ddl.create
                MatchVectors.ddl.create
            }
            
            while (true)
            {
                try
                {
                    processCVs()
                    processMatches()
                    
                    println( "Sleeping..." )
                    Thread.sleep( 5000 )
                }
                catch
                {
                    case e : java.lang.Exception =>
                    {
                        println( "Exception caught: ", e.getMessage() )
                    }
                }
            }
        }
    }
}


object WebCVProcess
{   
    def main( args : Array[String] )
    {
        val baseUrl = args(0)
        val localDb = args(1)
        
        val wcp = new WebCVProcess( baseUrl, localDb )
        wcp.run()
    }
}

