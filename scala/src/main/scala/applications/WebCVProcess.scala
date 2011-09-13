import java.io._
import org.apache.http.client._
import org.apache.http.impl.client._
import org.apache.http.client.methods._
import org.apache.commons.io.IOUtils
import org.apache.http.entity.InputStreamEntity

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

class WebCVProcess
{
    def fetch( url : String ) : String =
    {
        val httpclient = new DefaultHttpClient()
        val httpget = new HttpGet( url )
        val response = httpclient.execute(httpget)
        val entity = response.getEntity()
        if (entity != null)
        {
            val instream = entity.getContent()
            val writer = new StringWriter()
            IOUtils.copy(instream, writer, "UTF-8")
            
            writer.toString()
        }
        else
        {
            null
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

object Digests extends Table[(Int, Blob)]("CVs")
{
    def id      = column[Int]("id", O PrimaryKey)
    def digest  = column[Blob]("digest")
    
    def * = id ~ digest
}

object Matches extends Table[(Int, Int, Double, Blob)]("Matches")
{
    def fromId      = column[Int]("fromId")
    def toId        = column[Int]("toId")
    def similarity  = column[Double]("distance")
    def matchVector = column[Blob]("matchVector")
    
    def * = fromId ~ toId ~ similarity ~ matchVector
    def pk = primaryKey("pk_Matches", fromId ~ toId )
}


object WebCVProcess
{
    def addDigest( docId : Int, digest : DocumentDigest )
    {
        val toCompare = (for ( row <- Digests ) yield row.id ~ row.digest).list
        
        val serialized = toByteArray( digest )
        
        Digests.insert( docId, new SerialBlob(serialized) )
        
        for ( (otherId, otherDigestBlob) <- toCompare )
        {
            val otherDigest = fromByteArray[DocumentDigest]( otherDigestBlob.getBytes(1, otherDigestBlob.length().toInt) )
            
            val (distance, why) = digest.topicVector.distance( otherDigest.topicVector )
            
            // Compare to existing min similarity for this from/to id and update
        }
    }
    
    def main( args : Array[String] )
    {
        val baseUrl = args(0)
        val localDb = args(1)
        val d = new Disambiguator( "./DisambigData/phraseMap.bin", "./DisambigData/dbout.sqlite", "./DisambigData/categoryHierarchy.bin" )
        
        val p = new WebCVProcess()
        
        val makeTables = !(new File( localDb )).exists()
        val db = Database.forURL("jdbc:h2:file:%s;DB_CLOSE_DELAY=-1".format( localDb ), driver = "org.h2.Driver")
        
        db withSession
        {
            if ( makeTables )
            {
                Digests.ddl.create
                Matches.ddl.create
            }
            
            while (true)
            {
                val res = p.fetch( "%s/Batch/listcvs?magic=%s".format( baseUrl, Utils.magic ) )
                
                val cvs = XML.loadString(res)
                for ( cv <- cvs \\ "cv" )
                {
                    val id = (cv \ "id").text.trim.toInt
                    val toProcess = (cv \ "dd").text.trim.toBoolean
                    
                    //if ( toProcess )
                    {
                        println( "Fetching: " + id )
                        val text = p.fetch( "%s/Batch/rpcCVText?id=%d&magic=%s".format( baseUrl, id, Utils.magic ) )
                        
                        val b = new d.Builder(text)
                        val forest = b.build()
                        //forest.dumpDebug( "ambiguitydebug" + id + ".xml" )
                        //forest.output( "ambiguity" + id + ".html", "ambiguityresolution" + id + ".xml" )
                        
                        
                        val dd = forest.getDocumentDigest( id )
                        
                        Utils.withTemporaryDirectory( dirName =>
                        {
                            val ddFile = new java.io.File( dirName, "documentDigest" + id + ".bin" )
                            
                            sbinary.Operations.toFile( dd )( ddFile )
                            p.post( "%s/Batch/uploadDocumentDigest?id=%d&magic=%s".format( baseUrl, id, Utils.magic ), new FileInputStream( ddFile ) )
                        } )

                        addDigest( id, dd )
                    }
                }
                
                println( "Sleeping..." )
                Thread.sleep( 1000 )
            }
        }
    }
}

