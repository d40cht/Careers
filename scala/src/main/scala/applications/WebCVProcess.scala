import java.io._
import org.apache.http.client._
import org.apache.http.impl.client._
import org.apache.http.client.methods._
import org.apache.commons.io.IOUtils
import org.apache.http.entity.InputStreamEntity

import scala.xml._

import org.seacourt.utility._
import org.seacourt.disambiguator._


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


object WebCVProcess
{
    def main( args : Array[String] )
    {
        val baseUrl = args(0)
        val minId = args(1).toInt
        val maxId = args(2).toInt
        
        val p = new WebCVProcess()   
        val res = p.fetch( "%s/application/listcvs?magic=%s".format( baseUrl, Utils.magic ) )
        val cvs = XML.loadString(res)
        

        
        val d = new Disambiguator( "./DisambigData/phraseMap.bin", "./DisambigData/dbout.sqlite", "./DisambigData/categoryHierarchy.bin" )
        for ( id <- (cvs \\ "id") if (id.text.trim.toInt >= minId && id.text.trim.toInt <= maxId) )
        {
            val trimmedId = id.text.trim
            println( "Fetching: " + trimmedId )
            val text = p.fetch( "%s/application/cvtext?id=%s&magic=%s".format( baseUrl, trimmedId, Utils.magic ) )
            
            //println( "Text: " + text )
            
            val b = new d.Builder(text)
            val forest = b.build()
            forest.dumpDebug( "ambiguitydebug" + trimmedId + ".xml" )
            forest.output( "ambiguity" + trimmedId + ".html", "ambiguityresolution" + trimmedId + ".xml" )
            
            val ddFileName = "documentDigest" + trimmedId + ".bin"
            forest.saveDocumentDigest( trimmedId.toInt, ddFileName )
            
            p.post( "%s/application/uploadDocumentDigest?id=%s&magic=%s".format( baseUrl, trimmedId, Utils.magic ), new FileInputStream( new File( ddFileName ) ) )
            
            //Thread.sleep( 10000 )
        }
    }
}

