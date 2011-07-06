import java.io._
import org.apache.http.client._
import org.apache.http.impl.client._
import org.apache.http.client.methods._
import org.apache.commons.io.IOUtils

import scala.xml._


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
}

object WebCVProcess
{
    def main( args : Array[String] )
    {
        val p = new WebCVProcess()   
        val res = p.fetch( "http://cvnlp.com:9000/application/listcvs" )
        val cvs = XML.loadString(res)
        
        val d = new Disambiguator( "./DisambigData/phraseMap.bin", "./DisambigData/dbout.sqlite" )
        for ( id <- cvs \\ "id" )
        {
            val trimmedId = id.text.trim
            println( "Fetching: " + trimmedId )
            val text = p.fetch( "http://cvnlp.com:9000/application/cvtext?id=" + trimmedId )
            
            println( "Text: " + text )
            
            val b = new d.Builder(text)
            val forest = b.build()
            forest.dumpDebug( "ambiguitydebug" + trimmedId + ".xml" )
            forest.htmlOutput( "ambiguity" + trimmedId + ".html" )
            
            Thread.sleep( 10000 )
        }
    }
}

