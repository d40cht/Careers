import java.io._
import org.apache.http.client._
import org.apache.http.impl.client._
import org.apache.http.client.methods._
import org.apache.commons.io.IOUtils

import scala.xml._


import org.seacourt.disambiguator._


import wordcram._
import processing.core.{PApplet, PGraphics, PConstants, PGraphicsJava2D}

class MyGraphics2D() extends PGraphicsJava2D
{
    override def displayable() = false
}

class Blah() extends PApplet
{
    //val pg = createGraphics( 400, 300, "MyGraphics2D" )
    
    override def setup()
    {
        // http://wordcram.org/2010/09/09/get-acquainted-with-wordcram/
        
        //colorMode(HSB)
        
        size( 400, 300 )
        
        println( "Here1" )
        noLoop()
    }
    
    override def draw()
    {
        background(255)
        val wc = new wordcram.WordCram( this )
        wc.fromTextFile( "./src/test/scala/data/georgecv.txt" )
        wc.withColors( color(0), color(230, 0, 0), color(0, 0, 230) )
        wc.withPlacer( Placers.centerClump() )
        wc.sizedByWeight( 10, 90 )
        //wc.withAngler( Anglers.mostlyHoriz() )
        
        //wc.withColorer( Colorers.pickFrom() )
            //Fonters.alwaysUse(createFont("LiberationSerif-Regular.ttf", 1)),

        println( "Here2" )
        wc.drawAll()
        println( "Here3" )
        saveFrame( "wordcram.png" )
        exit()
        //Thread.sleep( 15000 )
        //exit()
    }
}


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
        if ( false )
        {
            //val b = new Blah()
            //b.init()
            /*println( "Here4" )
            b.setup()
            b.redraw()
            println( "Here5" )*/
            PApplet.main( List("--present", "Blah").toArray )
        }
        else
        {
            val p = new WebCVProcess()   
            val res = p.fetch( "http://cvnlp.com:9000/application/listcvs" )
            val cvs = XML.loadString(res)
            
            val minId = args(0).toInt
            
            val d = new Disambiguator( "./DisambigData/phraseMap.bin", "./DisambigData/dbout.sqlite", "./DisambigData/categoryHierarchy.bin" )
            for ( id <- (cvs \\ "id") if (id.text.trim.toInt >= minId) )
            {
                val trimmedId = id.text.trim
                println( "Fetching: " + trimmedId )
                val text = p.fetch( "http://cvnlp.com:9000/application/cvtext?id=" + trimmedId )
                
                //println( "Text: " + text )
                
                val b = new d.Builder(text)
                val forest = b.build()
                forest.dumpDebug( "ambiguitydebug" + trimmedId + ".xml" )
                forest.output( "ambiguity" + trimmedId + ".html", "ambiguityresolution" + trimmedId + ".xml" )
                
                Thread.sleep( 10000 )
            }
        }
    }
}

