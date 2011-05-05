import org.scalatest.FunSuite

import java.io.{File, BufferedReader, FileReader}
import scala.collection.mutable.ArrayBuffer

import org.apache.lucene.util.Version.LUCENE_30
import org.apache.lucene.analysis.Token
import org.apache.lucene.analysis.tokenattributes.TermAttribute
import org.apache.lucene.analysis.standard.StandardTokenizer

import scala.xml.XML


import org.seacourt.sql.SqliteWrapper._
import org.seacourt.disambiguator.Disambiguator._

class DisambiguatorTest extends FunSuite
{
    test("Efficient disambiguator test")
    {
        //if ( false )
        {
            val testFileName = "./src/test/scala/data/simpleTest.txt"
            val testDbName = "disambig.sqlite3"
            
            val tokenizer = new StandardTokenizer( LUCENE_30, new BufferedReader( new FileReader( testFileName ) ) )
            var run = true
            /*val wordList = new ArrayBuffer[String]
            
            while ( run )
            {
                val term = tokenizer.getAttribute(classOf[TermAttribute]).term()
                if ( term != "" )
                {
                    wordList.append( term.toLowerCase() )
                }
                run = tokenizer.incrementToken()
            }
            tokenizer.close()*/
            
            //var wordList = "on"::"the"::"first"::"day"::"of"::"christmas"::"partridge"::"in"::"a"::"pear"::"tree"::Nil
            //var wordList = "george" :: "bush" :: "rice" :: "tony" :: "blair" :: "iraq" :: "saddam" :: "gulf" :: "war" :: Nil
            //var wordList = "george" :: "bush" :: "john" :: "major" :: "iraq" :: "saddam" :: "invasion" :: "of" :: "kuwait" :: Nil
            //var wordList = "standing" :: "astride" :: "the" :: "river" :: "temperature" :: "gradient" :: Nil
            //var wordList = "george" :: "bush" :: Nil
            //var wordList = "bush" :: "blair" :: "rice" :: "cheney" :: "saddam" :: Nil
            var wordList = "bush" :: "major" :: "kuwait" :: "saddam" :: Nil

            val disambiguator = new Disambiguator( wordList.toList, new SQLiteWrapper( new File(testDbName) ) )
            disambiguator.build()
            disambiguator.resolve()
        }
    }
    
    /*
    test("Monbiot disambiguator test")
    {
        if ( false )
        {
            val result =
                <html>
                    <head>
                        <title>Simple title</title>
                    </head>
                    <body>
                        <h1>Simple title</h1>
                        { wordList.reverse.mkString( " " ) }
                    </body>
                </html>
                
            XML.save( testOutName, result )
            
            db.dispose()
        }
    }*/
    
    test( "Disambiguation alternative test 1" )
    {
        val test = List( List(1), List(1,2), List(1,2,3) )
        
        
    }
    
    test( "Phrase topic combination test" )
    {
        //                  0      1       2       3      4        5        6       7       8      9      10    11
        val phrase = List( "on", "the", "first", "day", "of", "christmas", "my", "true", "love", "sent", "to", "me" )
        val topics = List( List(1,2,3), List(1,2,3,4,5), List(3,4,5), List(5), List(6,7,8), List(7,8) )
    }
}


