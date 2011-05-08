import org.scalatest.FunSuite
import scala.io.Source._

import java.io.{File, BufferedReader, FileReader}
import scala.collection.mutable.ArrayBuffer
import scala.collection.immutable.TreeSet

import org.apache.lucene.util.Version.LUCENE_30
import org.apache.lucene.analysis.Token
import org.apache.lucene.analysis.tokenattributes.TermAttribute
import org.apache.lucene.analysis.standard.StandardTokenizer

import scala.xml.XML


import org.seacourt.utility._
import org.seacourt.sql.SqliteWrapper._
import org.seacourt.disambiguator.Disambiguator._

class DisambiguatorTest extends FunSuite
{
    private def disambigAssert( phrase : String, expectedTopics : TreeSet[String] )
    {
        val wordList = Utils.luceneTextTokenizer( Utils.normalize( phrase ) )
        val disambiguator = new Disambiguator( wordList.toList, new SQLiteWrapper( new File("disambig.sqlite3") ) )
        disambiguator.build()
        val res = disambiguator.resolve(1)
        assert( expectedTopics.size === res.length )
        
        val resultSet = res.foldLeft( TreeSet[String]() )( _ + _.head._3 )
        for ( (expected, result) <- expectedTopics.zip(resultSet) )
        {
            assert( expected === result )
        }
    }
    
    test( "Disambiguator short phrase test" )
    {
        //val testDbName = "disambig.sqlite3"
        //val testPhrase = "gerry adams troubles bloody sunday"
        // "rice wheat barley"
        // "rice cambridge oxford yale harvard"
        // "rice cheney bush"
        // "java haskell scala c"
        // "la scala covent garden puccini"
        // "tea party palin"
        // "python palin"
        //val disambiguator = new Disambiguator( wordList.toList, new SQLiteWrapper( new File(testDbName) ) )
        //disambiguator.build()
        //disambiguator.resolve()
        
        disambigAssert( "python palin", TreeSet("Main:Monty Python", "Main:Michael Palin") )
        disambigAssert( "rice cheney bush", TreeSet("Main:Condoleezza Rice", "Main:Dick Cheney", "Main:George W. Bush") )
        disambigAssert( "invasion of kuwait, george bush, saddam hussein", TreeSet("Main:Invasion of Kuwait", "Main:George H. W. Bush", "Main:Saddam Hussein") )
    }
    
    test("Efficient disambiguator test")
    {
        //if ( false )
        /*{
            //val testFileName = "./src/test/scala/data/simpleTest.txt"
            val testFileName = "./src/test/scala/data/monbiotTest.txt"
            val testDbName = "disambig.sqlite3"
            
            val fileText = fromFile(testFileName).getLines.mkString
            val wordList = Utils.luceneTextTokenizer( Utils.normalize( fileText ) )
            
            //var wordList = "on"::"the"::"first"::"day"::"of"::"christmas"::"partridge"::"in"::"a"::"pear"::"tree"::Nil
            //var wordList = "george" :: "bush" :: "rice" :: "tony" :: "blair" :: "iraq" :: "saddam" :: "gulf" :: "war" :: Nil
            //var wordList = "george" :: "bush" :: "john" :: "major" :: "iraq" :: "saddam" :: "invasion" :: "of" :: "kuwait" :: Nil
            //var wordList = "standing" :: "astride" :: "the" :: "river" :: "temperature" :: "gradient" :: Nil
            //var wordList = "george" :: "bush" :: Nil
            
            
            
            //var wordList = "bush" :: "blair" :: "rice" :: "cheney" :: "saddam" :: Nil
            //var wordList = "in" :: "the" :: "first" :: "place" :: "bush" :: "major" :: "kuwait" :: "war" :: "saddam" :: Nil
            //var wordList = "bush" :: "tree" :: "shrub" :: Nil
            //var wordList = "java" :: "coffee" :: "tea" :: Nil
            //var wordList = "java" :: "haskell" :: "c++" :: Nil
            
            //var wordList = "design" :: "patterns" :: "cotton" :: Nil
            //var wordList = "design" :: "patterns" :: "java" :: Nil
            //var wordList = "rice" :: "wheat" :: "bread" :: Nil
            //var wordList = "rice" :: "yale" :: "oxford" :: "cambridge" :: Nil
            

            val disambiguator = new Disambiguator( wordList.toList, new SQLiteWrapper( new File(testDbName) ) )
            disambiguator.build()
            disambiguator.resolve(3)
        }*/
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


