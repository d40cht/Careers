import org.scalatest.FunSuite
import scala.io.Source._

import java.io.{File, BufferedReader, FileReader, DataInputStream, DataOutputStream, FileInputStream, FileOutputStream}
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
import org.seacourt.wikibatch._
import org.seacourt.disambiguator.{PhraseMapBuilder, PhraseMapLookup}

import org.apache.hadoop.io.{Writable, Text, IntWritable}

class WikiBatchPhraseDictTest extends FunSuite
{
    
    test( "Phrase map etc" )
    {
        // Parse all words from a text
        val wordSource = List[(String, Int)]( ("on", 10), ("the", 10), ("first", 10), ("day", 10), ("of", 10), ("christmas", 10), ("my", 10), ("true", 10), ("love", 10), ("sent", 10), ("to", 10), ("me", 10) )
        val phraseSource = List[(String, List[(String, Int)])]( ("on", Nil), ("on the first", Nil), ("first day", Nil), ("on the first day of christmas", Nil), ("my true love", Nil), ("true love", Nil) )
        
        // Then a few parse phrases and save all out       
        {
            val wb = new PhraseMapBuilder( "wordMap", "phraseMap" )
            val wordMap = wb.buildWordMap( wordSource.iterator )
            val phraseMap = wb.parseSurfaceForms( phraseSource.iterator )
            
            val pml = new PhraseMapLookup( wordMap, phraseMap )
            pml.save( new DataOutputStream( new FileOutputStream( new File( "disambigTest.bin" ) ) ) )
        }
        
        // Then re-run and check that the phrases exist
        {
            val pml = new PhraseMapLookup()
            pml.load( new DataInputStream( new FileInputStream( new File( "disambigTest.bin" ) ) ) )
            //pml.dump()
            
            assert( pml.getIter().find( "chicken tikka" ) === -1 )
            assert( pml.getIter().find( "on the first day of christmas bloo" ) === -1 )
            assert( pml.getIter().find( "bloo on the first day of christmas" ) === -1 )

            assert( pml.phraseByIndex( pml.getIter().find( "on" ) ) === List("on") )
            assert( pml.phraseByIndex( pml.getIter().find( "on the first" ) ) === List("on", "the", "first") )
            assert( pml.phraseByIndex( pml.getIter().find( "first day" ) ) === List("first", "day") )
            assert( pml.phraseByIndex( pml.getIter().find( "on the first day of christmas" ) ) === List("on", "the", "first", "day", "of", "christmas") )
        }
    }
}

class DisambiguatorTest extends FunSuite
{
    private def disambigAssert( phrase : String, expectedTopics : TreeSet[String] )
    {
        /*val wordList = Utils.luceneTextTokenizer( Utils.normalize( phrase ) )
        val disambiguator = new Disambiguator( wordList.toList, new SQLiteWrapper( new File("disambig.sqlite3") ) )
        disambiguator.build()
        val res = disambiguator.resolve(1)
        assert( expectedTopics.size === res.length )
        
        val resultSet = res.foldLeft( TreeSet[String]() )( _ + _.head._3 )
        for ( (expected, result) <- expectedTopics.zip(resultSet) )
        {
            assert( expected === result )
        }*/
    }
    
    test( "New disambiguator test" )
    {
        val d = new org.seacourt.disambiguator.Disambiguator.Disambiguator2( "./DisambigData/phraseMap.bin", "./DisambigData/dbout.sqlite" )
        
        //val fileText = fromFile("./src/test/scala/data/sem.txt").getLines.mkString(" ")
        //val fileText = fromFile("./src/test/scala/data/awcv.txt").getLines.mkString(" ")
        //val fileText = fromFile("./src/test/scala/data/stevecv.txt").getLines.mkString(" ")
        
        //val fileText = "gerry adams troubles bloody sunday"
        val fileText = "rice cambridge oxford yale harvard"
        
        val b = new d.Builder(fileText)
        b.build()
        val res = b.resolve(2)
        println( res )
    }
    
    test( "Disambiguator short phrase test" )
    {
       
        val tests = List[(String, List[String])](
            ("python palin", List[String]("Main:Monty Python", "Main:Michael Palin")),
            ("tea party palin", List[String]("Main:Tea Party protests", "Main:Sarah Palin")),
            // Currently rice ends up as Rice, Oregon because the article mentions 'Wheat' by link
            //("cereal wheat barley rice", List[String]("Main:Cereal", "Main:Wheat", "Main:Barley", "Main:Rice")),
            
            // Produces a rubbish list of categories
            //("a cup of coffee or a cup of english breakfast in the morning", Nil)
            ("cereal maize barley rice", List[String]("Main:Cereal", "Main:Maize", "Main:Barley", "Main:Rice")),
            
            // Do we have 'covent' in the dictionary?
            //("la scala covent garden puccini", List[String]()),
            
            // Obsessed with the programming language
            //("java coffee tea", List[String]("Main:Java", "Main:Coffee", "Main:Tea")),
            
            ("rice cambridge oxford yale harvard", List[String]("Main:Rice University", "Main:University of Cambridge", "Main:University of Oxford", "Main:Yale University", "Main:Harvard University" )),
            ("rice cheney george bush", List[String]("Main:Condoleezza Rice", "Main:Dick Cheney", "Main:George W. Bush")),
            ("george bush major invasion of kuwait", List[String]("Main:George H. W. Bush", "Main:John Major", "Main:Invasion of Kuwait")),
            ("java c design patterns", List[String]("Main:Java (programming language)", "Main:C (programming language)", "Main:Design Patterns") ),
            ("wool design patterns", List[String]("Main:Wool", "Main:Pattern (sewing)")),
            ("gerry adams troubles bloody sunday", List[String]("Main:Gerry Adams", "Main:The Troubles", "Main:Bloody Sunday (1972)")) )
            
        for ( (phrase, res) <- tests )
        {
            val d = new org.seacourt.disambiguator.Disambiguator.Disambiguator2( "./DisambigData/phraseMap.bin", "./DisambigData/dbout.sqlite" )
            val b = new d.Builder(phrase)
            b.build()
            val dres = b.resolve(1)
            
            assert( dres.length === res.length )
            for ( (topicl, expected) <- dres.zip(res) )
            {
                val topic = topicl.head._3
                assert( topic === expected )
            }
        }
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


