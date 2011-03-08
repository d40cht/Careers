import org.scalatest.FunSuite
import scala.collection.mutable.Stack
import scala.collection.immutable.HashSet

import org.dbpedia.extraction.wikiparser._
import org.dbpedia.extraction.sources.WikiPage
import org.dbpedia.extraction.wikiparser.{Node}

import java.io.File
import com.almworks.sqlite4java._
 
class TestSuite extends FunSuite
{

    private def parseSurfaceForm( raw : String ) : String =
    {
        val markupFiltered = raw.filter( _ != '\'' )
        
        return markupFiltered
    }


    test("A first test")
    {
        assert( 3 === 5-2 )
        assert( "a" === "a" )
        
        val testStr = "ABC"
        intercept[StringIndexOutOfBoundsException]
        {
            testStr(-1)
        }
    }
    
    test("A simple dbpedia test")
    {
        val topicTitle = "Hello_World"
        val topicText = "[[Blah|'''blah blah''']] ''An italicised '''bit''' of text'' <b>Some markup</b>"
        
        //println( topicText.filter(_ != '\'' ) )

        val markupParser = WikiParser()
        val page = new WikiPage( WikiTitle.parse( topicTitle.toString ), 0, 0, topicText.toString )
        val parsed = markupParser( page )
        
        //println( parsed.toString() )
    }
    
    
    
    test("A simple phrasemap test")
    {
        class ResChecker( var expectedResults : List[(String, List[Int])] )
        {
            def check( m : String, terminals : List[Int] )
            {
                assert( expectedResults != Nil )
                val (expectedPhrase, expectedTerminals) = expectedResults.head
                assert( m === expectedPhrase )
                assert( terminals.sortWith( _ < _ ) === expectedTerminals.sortWith( _ < _ ) )
                expectedResults = expectedResults.tail
            }
        }
        
        val pm = new PhraseMap[Int]()
        
        pm.addPhrase( "hell", 1 )
        pm.addPhrase( "hello world, la de la", 2 )
        pm.addPhrase( "hello", 3 )
        pm.addPhrase( "hello", 4 )
        pm.addPhrase( "hello world", 5 )
        pm.addPhrase( "hello world, la de la", 6 )
        pm.addPhrase( "hello world, la de la", 7 )
        
        val testPhrase = "hello birds, hello sky, hello world. it's a hell of a day to be saying hello. hello world, la de la de la."  
        val rc = new ResChecker( List(
            ("hello", List(3, 4)),
            ("hello", List(3, 4)),
            ("hello", List(3, 4)),
            ("hello world", List(5)),
            ("hell", List(1)),
            ("hello", List(3,4)),
            ("hello", List(3,4)),
            ("hello world", List(5)),
            ("hello world, la de la", List(2,6,7) ) ) )
      
        var lastChar = ' '
        val pw = new PhraseWalker( pm, rc.check )
        for (c <- testPhrase )
        {
            if ( PhraseMap.isNonWordChar(lastChar) )
            {
                pw.startNew()
                
            }
            pw.update( c )
            lastChar = c
        }
    }
    
    test("Simple sqlite test")
    {
        //val db = new SQLiteConnection( new File( "test.sqlite3" ) )
        val db = new SQLiteConnection()
        db.open()
        
        db.exec( "CREATE TABLE surfaceForms( form TEXT, topic TEXT )" )
        db.exec( """INSERT INTO surfaceForms VALUES( "hello", "world" )""" )
        //val st = db.prepare( "CREATE TABLE  
    }
}
