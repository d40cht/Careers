import org.scalatest.FunSuite
import scala.collection.mutable.Stack
import scala.collection.immutable.HashSet
import scala.util.Random
import compat.Platform.currentTime

import org.dbpedia.extraction.wikiparser._
import org.dbpedia.extraction.sources.WikiPage
import org.dbpedia.extraction.wikiparser.{Node}

import java.io.File
import scala.io.Source._

import org.seacourt.sql.SqliteWrapper._
import org.seacourt.utility._

import scala.util.matching.Regex
 
class VariousDbpediaParseTests extends FunSuite
{
    val markupParser = WikiParser()
    
    test("Redirect parsing")
    {
        val pageTitle = "Academic Acceleration"
        val pageText = "#REDIRECT [[Academic acceleration]] {{R from other capitalisation}}"
        
        val page = new WikiPage( WikiTitle.parse( pageTitle ), 0, 0, pageText )
        val parsed = markupParser( page )
        println( parsed )
        assert( parsed.isRedirect === true )
    }
    
    test("Page parsing")
    {
        val testFileName = "./src/test/scala/data/parsetest.txt"
        val topicTitle="Test title"
        val text = fromFile(testFileName).getLines.mkString
        
        val parsed = Utils.wikiParse( topicTitle, text )
             
                // Don't bother with list-of and table-of links atm because it's hard to work
                // out which links in the page are part of the list and which are context
        val linkRegex = new Regex( "[=]+[^=]+[=]+" )
        
        Utils.foldlWikiTree( parsed, true, (element : Node, inFirstSection : Boolean) =>
        {
            var newInFirstSection = inFirstSection
            
            element match
            {
                case InternalLinkNode( destination, children, line ) =>
                {
                    // Contexts are: any link to a category or any link in the first section
                    // (also could be any links to topics that are reciprocated)
                    val namespace = destination.namespace.toString
                    if ( namespace == "Category" || (namespace == "Main" && inFirstSection) )
                    {
                        val qualifiedTopicTitle = if (topicTitle.toString.contains(":")) topicTitle.toString else "Main:" + topicTitle
                        println( qualifiedTopicTitle, namespace + ":" + destination.decoded.toString )
                        //output.write( new Text(qualifiedTopicTitle), new Text(namespace + ":" + destination.decoded.toString) )
                    }
                }
                case TextNode( text, line ) =>
                {
                    linkRegex.findFirstIn(text) match
                    {
                        case None =>
                        case _ => newInFirstSection = false
                    }
                }
                case _ =>
            }
            
            newInFirstSection
        } )
    }
}
 
class ResTupleTestSuite extends FunSuite
{
    test("SQLite wrapper test")
    {
        val db = new SQLiteWrapper( null )
        db.exec( "BEGIN" )
        db.exec( "CREATE TABLE test( number INTEGER, value FLOAT, name TEXT )" )
        
        val data = (1, 5.0, "Hello1")::(2, 6.0, "Hello2")::(3, 7.0, "Hello3")::(4, 8.0, "Hello4")::Nil
                
        val insStatement = db.prepare( "INSERT INTO test VALUES( ?, ?, ? )", HNil )
        for ( v <- data ) insStatement.exec( v._1, v._2, v._3 )
        
        val getStatement = db.prepare( "SELECT * from test ORDER BY NUMBER ASC", Col[Int]::Col[Double]::Col[String]::HNil )
	    for ( (row, expected) <- getStatement.zip(data.iterator) )
        {
        	assert( expected._1 === _1(row).get )
        	assert( expected._2 === _2(row).get )
        	assert( expected._3 === _3(row).get )
        }
        
        db.exec( "ROLLBACK" )
    }
}
 
class BasicTestSuite1 extends FunSuite
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
}

