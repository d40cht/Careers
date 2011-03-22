import org.scalatest.FunSuite
import scala.collection.mutable.Stack
import scala.collection.immutable.HashSet
import scala.util.Random
import compat.Platform.currentTime

import org.dbpedia.extraction.wikiparser._
import org.dbpedia.extraction.sources.WikiPage
import org.dbpedia.extraction.wikiparser.{Node}

import java.io.File
import com.almworks.sqlite4java._

import SqliteWrapper._
 
class ResTupleTestSuite extends FunSuite
{
    test("SQLite wrapper test")
    {
        val db = new SQLiteWrapper( null )
        db.exec( "BEGIN" )
        db.exec( "CREATE TABLE test( number INTEGER, value FLOAT, name TEXT )" )
        
        val insStatement = db.prepare( "INSERT INTO test VALUES( ?, ?, ? )", HNil )
        insStatement.exec( 1, 5.0, "Hello1" )
        insStatement.exec( 2, 6.0, "Hello2" )
        insStatement.exec( 3, 7.0, "Hello3" )
        insStatement.exec( 4, 8.0, "Hello4" )
        
        val getStatement = db.prepare( "SELECT * from test", Col[Int]::Col[Double]::Col[String]::HNil )
        assert( getStatement.step() === true )
        assert( _1(getStatement.row) === Some(1) )
        assert( _2(getStatement.row) === Some(5.0) )
        assert( _3(getStatement.row) === Some("Hello1") )
        
        getStatement.reset()
        
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
    
    
    
    /*test("A simple phrasemap test")
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
    }*/
    
    test("Simple sqlite test")
    {
        //val db = new SQLiteConnection( new File( "test.sqlite3" ) )
        val db = new SQLiteConnection()
        db.open()
        
        db.exec( "CREATE TABLE surfaceForms( form TEXT, topic TEXT )" )
        db.exec( """INSERT INTO surfaceForms VALUES( "hello", "world" )""" )
        //val st = db.prepare( "CREATE TABLE  
    }
    
    class TestTreeClass( fileName : String )
    {
        val db = new SQLiteConnection( new File( fileName ) )
        //val db = new SQLiteConnection()
        db.open()
        // 200Mb page cache
        db.exec( "PRAGMA cache_size=512000" )
        db.exec( "PRAGMA journal_mode=off" )
        db.exec( "PRAGMA synchronous=off" )
        db.exec( "CREATE TABLE testTree( id INTEGER PRIMARY KEY AUTOINCREMENT, parentId INTEGER, value INTEGER, FOREIGN KEY(parentId) REFERENCES testTree(id), UNIQUE(parentId, value) )" )
        db.exec( "BEGIN" )
        
        val addTreeNode = db.prepare( "INSERT INTO testTree VALUES( NULL, ?, ? )" )
        val getExistingTreeNode = db.prepare( "SELECT id FROM testTree WHERE parentId=? AND value=?" )
        var count = 0
        var checkTime = currentTime
        
        def addLink( parentId : Long, value : Int ) : Long =
        {
            //println( "++ " + parentId + " " + value )
            var treeNodeId = 0L
            
            getExistingTreeNode.reset()
            getExistingTreeNode.bind(1, parentId)
            getExistingTreeNode.bind(2, value)
            if ( getExistingTreeNode.step() )
            {
                treeNodeId = getExistingTreeNode.columnInt(0)
            }
            else
            {
                addTreeNode.reset()
                addTreeNode.bind(1, parentId)
                addTreeNode.bind(2, value)
                addTreeNode.step()
                treeNodeId = db.getLastInsertId()
            }
            getExistingTreeNode.reset()

            
            count += 1
            if ( (count % 100000) == 0 )
            {
                println( "Check " + count + ": " + (currentTime-checkTime) )
                checkTime = currentTime
                db.exec( "COMMIT" )
                db.exec( "BEGIN" )
            }
            
            return treeNodeId
        }
    }
    
    /*test("SQLite performance test")
    {
        val randSource = new Random()
        val testTree = new TestTreeClass( "testTree.sqlite3" )
        
        for ( i <- 0 until 160000 )
        {
            var lastInsertId : Long = -1
            while ( randSource.nextDouble() < 0.8 )
            {
                var nodeValue = randSource.nextInt( 1000000 )
                lastInsertId = testTree.addLink( lastInsertId, nodeValue )
            }
        }
    }*/
}

