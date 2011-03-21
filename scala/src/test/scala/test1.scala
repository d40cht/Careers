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
 
class BasicTestSuite1 extends FunSuite
{

    private def parseSurfaceForm( raw : String ) : String =
    {
        val markupFiltered = raw.filter( _ != '\'' )
        
        return markupFiltered
    }

    trait TypedHolder[T]
    {
        var v : Option[T] = None
        def assign( value : String )
    }
    
    final class StringTypeHolder extends TypedHolder[String]
    {
        override def assign( value : String ) { v = Some(value) }
    }
    
    final class IntTypeHolder extends TypedHolder[Int]
    {
        override def assign( value : String ) { v = Some(value.toInt) }
    }
    
    final class DoubleTypeHolder extends TypedHolder[Double]
    {
        override def assign( value : String ) { v = Some(value.toDouble) }
    }
    
    trait TypedHolderMaker[T]
    {
        def build() : TypedHolder[T]
    }
    
    object TypedHolderMaker
    {
        implicit object StringHolderMaker extends TypedHolderMaker[String]
        {
            def build() = new StringTypeHolder()
        }
        
        implicit object IntHolderMaker extends TypedHolderMaker[Int]
        {
            def build() = new IntTypeHolder()
        }
        
        implicit object DoubleTypeHolder extends TypedHolderMaker[Double]
        {
            def build() = new DoubleTypeHolder()
        }
    }
    
    def buildTypeHolder[T : TypedHolderMaker]() = implicitly[TypedHolderMaker[T]].build()
    

    test("Column type test")
    {
        val v1 = buildTypeHolder[Int]()
        v1.assign( "12" )
        assert( v1.v === Some(12) )
        /*val v1 = makeHolder( "12", 0 )
        val v2 = makeHolder( "Hello", "" )
        val v3 = makeHolder( "13.0", 0.0 )
        assert( v1.v === 12 )
        assert( v2.v === "Hello" )
        assert( v3.v === 13.0 )*/
        
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

