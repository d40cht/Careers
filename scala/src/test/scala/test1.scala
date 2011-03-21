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
 
class ResTupleTestSuite extends FunSuite
{
    trait TypedCol[T]
    {
        var v : Option[T] = None
        def assign( value : String )
    }
    
    sealed trait HList
    {
        def assign( vals : List[String] )
    }

    final case class HCons[H <: TypedCol[_], T <: HList]( var head : H, tail : T ) extends HList
    {
        def ::[T <: TypedCol[_]](v : T) = HCons(v, this)
        def assign( vals : List[String] )
        {
            head.assign( vals.head )
            tail.assign( vals.tail )
        }
    }
    
    final class HNil extends HList
    {
        def ::[T <: TypedCol[_]](v : T) = HCons(v, this)
        def assign( vals : List[String] )
        {
            require( vals == Nil )
        }
    }
    
    type ::[H <: TypedCol[_], T <: HList] = HCons[H, T]
    
    val HNil = new HNil()
    
    
    
    final class IntCol extends TypedCol[Int]
    {
        def assign( value : String ) { v = Some( value.toInt ) }
    }
    
    final class DoubleCol extends TypedCol[Double]
    {
        def assign( value : String ) { v = Some( value.toDouble ) }
    }
    
    final class StringCol extends TypedCol[String]
    {
        def assign( value : String ) { v = Some( value ) }
    }
    
    trait TypedColMaker[T]
    {
        def build() : TypedCol[T]
    }
    
    object TypedColMaker
    {
        implicit object IntColMaker extends TypedColMaker[Int]
        {
            def build() : TypedCol[Int] = new IntCol()
        }
        implicit object DoubleColMaker extends TypedColMaker[Double]
        {
            def build() : TypedCol[Double] = new DoubleCol()
        }
        implicit object StringColMaker extends TypedColMaker[String]
        {
            def build() : TypedCol[String] = new StringCol()
        }
    }
    
    def Col[T : TypedColMaker]() = implicitly[TypedColMaker[T]].build()
    
    test( "Simple test" )
    {
        val data = Col[Int]::Col[Double]::Col[String]::HNil
        
        data.assign( "12" :: "43.0" :: "Hello" :: Nil )
        assert( data.head.v === Some(12) )
        assert( data.tail.head.v == Some(43.0) )
        assert( data.tail.tail.head.v === Some("Hello") )
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

