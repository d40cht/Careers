import org.scalatest.FunSuite
import scala.collection.mutable.Stack
import scala.collection.immutable.HashSet
import scala.util.Random
import compat.Platform.currentTime

import org.dbpedia.extraction.wikiparser._
import org.dbpedia.extraction.sources.WikiPage
import org.dbpedia.extraction.wikiparser.{Node}

import java.io.File
import java.lang.System
import scala.io.Source._

import org.seacourt.sql.SqliteWrapper._
import org.seacourt.utility._
import org.seacourt.berkeleydb

import scala.util.matching.Regex
import scala.util.Sorting._

import resource._

import java.io.{DataInput, DataOutput}

final class FixedLengthString( var value : String ) extends FixedLengthSerializable
{
    def size = 16
    
    def this() = this("")
    
    override def saveImpl( out : DataOutput )
    {
        out.writeUTF( value )
    }
    
    override def loadImpl( in : DataInput )
    {
        value = in.readUTF()
    }
}

class SizeTests extends FunSuite
{
    test("Efficient array test 1")
    {
        val arr = new EfficientArray[FixedLengthString]( 5 )
        arr(0) = new FixedLengthString( "56" )
        arr(1) = new FixedLengthString( "55" )
        arr(2) = new FixedLengthString( "53" )
        arr(3) = new FixedLengthString( "54" )
        arr(4) = new FixedLengthString( "52" )
        
        
        assert( arr(0).value === "56" )
        assert( arr(1).value === "55" )
        assert( arr(2).value === "53" )
        assert( arr(3).value === "54" )
        assert( arr(4).value === "52" )
        
        val r : Seq[FixedLengthString] = arr
        stableSort( r, (x:FixedLengthString, y:FixedLengthString) => x.value < y.value )
        
        assert( arr(0).value === "52" )
        assert( arr(1).value === "53" )
        assert( arr(2).value === "54" )
        assert( arr(3).value === "55" )
        assert( arr(4).value === "56" )
    }

    test("Array size test")
    {
        System.gc()
        val before = Runtime.getRuntime().totalMemory()
        
        val db = new SQLiteWrapper( null )
        
        db.exec( "CREATE TABLE test( number TEXT PRIMARY KEY )" )
        val insStatement = db.prepare( "INSERT INTO test VALUES( ? )", HNil )
        
        val testSize = 2000000
        //val b = new Array[Array[Byte]]( testSize )

        db.exec( "BEGIN" )
        for ( i <- 0 until testSize )
        {
            insStatement.exec( i.toString )
            //b(i) = i.toString.getBytes()
        }
        db.exec( "COMMIT" )
        System.gc()
        
        val after = Runtime.getRuntime().totalMemory()
        println( "##########################> Heapsize change: " + ((after-before) / (1024.0*1024.0)) + "Mb" )
    }
}

class BerkeleyDbTests extends FunSuite
{
    test("Simple test")
    {
        val envPath = new File( "./bdblocaltest" )
        
        for ( env   <- managed( new berkeleydb.Environment( envPath, true ) );
              db    <- managed( env.openDb( "test", true ) ) )
        {        
            db.put( "Hello1", "World1" )
            db.put( "Hello2", "World2" )
            db.put( "Hello3", "World3" )
            db.put( "Hello4", "World4" )
            
            assert( db.get( "Hello1" ) === Some( "World1" ) )
            assert( db.get( "Hello2" ) === Some( "World2" ) )
            assert( db.get( "Hello3" ) === Some( "World3" ) )
            assert( db.get( "Hello4" ) === Some( "World4" ) )
            
            assert( db.get( "Hello5" ) === None )
            assert( db.get( "Hello6" ) === None )
        }
    }
}
 
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
            println( ":__: " + _1(row).get )
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

