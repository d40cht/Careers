import org.scalatest.FunSuite
import org.scalatest.Tag

import scala.collection.mutable.{Stack, ArrayBuffer}
import scala.collection.immutable.{HashSet, HashMap}
import scala.util.Random
import compat.Platform.currentTime

import org.dbpedia.extraction.wikiparser._
import org.dbpedia.extraction.sources.WikiPage

import org.apache.lucene.util.Version.LUCENE_30
import org.apache.lucene.analysis.Token
import org.apache.lucene.analysis.tokenattributes.{TermAttribute, OffsetAttribute, TypeAttribute}
import org.apache.lucene.analysis.standard.StandardTokenizer
import org.apache.lucene.analysis.ASCIIFoldingFilter

import java.io.File
import java.lang.System
import scala.io.Source._

import org.seacourt.sql.SqliteWrapper._
import org.seacourt.utility.{PriorityQ, _}
import org.seacourt.berkeleydb

import scala.util.matching.Regex
import scala.util.Sorting._

import resource._

import java.io.{DataInput, DataOutput, DataInputStream, DataOutputStream, FileInputStream, FileOutputStream}
import java.io.{StringReader}



class TokenizerTest extends FunSuite
{
    test( "Smarter tokenizer test", Tag("UnitTests") )
    {
        val s = "On the first day of Christmas my true love sent to me some lovely C++ wrapped in F# and C#"
        
        val textSource = new StringReader( s.replace( "+", "Plus" ).replace( "#", "Hash" ) )
    	
        val tokenizerBase = new StandardTokenizer( LUCENE_30, textSource )
        val tokenizer = new ASCIIFoldingFilter( tokenizerBase )
    
        var run = true
        var wordList : List[String] = Nil
        while ( run )
        {
            val offset = tokenizer.getAttribute(classOf[OffsetAttribute])
            val ta = tokenizer.getAttribute(classOf[TypeAttribute])
            println( offset.startOffset() + ", " + offset.endOffset() + ": " + ta.`type`() )
            
            val nextTerm = tokenizer.getAttribute(classOf[TermAttribute]).term()
            if ( nextTerm != "" )
            {
                println( nextTerm )
                wordList = nextTerm :: wordList
            }
            run = tokenizer.incrementToken()
        }
        tokenizer.close()
    }
}

class DisjointSetTest extends FunSuite
{
    test( "Disjoint set", Tag("UnitTests") )
    {
        val d1 = new DisjointSet[Int](1)
        val d2 = new DisjointSet[Int](2)
        val d3 = new DisjointSet[Int](4)
        val d4 = new DisjointSet[Int](8)
        
        assert( d1.size() === 1 )
        assert( d2.size() === 1 )
        assert( d3.size() === 1 )
        assert( d4.size() === 1 )
        
        assert( d1.members() === List(d1) )
        assert( d2.members() === List(d2) )
        assert( d3.members() === List(d3) )
        assert( d4.members() === List(d4) )
        
        assert( !(d1 equals d2) )
        assert( !(d3 equals d4) )
        
        d1 join d2
        
        assert( d1.size() === 2 )
        assert( d2.size() === 2 )
        assert( d3.size() === 1 )
        assert( d4.size() === 1 )
        
        assert( d1.members() === List(d1, d2) )
        assert( d2.members() === List(d1, d2) )
        assert( d3.members() === List(d3) )
        assert( d4.members() === List(d4) )
	
	    d3 join d4
	    
	    assert( d1.size() === 2 )
        assert( d2.size() === 2 )
        assert( d3.size() === 2 )
        assert( d4.size() === 2 )
        
        assert( d1.members() === List(d1, d2) )
        assert( d2.members() === List(d1, d2) )
        assert( d3.members() === List(d3, d4) )
        assert( d4.members() === List(d3, d4) )
	    
	    assert( d1 equals d2 )
	    assert( d3 equals d4 )
	
	    d1 join d3
	
	    assert( d1 equals d2 )
	    assert( d2 equals d3 )
	    assert( d3 equals d4 )

        assert( d1.size() === 4 )	
        assert( d2.size() === 4 )
        assert( d3.size() === 4 )
        assert( d4.size() === 4 )
        assert( d1.members() === List(d1, d3, d4, d2) )
        assert( d2.members() === List(d1, d3, d4, d2) )
        assert( d3.members() === List(d1, d3, d4, d2) )
        assert( d4.members() === List(d1, d3, d4, d2) )
    }
}


class SizeTests extends FunSuite
{
    test( "Efficient array large test", Tag("UnitTests") )
    {
        val count = 50000
        val tarr = new EfficientArray[FixedLengthString](0)
        val builder = tarr.newBuilder
        
        for ( i <- 0 until count )
        {
            builder += new FixedLengthString(((count-1)-i).toString)
        }
        
        val data = builder.result()
        
        val sorted = data.sortWith( _.value.toInt < _.value.toInt )
        
        Utils.withTemporaryDirectory( dirName =>
        {
            sorted.save( new DataOutputStream( new FileOutputStream( new File(dirName, "largeTest.bin" ) ) ) )
            
            val larr = new EfficientArray[FixedLengthString](0)
            larr.load( new DataInputStream( new FileInputStream( new File(dirName, "largeTest.bin") ) ) )
            
            assert( larr.size === count )
            for ( i <- 0 until count )
            {
                assert( larr(i).value === i.toString )
            }
        } )
    }
    
    test( "Efficient array builder and serialization test", Tag("UnitTests") )
    {
        val tarr = new EfficientArray[FixedLengthString](0)
        val builder = tarr.newBuilder
        
        builder += new FixedLengthString( "56" )
        builder += new FixedLengthString( "55" )
        builder += new FixedLengthString( "53" )
        builder += new FixedLengthString( "54" )
        builder += new FixedLengthString( "52" )
        
        val arr = builder.result()
        assert( arr.length === 5 )
        
        Utils.withTemporaryDirectory( dirName =>
        { 
            arr.save( new DataOutputStream( new FileOutputStream( new File( dirName, "testEArr.bin" ) ) ) )
            
            val larr = new EfficientArray[FixedLengthString](0)
            larr.load( new DataInputStream( new FileInputStream( new File( dirName, "testEArr.bin" ) ) ) )
            
            assert( arr.length === larr.length )
            for ( i <- 0 until arr.length )
            {
                assert( arr(i).value === larr(i).value )
            }
        } )
    }
    
    test( "Efficient array lower bound test", Tag("UnitTests") )
    {
        def comp( x : EfficientIntPair, y : EfficientIntPair ) =
        {
            if ( x.first != y.first ) x.first < y.first
            else x.second < y.second
        }
        
        {
            val arr = new EfficientArray[EfficientIntPair]( 1 )
            
            arr(0) = new EfficientIntPair( 1, 2 )
            
            assert( comp( new EfficientIntPair( 1, 1 ), new EfficientIntPair( 1, 2 ) ) )
            assert( !comp( new EfficientIntPair( 2, 1 ), new EfficientIntPair( 1, 1 ) ) )
            assert( Utils.lowerBound( new EfficientIntPair( 1, 1 ), arr, comp ) === 0 )
            assert( Utils.lowerBound( new EfficientIntPair( 1, 2 ), arr, comp ) === 0 )
            assert( Utils.lowerBound( new EfficientIntPair( 1, 3 ), arr, comp ) === 1 )
        }
        
        {
            val arr = new EfficientArray[EfficientIntPair]( 2 )
            
            arr(0) = new EfficientIntPair( 1, 2 )
            arr(1) = new EfficientIntPair( 1, 4 )
            
            assert( Utils.lowerBound( new EfficientIntPair( 1, 1 ), arr, comp ) === 0 )
            assert( Utils.lowerBound( new EfficientIntPair( 1, 2 ), arr, comp ) === 0 )
            assert( Utils.lowerBound( new EfficientIntPair( 1, 3 ), arr, comp ) === 1 )
            assert( Utils.lowerBound( new EfficientIntPair( 1, 4 ), arr, comp ) === 1 )
            assert( Utils.lowerBound( new EfficientIntPair( 1, 5 ), arr, comp ) === 2 )
        }
        
        {
            val arr = new EfficientArray[EfficientIntPair]( 5 )
            
            arr(0) = new EfficientIntPair( 1, 2 )
            arr(1) = new EfficientIntPair( 1, 3 )
            arr(2) = new EfficientIntPair( 2, 2 )
            arr(3) = new EfficientIntPair( 2, 4 )
            arr(4) = new EfficientIntPair( 4, 5 )
            
            
            
            assert( Utils.lowerBound( new EfficientIntPair( 1, 1 ), arr, comp ) === 0 )
            assert( Utils.lowerBound( new EfficientIntPair( 1, 2 ), arr, comp ) === 0 )
            assert( Utils.lowerBound( new EfficientIntPair( 1, 3 ), arr, comp ) === 1 )
            
            assert( Utils.lowerBound( new EfficientIntPair( 1, 4 ), arr, comp ) === 2 )
            assert( Utils.lowerBound( new EfficientIntPair( 2, 0 ), arr, comp ) === 2 )
            assert( Utils.lowerBound( new EfficientIntPair( 2, 1 ), arr, comp ) === 2 )
            assert( Utils.lowerBound( new EfficientIntPair( 2, 2 ), arr, comp ) === 2 )
            
            assert( Utils.lowerBound( new EfficientIntPair( 2, 4 ), arr, comp ) === 3 )
            assert( Utils.lowerBound( new EfficientIntPair( 3, 0 ), arr, comp ) === 4 )
            assert( Utils.lowerBound( new EfficientIntPair( 4, 5 ), arr, comp ) === 4 )
            assert( Utils.lowerBound( new EfficientIntPair( 4, 6 ), arr, comp ) === 5 )
        }
    }

    test( "Efficient array test 1", Tag("UnitTests") )
    {
        val arr = new EfficientArray[FixedLengthString]( 5 )
        arr(0) = new FixedLengthString( "57" )
        arr(1) = new FixedLengthString( "55" )
        arr(2) = new FixedLengthString( "53" )
        arr(3) = new FixedLengthString( "54" )
        arr(4) = new FixedLengthString( "52" )
        
        
        assert( arr(0).value === "57" )
        assert( arr(1).value === "55" )
        assert( arr(2).value === "53" )
        assert( arr(3).value === "54" )
        assert( arr(4).value === "52" )
        
        //val comparator = Function2[FixedLengthString, FixedLengthString, Boolean] = _.value < _.value
        val comp = (x : FixedLengthString, y : FixedLengthString) => x.value < y.value
        
        //stableSort( arr, (x:FixedLengthString, y:FixedLengthString) => x.value < y.value )
        val sarr : EfficientArray[FixedLengthString] = arr.sortWith( comp )
        
        assert( sarr.length === 5 )
        
        assert( sarr(0).value === "52" )
        assert( sarr(1).value === "53" )
        assert( sarr(2).value === "54" )
        assert( sarr(3).value === "55" )
        assert( sarr(4).value === "57" )
        
        assert( comp( sarr(0), sarr(1) ) )
        assert( !comp( sarr(1), sarr(0) ) )
        
        assert( Utils.binarySearch( new FixedLengthString("20"), sarr, comp ) === None )
        assert( Utils.binarySearch( new FixedLengthString("52"), sarr, comp ) === Some(0) )
        assert( Utils.binarySearch( new FixedLengthString("53"), sarr, comp ) === Some(1) )
        assert( Utils.binarySearch( new FixedLengthString("54"), sarr, comp ) === Some(2) )
        assert( Utils.binarySearch( new FixedLengthString("55"), sarr, comp ) === Some(3) )
        assert( Utils.binarySearch( new FixedLengthString("57"), sarr, comp ) === Some(4) )
        
        val arr2 = new EfficientArray[FixedLengthString]( 1 )
        arr2(0) = new FixedLengthString( "56" )
        assert( Utils.binarySearch( new FixedLengthString("20"), arr2, comp ) === None )
        assert( Utils.binarySearch( new FixedLengthString("80"), arr2, comp ) === None )
        assert( Utils.binarySearch( new FixedLengthString("56"), arr2, comp ) === Some(0) )
        
        val arr3 = new EfficientArray[FixedLengthString]( 1 )
        arr3(0) = new FixedLengthString("1")
        arr3(0) = new FixedLengthString("12")
        arr3(0) = new FixedLengthString("123")
        arr3(0) = new FixedLengthString("1234")
        arr3(0) = new FixedLengthString("12345")
        arr3(0) = new FixedLengthString("123456")
        arr3(0) = new FixedLengthString("1234567")
        arr3(0) = new FixedLengthString("12345678")
        arr3(0) = new FixedLengthString("123456789")
        arr3(0) = new FixedLengthString("1234567891")
        arr3(0) = new FixedLengthString("12345678912")
        arr3(0) = new FixedLengthString("123456789123")
        arr3(0) = new FixedLengthString("1234567891234")
        arr3(0) = new FixedLengthString("12345678912345")
        //arr3(0) = new FixedLengthString("123456789123456")
        //arr3(0) = new FixedLengthString("1234567891234567")
        //arr3(0) = new FixedLengthString("12345678912345678")
        //arr3(0) = new FixedLengthString("123456789123456789")
    }

    test( "Array size test", Tag("UnitTests") )
    {
        System.gc()
        val before = Runtime.getRuntime().totalMemory()
        
        val db = new SQLiteWrapper( null )
        
        db.exec( "CREATE TABLE test( number TEXT PRIMARY KEY )" )
        val insStatement = db.prepare( "INSERT INTO test VALUES( ? )", HNil )
        
        val testSize = 20000
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
    }
}

class BerkeleyDbTests extends FunSuite
{
    test( "Simple test", Tag("UnitTests") )
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
    
    test( "Redirect parsing", Tag("UnitTests") )
    {
        val pageTitle = "Academic Acceleration"
        val pageText = "#REDIRECT [[Academic acceleration]] {{R from other capitalisation}}"
        
        val page = new WikiPage( WikiTitle.parse( pageTitle ), 0, 0, pageText )
        val parsed = markupParser( page )
        assert( parsed.isRedirect === true )
    }
    
    
}
 
class ResTupleTestSuite extends FunSuite
{
    test( "SQLite wrapper test", Tag("UnitTests") )
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

    test("A first test", Tag("UnitTests"))
    {
        assert( 3 === 5-2 )
        assert( "a" === "a" )
        
        val testStr = "ABC"
        intercept[StringIndexOutOfBoundsException]
        {
            testStr(-1)
        }
    }
    
    test("A simple dbpedia test", Tag("UnitTests"))
    {
        val topicTitle = "Hello_World"
        val topicText = "[[Blah|'''blah blah''']] ''An italicised '''bit''' of text'' <b>Some markup</b>"

        val markupParser = WikiParser()
        val page = new WikiPage( WikiTitle.parse( topicTitle.toString ), 0, 0, topicText.toString )
        val parsed = markupParser( page )
    }
    
    test("Priority Q test", Tag("UnitTests") )
    {
        val v = new PriorityQ[Int]()
        v.add( 12.0, 4 )
        v.add( 12.0, 5 )
        v.add( 15.0, 6 )
        v.add( 14.0, 7 )
        v.add( 13.0, 8 )
        
        assert( v.size === 5 )
        val first = v.popFirst()
        val second = v.popFirst()
        assert( first._1 === 12.0 )
        assert( second._1 == 12.0 )
        assert( HashSet( 4, 5 ) === HashSet( first._2, second._2 ) )
        assert( v.size === 3 )
        v.add( 12.0, 6 )
        assert( v.size === 4 )
        assert( v.popFirst() === (12.0, 6) )
        assert( v.size === 3 )
        assert( v.popFirst() === (13.0, 8) )
        assert( v.size === 2 )
        assert( v.popFirst() === (14.0, 7) )
        assert( v.size === 1 )
        assert( v.popFirst() === (15.0, 6) )
        assert( v.size === 0 )
        assert( v.isEmpty )
    }
    
    test("N Priority Q test", Tag("UnitTests") )
    {
        val v = new NPriorityQ[Int]()
        v.add( 12.0, 4 )
        v.add( 12.0, 5 )
        v.add( 13.0, 6 )
        v.add( 13.0, 7 )
        v.add( 13.0, 8 )
        
        assert( v.size === 5 )
        assert( v.popFirst() === (12.0, 4) )
        assert( v.popFirst() === (12.0, 5) )
        assert( v.size === 3 )
        v.add( 12.0, 6 )
        assert( v.size === 4 )
        assert( v.popFirst() === (12.0, 6) )
        assert( v.size === 3 )
        assert( v.popFirst() === (13.0, 6) )
        assert( v.size === 2 )
        assert( v.popFirst() === (13.0, 7) )
        assert( v.size === 1 )
        assert( v.popFirst() === (13.0, 8) )
        assert( v.size === 0 )
        assert( v.isEmpty )
    }
}

