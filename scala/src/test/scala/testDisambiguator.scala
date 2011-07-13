import org.scalatest.FunSuite
import scala.io.Source._

import java.io.{File, BufferedReader, FileReader, DataInputStream, DataOutputStream, FileInputStream, FileOutputStream, FileWriter}
import scala.collection.mutable.ArrayBuffer
import scala.collection.immutable.{TreeSet, TreeMap, HashSet}

import org.apache.lucene.util.Version.LUCENE_30
import org.apache.lucene.analysis.Token
import org.apache.lucene.analysis.tokenattributes.TermAttribute
import org.apache.lucene.analysis.standard.StandardTokenizer

import scala.xml.{XML, PrettyPrinter}
import scala.collection.mutable.Stack
import scala.collection.mutable.ArrayBuffer


import org.seacourt.utility._
import org.seacourt.sql.SqliteWrapper._
import org.seacourt.disambiguator._
import org.seacourt.wikibatch._

import org.seacourt.disambiguator.Community._

import org.apache.hadoop.io.{Writable, Text, IntWritable}


// Used for generating the category hierarchy. Move somewhere suitable for batch mode.
/*class CategoryTests extends FunSuite
{
    test( "Category Hierarchy" )
    {
        println( "Starting category dump" )
        val db = new SQLiteWrapper( new File("./DisambigData/dbout.sqlite") )

        val allLinks = db.prepare( "SELECT t1.topicId, t1.contextTopicId, t2.name, t3.name FROM linkWeights2 AS t1 INNER JOIN topics AS t2 ON t1.topicId=t2.id INNER JOIN topics AS t3 ON t1.contextTopicId=t3.id ORDER BY topicId, contextTopicId", Col[Int]::Col[Int]::Col[String]::Col[String]::HNil )
        
        
        val linkData = new EfficientArray[EfficientIntPair](0)
        val b = linkData.newBuilder
        for ( row <- allLinks )
        {
            val topicName = _3(row).get
            val contextName = _4(row).get
            if ( topicName.startsWith("Category:") && contextName.startsWith("Category:") )
            {
                val categoryId = _1(row).get
                val parentCategoryId = _2(row).get
                
                b += new EfficientIntPair( categoryId, parentCategoryId )
            }
        }
        
        b.result().save( new DataOutputStream( new FileOutputStream( new File( "categoryHierarchy.bin" ) ) ) )
        println( "   complete..." )
    }
}*/

class CommunityTests extends FunSuite
{
    test( "Louvain" )
    {
        val v = new Louvain[Int]()
        v.addEdge( 1, 2, 1.0 )
        v.addEdge( 1, 3, 1.0 )
        v.addEdge( 1, 4, 1.0 )
        v.addEdge( 2, 3, 1.0 )
        v.addEdge( 2, 4, 1.0 )
        v.addEdge( 3, 4, 1.0 )
        
        v.addEdge( 5, 6, 1.0 )
        v.addEdge( 5, 7, 1.0 )
        v.addEdge( 5, 8, 1.0 )
        v.addEdge( 6, 7, 1.0 )
        v.addEdge( 6, 8, 1.0 )
        v.addEdge( 7, 8, 1.0 )
        
        v.addEdge( 3, 5, 0.2 )
        v.addEdge( 4, 6, 0.2 )
        
        val res = v.run()
        
        println( res )
        
        val expected = new InternalNode[Int]( ArrayBuffer(
            new InternalNode[Int]( ArrayBuffer( new LeafNode[Int]( ArrayBuffer( 1, 2, 3, 4 ) ) ) ),
            new InternalNode[Int]( ArrayBuffer( new LeafNode[Int]( ArrayBuffer( 5, 6, 7, 8 ) ) ) ) ) )
            
        assert( res === expected )
            
    }
}

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
    // NOTE: All categories seem to have a link weight of zero which isn't ideal.
    
    // NOTE: Sad to have 'test suite' and not 'test suites'. Consider stemming.
    
    test( "New disambiguator test" )
    {
        if ( false )
        {
            val d = new Disambiguator( "./DisambigData/phraseMap.bin", "./DisambigData/dbout.sqlite", "./DisambigData/categoryHierarchy.bin" )
            
            //val fileText = fromFile("./src/test/scala/data/georgecv.txt").getLines.mkString(" ")
            //val fileText = fromFile("./src/test/scala/data/RobDonald-CV-Analyst-V6.txt").getLines.mkString(" ")
            //val fileText = fromFile("./src/test/scala/data/gavcv.txt").getLines.mkString(" ")
            //val fileText = fromFile("./src/test/scala/data/sem.txt").getLines.mkString(" ")
            val fileText = fromFile("./src/test/scala/data/awcv.txt").getLines.mkString(" ")
            //val fileText = fromFile("./src/test/scala/data/stevecv.txt").getLines.mkString(" ")
            
            //val fileText = "gerry adams troubles bloody sunday"
            //val fileText = "rice cambridge oxford yale harvard"
            //val fileText = "the leaf, a new electric car from nissan. Blah blah blah blah blah blah blah. One autumn morning, the leaf dropped from the tree."
            
            // At present no shared context between 'smith waterman' and 'gene sequencing'.
            // A case for broadening the context to an additional step away?
            //val fileText = "smith waterman gene sequencing"
            //val fileText = "smith waterman gene sequencing bioinformatics"
            
            //val fileText = "objective caml haskell"
            //val fileText = "smith waterman gene sequencing"
            
            val b = new d.Builder(fileText)
            val forest = b.build()
            forest.dumpDebug( "ambiguitydebug.xml" )
            forest.htmlOutput( "ambiguity.html" )
            forest.dumpGraph( "test.graph", "test.names" )
        }
    }
    
    test( "Disambiguator short phrase test" )
    {
        if ( true )
        {
            val d = new Disambiguator( "./DisambigData/phraseMap.bin", "./DisambigData/dbout.sqlite", "./DisambigData/categoryHierarchy.bin" )
            var fail = false
            
            val testData = XML.loadFile("./src/test/scala/shortPhrases.xml")
            for ( test <- testData \\ "test" )
            {
                val phrase = (test \\ "phrase").text
                val res = (test \\ "topic").map( x => "Main:" + x.text )

                val b = new d.Builder(phrase)
                val forest = b.build()
                forest.dumpDebug( "ambiguitydebug.xml" )
                forest.htmlOutput( "ambiguity.html" )
                var dres = forest.disambiguated
                
                val dresf = dres.filter( _.weight > 0.0 )
                println( phrase, dresf.map( x=>x.name) )
                
                assert( dresf.length === res.length )
                if ( dresf.length == res.length )
                {
                    for ( (topicl, expected) <- dresf.zip(res) )
                    {
                        val topic = topicl.name
                        assert( topic === expected )
                        if ( topic != expected )
                        {
                            println( "############## " + topic + " != " + expected )
                            fail = true
                        }
                        
                    }
                }
                else
                {
                    println( "################ " )
                    fail = true
                }
                
                forest.dumpGraph( "test.graph", "test.names" )
            }
            
            assert( fail === false )
        }
    }
    
    /*test("Monbiot disambiguator test")
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
        }
   }*/
    
    
    
    test( "Disambiguation alternative generation" )
    {
        //             0            1          2            3           4           5           6            7            8        9      10
        val words = "covent" :: "garden" :: "barack" :: "hussein" :: "obama" :: "design" :: "pattern" :: "language" :: "about" :: "a" :: "boy" :: Nil
        var sfs = (0,0) :: (0,1) :: (1,1) :: (2,2) :: (2,3) :: (2,4) :: (3,3) :: (3,4) :: (4,4) :: (5,5) :: (5,6) :: (6,6) :: (6,7) :: (7,7) :: (8,8) :: (8,10) :: (10,10) :: Nil

        val sorted = sfs.sortWith( (x, y) =>
        {
            if ( x._1 != y._1 ) x._1 < y._1
            else x._2 > y._2
        } )
        
        var sites = List[AmbiguitySiteBuilder]()
        for ( (start, end) <- sorted )
        {
            if ( sites == Nil || !sites.head.overlaps( start, end ) )
            {
                sites = new AmbiguitySiteBuilder( start, end ) :: sites
            }
            
            sites.head.extend( new AmbiguityForest.SurfaceFormDetails(start, end, 0, 0.0, TreeMap[Int, Double]() ) )
        }
        sites = sites.reverse
        
        assert( sites.length === 4 )
        val first = sites.head
        val second = sites.tail.head
        val third = sites.tail.tail.head
        val fourth = sites.tail.tail.tail.head
        assert( first.start === 0 )
        assert( first.end === 1 )
        assert( second.start === 2 )
        assert( second.end === 4 )
        assert( third.start === 5 )
        assert( third.end === 7 )
        assert( fourth.start === 8 )
        assert( fourth.end === 10 )
        
        def toWords( l : List[AmbiguitySite#AmbiguityAlternative] ) = l.foldLeft( HashSet[List[List[String]]]() )( (s, el) => s + el.sites.map( t => words.slice( t.start, t.end+1 ) ) )

        val res1 = toWords( first.buildSite().combs.toList )
        val res2 = toWords( second.buildSite().combs.toList )
        val res3 = toWords( third.buildSite().combs.toList )
        val res4 = toWords( fourth.buildSite().combs.toList )
        
        assert( res1.size == 2 )
        assert( res1.contains( ("covent"::Nil) :: ("garden"::Nil) :: Nil ) )
        assert( res1.contains( ("covent" :: "garden" ::Nil) :: Nil ) )
        
        assert( res2.size == 4 )

        assert( res2.contains( ("barack"::Nil) :: ("hussein"::Nil) :: ("obama"::Nil) :: Nil ) )
        assert( res2.contains( ("barack"::Nil) :: ("hussein"::"obama"::Nil) :: Nil ) )
        assert( res2.contains( ("barack"::"hussein"::Nil) :: ("obama"::Nil) :: Nil ) )
        assert( res2.contains( ("barack"::"hussein"::"obama"::Nil) :: Nil ) )
    
        assert( res3.size == 3 )
        assert( res3.contains( ("design"::Nil) :: ("pattern"::Nil) :: ("language"::Nil) :: Nil ) )
        assert( res3.contains( ("design"::Nil) :: ("pattern"::"language"::Nil) ::Nil ) )
        assert( res3.contains( ("design"::"pattern"::Nil) :: ("language"::Nil) ::Nil ) )
    
        assert( res4.size == 2 )
        assert( res4.contains( ("about"::Nil) :: ("boy"::Nil) :: Nil ) )
        assert( res4.contains( ("about"::"a"::"boy"::Nil) :: Nil ) )
        
    }
    
}


