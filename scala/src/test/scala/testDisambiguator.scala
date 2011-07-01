import org.scalatest.FunSuite
import scala.io.Source._

import java.io.{File, BufferedReader, FileReader, DataInputStream, DataOutputStream, FileInputStream, FileOutputStream}
import scala.collection.mutable.ArrayBuffer
import scala.collection.immutable.{TreeSet, TreeMap, HashSet}

import org.apache.lucene.util.Version.LUCENE_30
import org.apache.lucene.analysis.Token
import org.apache.lucene.analysis.tokenattributes.TermAttribute
import org.apache.lucene.analysis.standard.StandardTokenizer

import scala.xml.XML
import scala.collection.mutable.Stack


import org.seacourt.utility._
import org.seacourt.sql.SqliteWrapper._
import org.seacourt.disambiguator._
import org.seacourt.wikibatch._
import org.seacourt.disambiguator.{PhraseMapBuilder, PhraseMapLookup, AmbiguitySiteBuilder, AmbiguityForest, AmbiguitySite}

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
    // NOTE: All categories seem to have a link weight of zero which isn't ideal.
    
    // NOTE: Sad to have 'test suite' and not 'test suites'. Consider stemming.
    
    test( "New disambiguator test" )
    {
        if ( false )
        {
            val d = new Disambiguator( "./DisambigData/phraseMap.bin", "./DisambigData/dbout.sqlite" )
            
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
            val tests = List[(String, List[String])](
                // Don't worry about this one now. The parser failed to pull a decent amount of link detail from the John's page.
                //("st johns college durham university", List("Main:St John's College, Durham", "Main:Durham university")),
                ("la scala covent garden puccini", List("Main:La Scala", "Main:Royal Opera House", "Main:Giacomo Puccini")),
                ("cherwell oxford university student newspaper", List("Main:Cherwell (newspaper)", "Main:University of Oxford", "Main:Student newspaper")),
                ("python palin", List("Main:Monty Python", "Main:Michael Palin")),
                ("tea party palin", List("Main:Tea Party protests", "Main:Sarah Palin")),
                
                // Produces a rubbish list of categories
                //("a cup of coffee or a cup of english breakfast in the morning", Nil)
                ("cereal maize barley rice", List("Main:Cereal", "Main:Maize", "Main:Barley", "Main:Rice")),
                
                ("cambridge united kingdom", List("Main:Cambridge", "Main:United Kingdom")),
                ("objective caml, haskell", List("Main:Objective Caml", "Main:Haskell (programming language)")),
                
                ("smith waterman gene sequencing", List("Main:Smith–Waterman algorithm", "Main:DNA sequencing")),
                ("smith waterman gene sequencing bioinformatics", List("Main:Smith–Waterman algorithm", "Main:DNA sequencing", "Main:Bioinformatics")),
                
                //("java coffee tea", List("Main:Java coffee", "Main:Tea")),
                
                //("rice cambridge oxford yale harvard ", List[String]("Main:Rice University", "Main:University of Cambridge", "Main:University of Oxford", "Main:Yale University", "Main:Harvard University" )),
                //("rice cheney george bush", List[String]("Main:Condoleezza Rice", "Main:Dick Cheney", "Main:George W. Bush")),
                ("cheney bush rumsfeld", List[String]("Main:Dick Cheney", "Main:George W. Bush", "Main:Donald Rumsfeld")),
                //("george bush john major invasion of kuwait", List[String]("Main:George H. W. Bush", "Main:John Major", "Main:Invasion of Kuwait")),
                ("java c design patterns", List[String]("Main:Java (programming language)", "Main:C++", "Main:Design Patterns") ),
                //("wool design patterns", List[String]("Main:Wool", "Main:Pattern (sewing)")),
                ("the leaf, nissan's new electric car", List[String]("Main:Nissan Leaf", "Main:Nissan Motors", "Main:Electric car")),
                ("one autumn morning, the leaf dropped from the tree", List[String]("Main:Autumn", "Main:Leaf", "Main:Tree")),
                
                // Distance metric to be developed later.
                //("the leaf, a new electric car from nissan. Bloork bloork bloork bloork bloork bloork bloork. One autumn morning, the leaf dropped from the tree.",
                //    List[String]("Main:Nissan Leaf", "Main:Electric car", "Main:Nissan Motors", "Main:Autumn", "Main:Leaf", "Main:Tree") ),
                ("university of cambridge united kingdom", List("Main:University of Cambridge", "Main:United Kingdom")),
                ("hills road sixth form college cambridge", List("Main:Hills Road Sixth Form College", "Main:Cambridge")),
                ("infra red background radiation", List("Main:Infrared", "Main:Cosmic microwave background radiation")),
                ("gerry adams troubles bloody sunday", List[String]("Main:Gerry Adams", "Main:The Troubles", "Main:Bloody Sunday (1972)")) )
                
            val d = new Disambiguator( "./DisambigData/phraseMap.bin", "./DisambigData/dbout.sqlite" )
            for ( (phrase, res) <- tests )
            {
                //val d = new Disambiguator( "./DisambigData/phraseMap.bin", "./DisambigData/dbout.sqlite" )
                val b = new d.Builder(phrase)
                val forest = b.build()
                forest.dumpDebug( "ambiguitydebug.xml" )
                forest.htmlOutput( "ambiguity.html" )
                var dres = forest.disambiguated
                
                println( phrase, dres.map( x=>x.name) )
                assert( dres.length === res.length )
                for ( (topicl, expected) <- dres.zip(res) )
                {
                    val topic = topicl.name
                    assert( topic === expected )
                }
                
                forest.dumpGraph( "test.graph", "test.names" )
            }
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


