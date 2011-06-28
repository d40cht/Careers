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
import scala.collection.mutable.Stack


import org.seacourt.utility._
import org.seacourt.sql.SqliteWrapper._
import org.seacourt.disambiguator._
import org.seacourt.wikibatch._
import org.seacourt.disambiguator.{PhraseMapBuilder, PhraseMapLookup, AmbiguitySite, SurfaceForm, AmbiguityAlternative}

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
        if ( true )
        {
            val d = new Disambiguator( "./DisambigData/phraseMap.bin", "./dbout.sqlite" )
            
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
        }
    }
    
    test( "Disambiguator short phrase test" )
    {
        if ( false )
        {
            val tests = List[(String, List[String])](
                ("python palin", List("Main:Monty Python", "Main:Michael Palin")),
                ("tea party palin", List("Main:Tea Party protests", "Main:Sarah Palin")),
                // Currently rice ends up as Rice, Oregon because the article mentions 'Wheat' by link
                ("cereal wheat barley rice", List("Main:Cereal", "Main:Wheat", "Main:Barley", "Main:Rice")),
                
                // Produces a rubbish list of categories
                //("a cup of coffee or a cup of english breakfast in the morning", Nil)
                ("cereal maize barley rice", List("Main:Cereal", "Main:Maize", "Main:Barley", "Main:Rice")),
                
                // Because the tokenizer is insensitive to punctuation we end up with 'cambridge united' as the sf and
                // then a massive football context being asserted!
                ("cambridge united kingdom", List("Main:Cambridge", "Main:United Kingdom")),
                ("objective caml, haskell", List("Main:Objective Caml", "Main:Haskell (programming language)")),
                
                
                // Do we have 'covent' in the dictionary?
                ("la scala covent garden puccini", List("Main:La Scala", "Main:Royal Opera House", "Main:Giacomo Puccini")),
                ("smith waterman gene sequencing", List("Main:Smith–Waterman algorithm", "Main:Gene sequencing")),
                ("smith waterman gene sequencing bioinformatics", List("Main:Smith–Waterman algorithm", "Main:Gene sequencing", "Main:Bioinformatics")),
                
                ("java coffee tea", List("Main:Java coffee", "Main:Tea")),
                
                //("rice cambridge oxford yale harvard ", List[String]("Main:Rice University", "Main:University of Cambridge", "Main:University of Oxford", "Main:Yale University", "Main:Harvard University" )),
                //("rice cheney george bush", List[String]("Main:Condoleezza Rice", "Main:Dick Cheney", "Main:George W. Bush")),
                //("george bush john major invasion of kuwait", List[String]("Main:George H. W. Bush", "Main:John Major", "Main:Invasion of Kuwait")),
                ("java c design patterns", List[String]("Main:Java (programming language)", "Main:C++", "Main:Design Patterns") ),
                //("wool design patterns", List[String]("Main:Wool", "Main:Pattern (sewing)")),
                ("the leaf, nissan's new electric car", List[String]("Main:Nissan Leaf", "Main:Nissan Motors", "Main:Electric car")),
                ("one autumn morning, the leaf dropped from the tree", List[String]("Main:Autumn", "Main:Leaf", "Main:Tree")),
                ("the leaf, a new electric car from nissan. Bloork bloork bloork bloork bloork bloork bloork. One autumn morning, the leaf dropped from the tree.",
                    List[String]("Main:Nissan Leaf", "Main:Electric car", "Main:Nissan Motors", "Main:Autumn", "Main:Leaf", "Main:Tree") ),
                ("university of cambridge united kingdom", List("Main:University of Cambridge", "Main:United Kingdom")),
                //("st johns college durham university", List("Main:St John's College, Durham", "Main:Durham university")),
                ("hills road sixth form college cambridge", List("Main:Hills Road Sixth Form College", "Main:Cambridge")),
                ("infra red background radiation", List("Main:Infrared", "Main:Background radiation")),
                ("gerry adams troubles bloody sunday", List[String]("Main:Gerry Adams", "Main:The Troubles", "Main:Bloody Sunday (1972)")) )
                
            val d = new Disambiguator( "./DisambigData/phraseMap.bin", "./dbout.sqlite" )
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
        
        var sites = List[AmbiguitySite]()
        for ( (start, end) <- sorted )
        {
            if ( sites == Nil || !sites.head.overlaps( start, end ) )
            {
                sites = new AmbiguitySite( start, end ) :: sites
            }
            
            sites.head.extend( start, end, null )
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
        
        def toWords( l : List[AmbiguityAlternative] ) = l.map( el => el.sites.map( t => words.slice( t.start, t.end+1 ) ) )

        first.buildCombinations()
        second.buildCombinations()
        third.buildCombinations()
        fourth.buildCombinations()
        
        assert( toWords( first.combs ) ===
            ( ("covent"::Nil) :: ("garden"::Nil) :: Nil ) ::
            ( ("covent" :: "garden" ::Nil) :: Nil ) :: Nil )

        assert( toWords( second.combs ) ===
            ( ("barack"::Nil) :: ("hussein"::Nil) :: ("obama"::Nil) :: Nil ) ::
            ( ("barack"::Nil) :: ("hussein"::"obama"::Nil) :: Nil ) ::
            ( ("barack"::"hussein"::Nil) :: ("obama"::Nil) :: Nil ) ::
            ( ("barack"::"hussein"::"obama"::Nil) :: Nil ) :: Nil )
    
        assert( toWords( third.combs ) ===
            ( ("design"::Nil) :: ("pattern"::Nil) :: ("language"::Nil) :: Nil ) ::
            ( ("design"::Nil) :: ("pattern"::"language"::Nil) ::Nil ) ::
            ( ("design"::"pattern"::Nil) :: ("language"::Nil) ::Nil ) :: Nil )

        assert( toWords( fourth.combs ) ===
            ( ("about"::Nil) :: ("boy"::Nil) :: Nil ) ::
            ( ("about"::"a"::"boy"::Nil) :: Nil ) :: Nil )
        
    }
    
}


