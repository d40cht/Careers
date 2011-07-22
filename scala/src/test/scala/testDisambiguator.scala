import org.scalatest.FunSuite
import scala.io.Source._

import java.io.{File, BufferedReader, FileReader, DataInputStream, DataOutputStream, FileInputStream, FileOutputStream, FileWriter}
import scala.collection.mutable.ArrayBuffer
import scala.collection.immutable.{TreeSet, TreeMap, HashSet, HashMap}

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
import org.seacourt.disambiguator.CategoryHierarchy._

import org.apache.hadoop.io.{Writable, Text, IntWritable}

import math.{log}



// Used for generating the category hierarchy. Move somewhere suitable for batch mode.
/*class CategoryTests extends FunSuite
{
    test( "Category Hierarchy" )
    {
        println( "Starting category dump" )
        val db = new SQLiteWrapper( new File("./DisambigData/dbout.sqlite") )

        val allLinks = db.prepare( "SELECT t1.topicId, t1.contextTopicId, t1.weight, t2.name, t3.name FROM linkWeights2 AS t1 INNER JOIN topics AS t2 ON t1.topicId=t2.id INNER JOIN topics AS t3 ON t1.contextTopicId=t3.id ORDER BY topicId, contextTopicId", Col[Int]::Col[Int]::Col[Double]::Col[String]::Col[String]::HNil )
        
        
        val linkData = new EfficientArray[EfficientIntIntDouble](0)
        val b = linkData.newBuilder
        for ( row <- allLinks )
        {
            val topicName = _4(row).get
            val contextName = _5(row).get
            if ( contextName.startsWith("Category:") && Disambiguator.allowedContext(contextName) )
            {
                val categoryId = _1(row).get
                val parentCategoryId = _2(row).get
                val weight = _3(row).get
                
                b += new EfficientIntIntDouble( categoryId, parentCategoryId, weight )
            }
        }
        
        b.result().save( new DataOutputStream( new FileOutputStream( new File( "categoryHierarchy.bin" ) ) ) )
        println( "   complete..." )
    }
}*/

class CategoryHierarchyTest extends FunSuite
{
    test("Category hierarchy MST")
    {
        val topicIds = List( 1, 2, 3, 4 )
        val edges = List(
            (7, 8, 1.0),
            (7, 1, 1.0),
            (7, 1, 1.0),
            (1, 2, 1.0),
            (1, 6, 1.0),
            (1, 9, 1.0),
            (2, 10, 1.0),
            (10, 9, 1.0),
            (6, 5, 1.0),
            (9, 13, 1.0),
            (9, 12, 1.0),
            (10, 11, 1.0),
            (11, 12, 1.0),
            (12, 13, 1.0),
            (5, 3, 1.0),
            (13, 3, 1.0),
            (11, 15, 1.0),
            (12, 14, 1.0),
            (12, 2, 1.0),
            (14, 15, 1.0),
            (14, 4, 1.0),
            (15, 4, 1.0),
            (4, 16, 1.0),
            (16, 17, 1.0),
            (17, 18, 1.0),
            (18, 19, 1.0),
            (19, 20, 1.0),
            (19, 21, 1.0),
            (21, 22, 1.0),
            (22, 3, 1.0),
            (3, 24, 1.0),
            (24, 25, 1.0),
            (24, 23, 1.0)
        )
        
        /*val b = new Builder( topicIds, edges, x => x.toString )
        val trees = b.run( (x,y) => 0.0 )
        for( tree <- trees ) tree.print( x => x.toString )*/
    }
}


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
    
    test( "Category hierarchy" )
    {
        if ( true )
        {
            val topicDb = new SQLiteWrapper( new File("./DisambigData/dbout.sqlite") )
            topicDb.exec( "PRAGMA cache_size=2000000" )
            
            val ch = new CategoryHierarchy( "./DisambigData/categoryHierarchy.bin", topicDb )
            
            
            //ch.debugDumpCounts(topicDb)
         
            val testData = XML.loadFile("./src/test/scala/data/categoryHierarchyTest.xml")   
            val topicIds = for ( idNode <- testData \\ "id" ) yield idNode.text.toInt
            //val topicIds = List(6275968, 1333154, 2616932, 3903171, 4913632, 3448688, 4519789, 3313539, 5546110, 2090906, 4612339, 3617472, 58782, 415998, 2501803, 8870904, 9060275, 8359607, 4066240, 7323730, 4082275, 4003924, 6625562, 7937128, 4088938, 5786061, 7800535, 693745, 5256577, 779155, 2350518, 6883514, 3801373, 8460715, 2662485, 60809, 4222740, 660650, 5891285, 6887386, 9025757, 2239769, 1788538, 8509935, 7610006, 7261246, 4727354, 3916949, 2029791, 3705473, 1430456, 4954565, 7909026, 2110649, 8737407, 3906966, 1918658, 1575844, 8115928, 5708257, 6650884, 7281642, 6633824, 6657113, 3411260, 6378227, 7546140, 859630, 8324619, 7539795, 4870171, 482041, 7611036, 2170757, 522927, 7096991, 1579286, 3266446, 7979557, 1062303, 7753474, 2141202, 3981414, 4647221, 7835692, 1410124, 4615670, 7274269, 7181129, 8812691, 7077765, 4595217, 7899042, 5079046, 3259924, 5988437, 5203002, 5975225, 5072818, 3517025, 3987297, 8162670, 2479821, 2503972, 812016, 8670431, 3678284, 6796338, 5693770, 3433254, 8331509, 4893651, 7357173, 1704034, 4796230, 1502546, 8252237, 5209846, 4099028, 8375025, 2167575, 313694, 1017108, 7461150, 5422331, 6879102, 6647649, 985574, 5582600, 4796267, 1669050, 5342763, 2330729, 1410304, 941381, 203736, 5720638, 5920159, 8686265, 5329085, 4335580, 5270809, 9045963, 67982, 5621437, 1518228, 998547, 1543774, 1677972, 4474778, 4886195, 7432676, 1385241, 3942479, 1630850, 221884, 5621995, 7900052, 2037264, 5939926, 5152725, 5156857, 7999667, 440732, 3065940, 1903661, 1525727, 2083659, 5324132, 5362097, 8883105)
            //val topicIds = List(6275968, 1333154, 2616932, 3903171, 4913632, 3448688, 4519789, 3313539, 5546110, 2090906, 4612339, 3617472, 58782, 415998, 2501803, 8870904, 9060275, 8359607, 4066240, 7323730, 4082275, 4003924, 6625562, 7937128, 4088938, 5786061, 7800535, 693745, 5256577, 779155, 2350518, 6883514, 3801373, 8460715, 2662485, 60809, 4222740, 660650, 5891285, 6887386, 9025757, 2239769, 1788538)
            //val topicIds = List(6275968, 1333154, 2616932, 3903171, 4913632, 3448688, 4519789, 3313539, 5546110, 2090906, 4612339, 3617472, 58782, 415998, 2501803, 8870904, 9060275)
            //val topicIds = List(3617472, 3906966, 9045963, 4066240, 6657113)
            //val topicIds = List(3906966, 9045963, 4066240, 6657113)
            //val topicIds = List(4066240, 6657113)
           
            val nameQuery = topicDb.prepare( "SELECT name FROM topics WHERE id=?", Col[String]::HNil )
            def getName( id : Int ) =
            {
                nameQuery.bind(id)
                _1(nameQuery.toList(0)).get
            }
        
            for ( id <- topicIds ) println( id + " : " + getName(id) )
            val fullGraph = ch.toTop( topicIds )
            
            println( "Fetching topic linkage weights" )
            var topicDistances = HashMap[(Int, Int), Double]()
            
            {
                var distQuery = topicDb.prepare( "SELECT contextTopicId, weight FROM linkWeights2 WHERE topicId=?", Col[Int]::Col[Double]::HNil )
                for ( id1 <- topicIds; id2 <- topicIds )
                {
                    def contexts( id : Int ) =
                    {
                        distQuery.bind( id )
                        var res = distQuery.foldLeft( TreeMap[Int, Double]() )( (m, v) => m.updated( _1(v).get, _2(v).get ) )
                        
                        /*for ( (id1, weight1) <- res.toList )
                        {
                            distQuery.bind( id1 )
                            
                            for ( row <- distQuery )
                            {
                                val id2 = _1(row).get
                                val weight2 = _2(row).get
                                
                                val prev = res.getOrElse(id2, 0.0)
                                res = res.updated( id2, prev max (weight1*weight2) )
                            }
                        }*/
                        
                        res
                    }

                    val id1Context = contexts(id1)
                    val id2Context = contexts(id2)
                    
                    val dist = id1Context.foldLeft( 0.0 )( (acc, value) =>
                    {
                        val contextId = value._1
                        val weight = value._2
                        if ( id2Context.contains( contextId ) )
                        {
                            acc + weight * id2Context(contextId)
                        }
                        else
                        {
                            acc
                        }
                    } )
                    
                    //println( getName( id1 ) + " - " + getName( id2 ) + ": " + dist )
                    topicDistances = topicDistances.updated( (id1, id2), dist )
                }
            }
            
            //val b = new Builder( topicIds, fullGraph.map( x => (x._1, x._2, -log(x._3)) ) )
            val b = new Builder( topicIds, fullGraph.map( x => (x._1, x._2, 1.0/x._3) ), getName )
            val trees = b.run( (x,y) => topicDistances( (x, y) ) )
            for ( tree <- trees ) tree.print( getName )
            
            /*for ( (fromId, toId, weight) <- fullGraph )
            {
                //println( ":: " + getName(fromId) + " -> " + getName(toId) + ": " + weight )
            }

            val hb = new HierarchyBuilder( topicIds, fullGraph )
            val merges = hb.run( getName )
            
            
            for ( (category, members) <- merges )
            {
                println( "Category: " + getName(category) )
                for ( m <- members )
                {
                    println( "  " + getName(m) )
                }
            }*/
        }
    }
    
    test( "New disambiguator test" )
    {
        /*val v = new PriorityQ[Int]()
        v.add( 12.0, 4 )
        v.add( 12.0, 5 )
        v.add( 13.0, 6 )
        v.add( 13.0, 7 )
        v.add( 13.0, 8 )
        
        assert( v.size === 5 )
        assert( v.popFirst() == (12.0, 4) )
        assert( v.popFirst() == (12.0, 5) )
        assert( v.size == 3 )
        v.add( 12.0, 6 )
        assert( v.size == 4 )
        assert( v.popFirst() == (12.0, 6) )
        assert( v.size == 3 )
        assert( v.popFirst() == (13.0, 6) )
        assert( v.size == 2 )
        assert( v.popFirst() == (13.0, 7) )
        assert( v.size == 1 )
        assert( v.popFirst() == (13.0, 8) )
        assert( v.size == 0 )
        assert( v.isEmpty )*/
        
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
            forest.output( "ambiguity.html", "resolutions.xml" )
            forest.dumpGraph( "test.graph", "test.names" )
        }
    }
    
    test( "Disambiguator short phrase test" )
    {
        if ( false )
        {
            val d = new Disambiguator( "./DisambigData/phraseMap.bin", "./DisambigData/dbout.sqlite", "./DisambigData/categoryHierarchy.bin" )
            var fail = false
            
            val testData = XML.loadFile("./src/test/scala/data/shortPhrases.xml")
            for ( test <- testData \\ "test" )
            {
                val phrase = (test \\ "phrase").text
                val res = (test \\ "topic").map( x => "Main:" + x.text )

                val b = new d.Builder(phrase)
                val forest = b.build()
                forest.dumpDebug( "ambiguitydebug.xml" )
                forest.output( "ambiguity.html", "resolutions.xml" )
                var dres = forest.disambiguated
                
                val dresf = dres.filter( _.weight > 0.0 )
                println( phrase, dresf.map( x=>x.name) )
                
                //assert( dresf.length === res.length )
                if ( dresf.length == res.length )
                {
                    for ( (topicl, expected) <- dresf.zip(res) )
                    {
                        val topic = topicl.name
                        //assert( topic === expected )
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


