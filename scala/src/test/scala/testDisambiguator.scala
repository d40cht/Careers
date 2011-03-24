import org.scalatest.FunSuite

import java.io.{File, BufferedReader, FileReader}
import scala.collection.mutable.ArrayBuffer

import org.apache.lucene.util.Version.LUCENE_30
import org.apache.lucene.analysis.Token
import org.apache.lucene.analysis.tokenattributes.TermAttribute
import org.apache.lucene.analysis.standard.StandardTokenizer

import scala.xml.XML


import SqliteWrapper._
import Disambiguator._

class DisambiguatorTest extends FunSuite
{
    test("Efficient disambiguator test")
    {
        val testFileName = "./src/test/scala/data/simpleTest.txt"
        val testDbName = "disambig.sqlite3"
        
        /*val tokenizer = new StandardTokenizer( LUCENE_30, new BufferedReader( new FileReader( testFileName ) ) )
        var run = true
        //val wordList = new ArrayBuffer[String]
        
        while ( run )
        {
            val term = tokenizer.getAttribute(classOf[TermAttribute]).term()
            if ( term != "" )
            {
                wordList.append( term )
            }
            run = tokenizer.incrementToken()
        }
        tokenizer.close()*/
        
        //var wordList = "on"::"the"::"first"::"day"::"of"::"christmas"::Nil
        var wordList = "saudi" :: "arabian" :: "oil" :: "in":: "the" :: "desert" :: Nil
        
        val disambiguator = new Disambiguator( wordList, new SQLiteWrapper( new File(testDbName) ) )
        disambiguator.build()
        disambiguator.resolve()
    }
    
    /*class PhraseTracker( val db : SQLiteWrapper, val startIndex : Int )
    {
        var hasRealWords = false
        var parentId = -1L
        
        val wordQuery = db.prepare( "SELECT id FROM words WHERE name=?", Col[Int]::HNil )
        val phraseQuery = db.prepare( "SELECT id FROM phraseTreeNodes WHERE parentId=? AND wordId=?", Col[Int]::HNil )
        val topicQuery = db.prepare( "SELECT t1.id FROM topics AS t1 INNER JOIN phraseTopics AS t2 ON t1.id=t2.topicId WHERE t2.phraseTreeNodeId=?", Col[Int]::HNil )
        
        def update( rawWord : String, currIndex : Int ) : (Boolean, Int, Int, List[Int]) =
        {
            val word = rawWord.toLowerCase()
            if ( !StopWords.stopWordSet.contains( word ) )
            {
                hasRealWords = true
            }
            
            var success = false
            var topics = List[Int]()

			wordQuery.bind( word )
            if ( wordQuery.step() )
            {
                val wordId = _1(wordQuery.row).get
                
                phraseQuery.bind( parentId, wordId )
                if ( phraseQuery.step() )
                {
                    val currentId = _1(phraseQuery.row).get
                    if ( hasRealWords )
                    {
                        topicQuery.bind(currentId)
                        while ( topicQuery.step() )
                        {
                            val topicId = _1(topicQuery.row).get
                            topics = topicId :: topics
                        }
                        topicQuery.reset()
                    }
                    
                    parentId = currentId
                    success = true
                }
                phraseQuery.reset()
            }
            wordQuery.reset()
            
            val result = (success, startIndex, currIndex, topics)
            return result
        }
    }

    test("Monbiot disambiguator test")
    {
        if ( false )
        {
            //val testFileName = "./src/test/scala/data/monbiotTest.txt"
            val testFileName = "./src/test/scala/data/simpleTest.txt"
            val testDbName = "disambig.sqlite3"
            val testOutName = "out.html"
            
            val tokenizer = new StandardTokenizer( LUCENE_30, new BufferedReader( new FileReader( testFileName ) ) )
            
            var run = true
            val wordList = new ArrayBuffer[String]
            while ( run )
            {
                wordList.append( tokenizer.getAttribute(classOf[TermAttribute]).term() )
                run = tokenizer.incrementToken()
            }
            tokenizer.close() 
            
            val db = new SQLiteWrapper( new File(testDbName) )
            
            var topicList = List[(Int, Int, List[Int])]()
            
            var openQueryList = List[PhraseTracker]()
            var wordIndex = 0
            for ( word <- wordList )
            {
                openQueryList = new PhraseTracker(db, wordIndex) :: openQueryList
                
                var newList = List[PhraseTracker]()
                for ( query <- openQueryList )
                {
                    val (notTerminal, startIndex, endIndex, topicIds) = query.update(word, wordIndex)
                    if ( notTerminal )
                    {
                        newList = query :: newList
                    }
                    
                    if ( topicIds != Nil )
                    {
                        //println( ":: " + wordList.slice(startIndex,endIndex+1) )
                        topicList = (startIndex, endIndex, topicIds)::topicList
                    }
                }
                openQueryList = newList
                wordIndex += 1
            }
            
            // startIndex, endIndex, topicId
            topicList = topicList.sortWith( (a, b) => if ( a._1 == b._1 ) a._2 > b._2 else a._1 < b._1 )
            for ( (startIndex, endIndex, topicIds) <- topicList )
            {
                println( "++ " + wordList.slice(startIndex, endIndex+1) )
            }
            
            val bannedRegex = new Regex("[0-9]{4}")
            val categoryNameQuery = db.prepare("SELECT name FROM categories WHERE id=?", Col[String]::HNil)
            val categoryMembershipQuery = db.prepare( "SELECT categoryId FROM categoryMembership WHERE topicId=?", Col[Int]::HNil )
            var allTokens = List[DisambiguationAlternative]()
            for ( (startIndex, endIndex, topicIds) <- topicList )
            {
                var topicDetails = List[TopicDetails]()
                for ( topicId <- topicIds )
                {
                    var categorySet = TreeSet[Int]()
                    categoryMembershipQuery.bind(topicId)
                    while (categoryMembershipQuery.step() )
                    {
                        val categoryId = _1(categoryMembershipQuery.row).get
                        categoryNameQuery.bind(categoryId)
                        categoryNameQuery.step()
                        val categoryName = _1(categoryNameQuery.row).get
                        categoryNameQuery.reset()
                        
                        bannedRegex.findFirstIn( categoryName ) match
                        {
                            case None => categorySet += categoryId
                            case _ => println( "/// Punting banned category " + categoryName )
                        }
                        
                    }
                    categoryMembershipQuery.reset()
                    
                    topicDetails = new TopicDetails(topicId, categorySet)::topicDetails
                }
                val nextDA = new DisambiguationAlternative( startIndex, endIndex, topicDetails )
                if ( allTokens != Nil && allTokens.head.overlaps( nextDA ) )
                {
                    allTokens.head.addAlternative( nextDA )
                }
                else
                {
                    allTokens = nextDA :: allTokens
                }
            }
            
            
            def getCategoryCounts( allTokens : List[DisambiguationAlternative] ) =
            {
                val categoryCount = new TreeMap[Int, Int]()
                var count = 0
                for ( alternatives <- allTokens )
                {
                    val tokenCategoryIds = alternatives.allCategoryIds()
                    val topicAlternatives = alternatives.numTopicAlternatives()
                    println( count + "  " + tokenCategoryIds.size + " " + topicAlternatives )
                    count += 1
                    
                    for ( categoryId <- tokenCategoryIds )
                    {
                        if ( !categoryCount.containsKey(categoryId) )
                        {
                            categoryCount.put(categoryId, 0)
                        }
                        categoryCount.put(categoryId, categoryCount.get(categoryId) + 1 )
                    }
                }
                categoryCount
            }
            
            val categoryCounts = getCategoryCounts( allTokens )
            
            
            
            var sortedCategoryList = List[(Int, Int)]()
            
            val mapIt = categoryCounts.entrySet().iterator()
            while ( mapIt.hasNext() )
            {
                val next = mapIt.next()
                val categoryId = next.getKey()
                val count = next.getValue()
                
                sortedCategoryList = (count, categoryId) :: sortedCategoryList
            }
            
            // Sort so that most numerous categories come first
            sortedCategoryList = sortedCategoryList.sortWith( _._1 > _._1 ) 
            
            println( "Number of words: " + wordList.length )
            println( "Number of categories before: " + categoryCounts.size() )
            println( "DA sites before: " + allTokens.length )
            
            // TODO: Iterate over categories asserting them one by one
            for ( (count, categoryId) <- sortedCategoryList )
            {
                if ( count > 1 )
                {
                    allTokens = allTokens.filter( _.assertCategory( categoryId ) )
                }
            }
            
            
            val newCategoryCounts = getCategoryCounts( allTokens )
            println( "Number of categories after: " + newCategoryCounts.size() )
            println( "DA sites after pruning: " + allTokens.length )
            
            {
                val mapIt = newCategoryCounts.entrySet().iterator()
                while ( mapIt.hasNext() )
                {
                    val next = mapIt.next()
                    val categoryId = next.getKey()
                    val count = next.getValue()
                    
                    categoryNameQuery.bind()
                    categoryNameQuery.step()
                    println( "|| " + count + ": " + _1(categoryNameQuery.row).get )
                    categoryNameQuery.reset()
                }
            }
            val topicNameQuery = db.prepare("SELECT name FROM topics WHERE id=?", Col[String]::HNil )
            var count = 0
            for ( alternatives <- allTokens )
            {
                val tokenCategoryIds = alternatives.allCategoryIds()
                val topicAlternatives = alternatives.numTopicAlternatives()
                val topicDetails = alternatives.getTopicDetails()
                
                var topicNames = List[String]()
                for ( topicDetail <- topicDetails )
                {
                    topicNameQuery.bind(topicDetail.topicId)
                    topicNameQuery.step()
                    topicNames = _1(topicNameQuery.row).get :: topicNames
                    topicNameQuery.reset()
                }
                
                println( wordList.slice(alternatives.startIndex, alternatives.endIndex+1).toString() + ": " + count + "  " + tokenCategoryIds.size + " " + topicAlternatives + " " + topicNames )
                count += 1
            }
            
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
            
            db.dispose()
        }
    }*/
    
    
    test( "Disambiguation alternative test 1" )
    {
        val test = List( List(1), List(1,2), List(1,2,3) )
        
        
    }
    
    test( "Phrase topic combination test" )
    {
        //                  0      1       2       3      4        5        6       7       8      9      10    11
        val phrase = List( "on", "the", "first", "day", "of", "christmas", "my", "true", "love", "sent", "to", "me" )
        val topics = List( List(1,2,3), List(1,2,3,4,5), List(3,4,5), List(5), List(6,7,8), List(7,8) )
        
        
    }
}


