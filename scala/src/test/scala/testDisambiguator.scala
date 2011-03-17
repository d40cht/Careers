import org.scalatest.FunSuite

import java.io.{File, BufferedReader, FileReader}
import scala.collection.mutable.ArrayBuffer
import com.almworks.sqlite4java._

import org.apache.lucene.util.Version.LUCENE_30
import org.apache.lucene.analysis.Token
import org.apache.lucene.analysis.tokenattributes.TermAttribute
import org.apache.lucene.analysis.standard.StandardTokenizer

import scala.xml.XML
import scala.collection.immutable.TreeSet
import java.util.TreeMap

import scala.util.matching.Regex

class DisambiguatorTest extends FunSuite
{

    class PhraseTracker( val db : SQLiteConnection, val startIndex : Int )
    {
        var hasRealWords = false
        var parentId = -1L
        
        val wordQuery = db.prepare( "SELECT id FROM words WHERE name=?" )
        val phraseQuery = db.prepare( "SELECT id FROM phraseTreeNodes WHERE parentId=? AND wordId=?" )
        val topicQuery = db.prepare( "SELECT t1.id FROM topics AS t1 INNER JOIN phraseTopics AS t2 ON t1.id=t2.topicId WHERE t2.phraseTreeNodeId=?" )
        
        def update( rawWord : String, currIndex : Int ) : (Boolean, Int, Int, List[Int]) =
        {
            val word = rawWord.toLowerCase()
            if ( !StopWords.stopWordSet.contains( word ) )
            {
                hasRealWords = true
            }
            
            var success = false
            var topics = List[Int]()

            wordQuery.bind(1, word)
            if ( wordQuery.step() )
            {
                val wordId = wordQuery.columnInt(0)
                
                phraseQuery.bind(1, parentId)
                phraseQuery.bind(2, wordId)
                if ( phraseQuery.step() )
                {
                
                    val currentId = phraseQuery.columnInt(0)
                    if ( hasRealWords )
                    {
                        topicQuery.bind(1, currentId)
                        while ( topicQuery.step() )
                        {
                            val topicId = topicQuery.columnInt(0)
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
    
    class TopicDetails( val topicId : Int, val categoryIds : TreeSet[Int] )
    {
    }
    
    class DisambiguationAlternative( val startIndex : Int, val endIndex : Int, var topicDetails : List[TopicDetails] )
    {
        var children = List[DisambiguationAlternative]()
        
        def overlaps( da : DisambiguationAlternative ) : Boolean =
        {
            (startIndex >= da.startIndex && endIndex <= da.endIndex) ||
            (da.startIndex >= startIndex && da.endIndex <= endIndex)
        }
        
        def addAlternative( da : DisambiguationAlternative )
        {
            assert( overlaps( da ) )
            var allOverlap = true
            for ( child <- children )
            {
                if ( child.overlaps(da) )
                {
                    child.addAlternative(da)
                }
                else
                {
                    allOverlap = false
                }
            }
            if ( !allOverlap )
            {
                children = da :: children
            }
        }
        
        def numTopicAlternatives() : Int =
        {
            topicDetails.length + children.foldLeft(0)( _ + _.numTopicAlternatives() )
        }
        
        def getTopicDetails() : List[TopicDetails] =
        {
            topicDetails ++ children.foldLeft(List[TopicDetails]())( _ ++ _.getTopicDetails() )
        }
        
        def allCategoryIds() : TreeSet[Int] =
        {
            var categoryIds = TreeSet[Int]()
            for ( topicDetail <- topicDetails )
            {
                categoryIds = categoryIds ++ topicDetail.categoryIds
            }
            for ( child <- children )
            {
                categoryIds = categoryIds ++ child.allCategoryIds()
            }
            return categoryIds
        }
        
        def containsCategory( categoryId : Int ) : Boolean =
        {
            for ( td <- topicDetails )
            {
                if ( td.categoryIds.contains( categoryId ) )
                {
                    return true
                }
            }
            for ( child <- children )
            {
                if ( child.containsCategory( categoryId ) )
                {
                    return true
                }
            }
            
            return false
        }
        
        // Prune any topics which don't contain this category
        private def assertCategoryImpl( assertedCategoryId : Int ) : Boolean =
        {
            var categoryAlive = false
            topicDetails = topicDetails.filter( _.categoryIds.contains(assertedCategoryId) )
            
            if ( topicDetails != Nil ) categoryAlive = true
            
            children = children.filter( _.assertCategory( assertedCategoryId ) )
            
            if ( children != Nil ) categoryAlive = true
            
            return categoryAlive
        }
        
        def assertCategory( assertedCategoryId : Int ) : Boolean =
        {
            // If we don't contain this category then it's not relevant to us and we don't change
            if ( !containsCategory( assertedCategoryId ) )
            {
                return true
            }
            else
            {
                return assertCategoryImpl( assertedCategoryId )
            }
        }
    }

    test("Monbiot disambiguator test")
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
        
        val db = new SQLiteConnection( new File(testDbName) )
        db.open()
        
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
        
        topicList = topicList.sortWith( (a, b) => if ( a._1 == b._1 ) a._2 > b._2 else a._1 < b._1 )
        for ( (startIndex, endIndex, topicIds) <- topicList )
        {
            println( "++ " + wordList.slice(startIndex, endIndex+1) )
        }
        
        val bannedRegex = new Regex("[0-9]{4}")
        val categoryNameQuery = db.prepare("SELECT name FROM categories WHERE id=?")
        val categoryMembershipQuery = db.prepare( "SELECT categoryId FROM categoryMembership WHERE topicId=?" )
        var allTokens = List[DisambiguationAlternative]()
        for ( (startIndex, endIndex, topicIds) <- topicList )
        {
            var topicDetails = List[TopicDetails]()
            for ( topicId <- topicIds )
            {
                var categorySet = TreeSet[Int]()
                categoryMembershipQuery.bind(1, topicId)
                while (categoryMembershipQuery.step() )
                {
                    val categoryId = categoryMembershipQuery.columnInt(0)
                    categoryNameQuery.bind(1, categoryId)
                    categoryNameQuery.step()
                    val categoryName = categoryNameQuery.columnString(0)
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
                
                categoryNameQuery.bind(1, categoryId)
                categoryNameQuery.step()
                println( "|| " + count + ": " + categoryNameQuery.columnString(0) )
                categoryNameQuery.reset()
            }
        }
        val topicNameQuery = db.prepare("SELECT name FROM topics WHERE id=?" )
        var count = 0
        for ( alternatives <- allTokens )
        {
            val tokenCategoryIds = alternatives.allCategoryIds()
            val topicAlternatives = alternatives.numTopicAlternatives()
            val topicDetails = alternatives.getTopicDetails()
            
            var topicNames = List[String]()
            for ( topicDetail <- topicDetails )
            {
                topicNameQuery.bind(1, topicDetail.topicId)
                topicNameQuery.step()
                topicNames = topicNameQuery.columnString(0) :: topicNames
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
        
        //println( wordList.reverse.toString )
        
        /*val dbFileName = 
        val db = new SQLiteConnection( new File(dbFileName) )
        
        val topicQuery = db.prepare( "SELECT t2.id, t2.name, t3.categoryId, t4.name FROM surfaceForms AS t1 INNER JOIN topics AS t2 ON t1.topicId=t2.id INNER JOIN categoryMembership AS t3 ON t1.topicId=t3.topicId INNER JOIN categories AS t4 ON t3.categoryId=t4.id WHERE t1.name=? ORDER BY t2.id, t3.categoryId" )*/
    }
    
    
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


