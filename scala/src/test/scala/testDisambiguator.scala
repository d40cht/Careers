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
    
    class DisambiguationAlternative( val startIndex : Int, val endIndex : Int, topicIds : List[Int] )
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
        
        def allCategoryIds( getCategoriesFn : Int => List[Int] ) : TreeSet[Int] =
        {
            var categoryIds = TreeSet[Int]()
            for ( topicId <- topicIds )
            {
                categoryIds = categoryIds ++ getCategoriesFn(topicId)
            }
            for ( child <- children )
            {
                categoryIds = categoryIds ++ child.allCategoryIds( getCategoriesFn )
            }
            return categoryIds
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
        
        
        var allTokens = List[DisambiguationAlternative]()
        for ( (startIndex, endIndex, topicIds) <- topicList )
        {
            val nextDA = new DisambiguationAlternative( startIndex, endIndex, topicIds )
            if ( allTokens != Nil && allTokens.head.overlaps( nextDA ) )
            {
                allTokens.head.addAlternative( nextDA )
            }
            else
            {
                allTokens = nextDA :: allTokens
            }
        }
        
        
        
        val categoryQuery = db.prepare( "SELECT categoryId FROM categoryMembership WHERE topicId=?" )
        var count = 0
        
        val categoryCount = new TreeMap[Int, Int]()
        for ( alternatives <- allTokens )
        {
            count += 1
            val tokenCategoryIds = alternatives.allCategoryIds( (topicId : Int) =>
            {
                var categoryIds = List[Int]()
                categoryQuery.bind(1, topicId)
                while ( categoryQuery.step() )
                {
                    val categoryId = categoryQuery.columnInt(0)
                    categoryIds = categoryId :: categoryIds
                }
                categoryQuery.reset()
                
                categoryIds
            } )
            
            for ( categoryId <- tokenCategoryIds )
            {
                if ( !categoryCount.containsKey(categoryId) )
                {
                    categoryCount.put(categoryId, 0)
                }
                categoryCount.put(categoryId, categoryCount.get(categoryId) + 1 )
            }
            println( count + "  " + tokenCategoryIds.size )
        }
        
        
        var sortedCategoryList = List[(Int, String)]()
        val categoryNameQuery = db.prepare("SELECT name FROM categories WHERE id=?")
        val mapIt = categoryCount.entrySet().iterator()
        while ( mapIt.hasNext() )
        {
            val next = mapIt.next()
            val categoryId = next.getKey()
            
            categoryNameQuery.bind(1, categoryId)
            categoryNameQuery.step()
            val categoryName = categoryNameQuery.columnString(0)
            categoryNameQuery.reset()
            
            val count = next.getValue()
            
            //println( "Category: " + categoryName + " " + count )
            sortedCategoryList = (count, categoryName) :: sortedCategoryList
        }
        sortedCategoryList = sortedCategoryList.sortWith( _._1 < _._1 )
        
        for ( (count, name) <- sortedCategoryList )
        {
            println( "## " + count + " : " + name )
        }
        
        println( "Number of words: " + wordList.length )
        println( "DA sites: " + allTokens.length )
        
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


