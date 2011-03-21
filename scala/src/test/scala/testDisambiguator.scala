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
   
    final class SQLiteWrapper( fileName : String )
    {
        val conn = new SQLiteConnection( new File( fileName ) )
        conn.open()
        
        def exec( statement : String )
        {
            conn.exec( statement )
        }
        
        def prepare( query : String ) =
        {
            new PreparedStatement(query)
        }
        
        // All tuples inherit from product
        final class PreparedStatement( query : String )
        {
            val statement = conn.prepare( query )
            
            final class Column( index : Int )
            {
            }
            
            implicit def toInt( v : Column ) : Int = 4//v.conn.columnInt( v.index )
            
            private def bindRec( index : Int, params : List[Any] )
            {
                println( "Value " + params.head )
                // TODO: Does this need a pattern match?
                params.head match
                {
                    case v : Int => statement.bind( index, v )
                    case v : String => statement.bind( index, v )
                    case v : Double => statement.bind( index, v )
                    case _ => throw new ClassCastException( "Unsupported type in bind." )
                }
                
                if ( params.tail != Nil )
                {
                    bindRec( index+1, params.tail )
                }
            }
            
            def bind( args : Any* )
            {
                bindRec( 1, args.toList )
            }
            
            def exec( args : Any* )
            {
                bindRec( 1, args.toList )
                step()
                reset()
            }
            
            def reset()
            {
                statement.reset()
            }
            
            // Replace this hideousness with some HList joy if applicable
            def step() : Boolean =
            {
                return statement.step()
            }
            
            def col( index : Int ) = new Column(index)
        }
    }
    
    test("SQLite wrapper test")
    {
        val db = new SQLiteWrapper( "test.sqlite" )
        db.exec( "BEGIN" )
        db.exec( "CREATE TABLE test( number INTEGER, value FLOAT, name TEXT )" )
        
        val insStatement = db.prepare( "INSERT INTO test VALUES( ?, ?, ? )" )
        insStatement.exec( 1, 5.0, "Hello1" )
        insStatement.exec( 2, 6.0, "Hello2" )
        insStatement.exec( 3, 7.0, "Hello3" )
        insStatement.exec( 4, 8.0, "Hello4" )
        
        val getStatement = db.prepare( "SELECT * from test" )
        assert( getStatement.step() === true )
        
        //val t2 : (Int, Double, Text) = getStatement.row()
        
        getStatement.reset()
        
        db.exec( "ROLLBACK" )
    }
    
    
    test("Efficient disambiguator test")
    {
        val testFileName = "./src/test/scala/data/simpleTest.txt"
        val testDbName = "disambig.sqlite3"
        
        val tokenizer = new StandardTokenizer( LUCENE_30, new BufferedReader( new FileReader( testFileName ) ) )
        var run = true
        val wordList = new ArrayBuffer[String]
        while ( run )
        {
            val term = tokenizer.getAttribute(classOf[TermAttribute]).term()
            if ( term != "" )
            {
                wordList.append( term )
            }
            run = tokenizer.incrementToken()
        }
        tokenizer.close()
        
        println( wordList.toString() )
        
        val db = new SQLiteConnection( new File(testDbName) )
        db.open()
        
        db.exec( "BEGIN" )
        
        // Will need some indices
        db.exec( "CREATE TEMPORARY TABLE textWords( id INTEGER PRIMARY KEY, wordId INTEGER )" )
        db.exec( "CREATE TEMPORARY TABLE phraseLinks( level INTEGER, twId INTEGER, phraseTreeNodeId INTEGER )" )
        db.exec( "CREATE TEMPORARY TABLE phrasesAndTopics( startIndex INTEGER, endIndex INTEGER, topicId INTEGER )" )
        db.exec( "CREATE TEMPORARY TABLE topicCategories( topicId INTEGER, categoryId INTEGER, topicName TEXT, categoryName TEXT )" )
        db.exec( "CREATE INDEX phraseLinkLevel ON phraseLinks(level)" )
        
        //phraseTreeNodes( id INTEGER PRIMARY KEY, parentId INTEGER, wordId INTEGER )
        val insertQuery = db.prepare( "INSERT INTO textWords VALUES( NULL, (SELECT id FROM words WHERE name=?) )" )
        for ( word <- wordList )
        {
            insertQuery.bind(1, word)
            insertQuery.step()
            insertQuery.reset()
        }
        
        db.exec( "INSERT INTO phraseLinks SELECT 0, t1.id, t2.id FROM textWords AS t1 INNER JOIN phraseTreeNodes AS t2 ON t1.wordId=t2.wordId WHERE t2.parentId=-1" )
        println( "--> " + wordList.length + " " + db.getChanges() )
        
        // Will need to count how many rows inserted and when zero, stop running.
        var running = true
        var level = 1
        while (running)
        {
            var updateQuery = db.prepare( "INSERT INTO phraseLinks SELECT ?1, t1.twId+1, t3.id FROM phraseLinks AS t1 INNER JOIN textWords AS t2 ON t2.id=t1.twId+1 INNER JOIN phraseTreeNodes AS t3 ON t3.parentId=t1.phraseTreeNodeId AND t3.wordId=t2.wordId WHERE t1.level=?1-1" )
            updateQuery.bind(1, level)
            updateQuery.step()
            updateQuery.reset()
            
            val numChanges = db.getChanges()
            println( "--> " + level + " " + numChanges )
            if ( numChanges == 0 ) running = false
            
            level += 1
        }
        
        // get wordId from 
        //db.exec( "INSERT INTO phraseLinks (SELECT level, t1.id+1, t2.id FROM textWords AS t1 INNER JOIN phraseTreeNodes AS t2 ON 
        
        println( "Crosslinking topics" )
        db.exec( "INSERT INTO phrasesAndTopics SELECT t1.twId-t1.level, t1.twId, t2.topicId FROM phraseLinks AS t1 INNER JOIN phraseTopics AS t2 ON t1.phraseTreeNodeId=t2.phraseTreeNodeId ORDER BY t1.twId-t1.level, t1.twId" )
        println( "Crosslinking categories" )
        db.exec( "INSERT INTO topicCategories SELECT DISTINCT t1.topicId, t2.categoryId, t3.name, t4.name FROM phrasesAndTopics AS t1 INNER JOIN categoryMembership AS t2 ON t1.topicId=t2.topicId INNER JOIN topics AS t3 ON t1.topicId=t3.id INNER JOIN categories AS t4 on t2.categoryId=t4.id" )
        println( "Complete" )
        
        val getPhrases = db.prepare( "SELECT DISTINCT startIndex, endIndex FROM phrasesAndTopics" )
        while ( getPhrases.step() )
        {
            val fromIndex = getPhrases.columnInt(0)-1
            val toIndex = getPhrases.columnInt(1)-1
            println( ":: " + fromIndex + " " + toIndex + " " + wordList.slice(fromIndex, toIndex+1) )
        }
        getPhrases.reset()

        db.exec( "ROLLBACK" )
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


