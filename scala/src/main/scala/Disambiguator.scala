import scala.collection.immutable.TreeSet
import java.util.TreeMap

import scala.util.matching.Regex

import SqliteWrapper._

object Disambiguator
{
    class TopicDetails( val topicId : Int, var categoryIds : TreeSet[Int] )
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

    class Disambiguator( val wordList : List[String], db : SQLiteWrapper )
    {
        var daSites = List[DisambiguationAlternative]()
        var topicNameMap = new TreeMap[Int, String]()
        var categoryNameMap = new TreeMap[Int, String]()
        
        def build()
        {
            db.exec( "BEGIN" )
            
            // Will need some indices
            db.exec( "CREATE TEMPORARY TABLE textWords( id INTEGER PRIMARY KEY, wordId INTEGER )" )
            db.exec( "CREATE TEMPORARY TABLE phraseLinks( level INTEGER, twId INTEGER, phraseTreeNodeId INTEGER )" )
            db.exec( "CREATE TEMPORARY TABLE phrasesAndTopics( startIndex INTEGER, endIndex INTEGER, topicId INTEGER )" )
            db.exec( "CREATE TEMPORARY TABLE topicCategories( topicId INTEGER, categoryId INTEGER, topicName TEXT, categoryName TEXT )" )
            db.exec( "CREATE INDEX phraseLinkLevel ON phraseLinks(level)" )
            
            val insertQuery = db.prepare( "INSERT INTO textWords VALUES( NULL, (SELECT id FROM words WHERE name=?) )", HNil )
            for ( word <- wordList )
            {
                insertQuery.exec( word )
            }
            
            println( "Building phrase table.")
            db.exec( "INSERT INTO phraseLinks SELECT 0, t1.id, t2.id FROM textWords AS t1 INNER JOIN phraseTreeNodes AS t2 ON t1.wordId=t2.wordId WHERE t2.parentId=-1" )
            
            // Will need to count how many rows inserted and when zero, stop running.
            var running = true
            var level = 1
            while (running)
            {
                var updateQuery = db.prepare( "INSERT INTO phraseLinks SELECT ?1, t1.twId+1, t3.id FROM phraseLinks AS t1 INNER JOIN textWords AS t2 ON t2.id=t1.twId+1 INNER JOIN phraseTreeNodes AS t3 ON t3.parentId=t1.phraseTreeNodeId AND t3.wordId=t2.wordId WHERE t1.level=?1-1", HNil )
                updateQuery.exec(level)
                
                val numChanges = db.getChanges()
                if ( numChanges == 0 ) running = false
                
                level += 1
            }
            
            println( "Crosslinking topics." )
            db.exec( "INSERT INTO phrasesAndTopics SELECT t1.twId-t1.level, t1.twId, t2.topicId FROM phraseLinks AS t1 INNER JOIN phraseTopics AS t2 ON t1.phraseTreeNodeId=t2.phraseTreeNodeId ORDER BY t1.twId-t1.level, t1.twId" )
            println( "Crosslinking categories." )
            db.exec( "INSERT INTO topicCategories SELECT DISTINCT t1.topicId, t2.categoryId, t3.name, t4.name FROM phrasesAndTopics AS t1 INNER JOIN categoryMembership AS t2 ON t1.topicId=t2.topicId INNER JOIN topics AS t3 ON t1.topicId=t3.id INNER JOIN categories AS t4 on t2.categoryId=t4.id" )
            println( "Building index." )
            db.exec( "CREATE INDEX topicCategoriesIndex ON topicCategories(topicId)" )
            
            println( "Retrieving data." )
            val getPhrases = db.prepare( "SELECT t1.startIndex-1, t1.endIndex-1, t1.topicId, t2.categoryId, t2.categoryName FROM phrasesAndTopics AS t1 INNER JOIN topicCategories AS t2 ON t1.topicId=t2.topicId ORDER BY t1.endIndex DESC, t1.startIndex ASC, t1.topicId, t2.categoryId", Col[Int]::Col[Int]::Col[Int]::Col[Int]::Col[String]::HNil )
            
            println( "Building disambiguation alternative forest." )
            val bannedRegex = new Regex("[0-9]{4}")
            var currDA : DisambiguationAlternative = null
            var topicDetails : TopicDetails = null
            while ( getPhrases.step() )
            {
                val startIndex      = _1( getPhrases.row ).get
                val endIndex        = _2( getPhrases.row ).get
                val topicId         = _3( getPhrases.row ).get
                val categoryId      = _4( getPhrases.row ).get
                val categoryName    = _5( getPhrases.row ).get
                
                if ( currDA == null || currDA.startIndex != startIndex || currDA.endIndex != endIndex )
                {
                    currDA = new DisambiguationAlternative( startIndex, endIndex, List[TopicDetails]() )
                    
                    if ( daSites == Nil || !daSites.head.overlaps( currDA ) )
                    {
                        daSites = currDA :: daSites
                    }
                    else
                    {
                        daSites.head.addAlternative( currDA )
                    }
                }
                
                if ( currDA.topicDetails == Nil || currDA.topicDetails.head.topicId != topicId )
                {
                    currDA.topicDetails = new TopicDetails( topicId, new TreeSet[Int]() )::currDA.topicDetails
                }
                val curTopicDetails = currDA.topicDetails.head
                
                println( "KJKJK::: " + categoryName )
                bannedRegex.findFirstIn( categoryName ) match
                {
                    case None => curTopicDetails.categoryIds = curTopicDetails.categoryIds + categoryId
                    case _ =>
                }
            }
            getPhrases.reset()
            println( "  complete..." )
            
            for ( da <- daSites )
            {
                println( da.startIndex, da.endIndex, da.topicDetails.length )
            }

            println( "Pulling topic and category names." )
            val topicNameQuery = db.prepare( "SELECT DISTINCT topicId, topicName FROM topicCategories", Col[Int]::Col[String]::HNil )
            while ( topicNameQuery.step() )
            {
                val id = _1(topicNameQuery.row).get
                val name = _2(topicNameQuery.row).get
                topicNameMap.put(id, name)
            }
            topicNameQuery.reset()
            
            val categoryNameQuery = db.prepare( "SELECT DISTINCT categoryId, categoryName FROM topicCategories", Col[Int]::Col[String]::HNil )
            while ( categoryNameQuery.step() )
            {
                val id = _1(categoryNameQuery.row).get
                val name = _2(categoryNameQuery.row).get
                categoryNameMap.put(id, name)
            }
            categoryNameQuery.reset()

            db.exec( "ROLLBACK" )
            db.dispose()
        }

        private def getCategoryCounts( daSites : List[DisambiguationAlternative] ) =
        {
            val categoryCount = new TreeMap[Int, Int]()
            var count = 0
            for ( alternatives <- daSites )
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
        
        def resolve()
        {
            val categoryCounts = getCategoryCounts( daSites )
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
            println( "DA sites before: " + daSites.length )
            
            // TODO: Iterate over categories asserting them one by one
            for ( (count, categoryId) <- sortedCategoryList )
            {
                if ( count > 1 )
                {
                    daSites = daSites.filter( _.assertCategory( categoryId ) )
                }
            }
            
            
            
            val newCategoryCounts = getCategoryCounts( daSites )
            println( "Number of categories after: " + newCategoryCounts.size() )
            println( "DA sites after pruning: " + daSites.length )
            
            {
                val mapIt = newCategoryCounts.entrySet().iterator()
                while ( mapIt.hasNext() )
                {
                    val next = mapIt.next()
                    val categoryId = next.getKey()
                    val count = next.getValue()
                    
                    println( "|| " + count + ": " + categoryNameMap.get(categoryId) )
                }
            }
            var count = 0
            for ( alternatives <- daSites )
            {
                val tokenCategoryIds = alternatives.allCategoryIds()
                val topicAlternatives = alternatives.numTopicAlternatives()
                val topicDetails = alternatives.getTopicDetails()
                
                val topicNames = topicDetails.map( x => topicNameMap.get( x.topicId ) )
                
                println( wordList.slice(alternatives.startIndex, alternatives.endIndex+1).toString() + ": " + count + "  " + tokenCategoryIds.size + " " + topicAlternatives + " " + topicNames )
                count += 1
            }
        }
    }
}

