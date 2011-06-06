package org.seacourt.disambiguator

import scala.collection.immutable.{TreeSet, TreeMap}
import java.util.{TreeMap => JTreeMap}
import math.{max, log}
import java.io.{File, DataInputStream, FileInputStream}

import scala.util.matching.Regex

import org.seacourt.sql.SqliteWrapper._
import org.seacourt.utility.StopWords.stopWordSet
import org.seacourt.utility._

// Thoughts:
//
// * Word frequency - should that be tf/idf on words, or td/idf on phrases that are surface forms?
// * Testing: are the various tables actually correct. Are there really 55331 articles that reference 'Main:Voivodeships of Poland'?
// *    Indeed, are there really 466132 articles that mention the word 'voivodeships'?
// * Get some test documents and test disambig at the paragraph level, then at the doc level and compare the results
// * Mark up the test documents manually with expected/desired results
// * Get stuff up in the scala REPL to test ideas
// * Count at each site whenever an assertion has been applied (i.e. the category was relevant). Disable sites where only a few were relevant? Or something else!



// Richard Armitage, 'The Vulcans', Scooter Libby


object Disambiguator
{
	val bannedRegex = new Regex("[0-9]{4}")
	
	val bannedCategories = TreeSet(
	    "Category:Living people",
	    "Category:Greek loanwords",
	    "Category:Unprintworthy redirects",
	    "Main:British Phonographic Industry",
	    "Category:Protected redirects" )
	
	def validCategory ( categoryName : String ) =
	{
		var valid = true
		if ( bannedCategories.contains( categoryName ) ) valid = false
		
	    bannedRegex.findFirstIn( categoryName ) match
		{
			case None =>
			case _ => valid = false
		}
		
		valid
	}
	
	def validPhrase( words : List[String] ) = words.foldLeft( false )( _ || !stopWordSet.contains(_) )
	
    class TopicDetails( val topicId : Int, val linkCount : Int, var categoryIds : TreeSet[Int], val topicName : String )
    {
        var score = 0.0
        
        def assertCategory( categoryId : Int, categoryWeight : Double )
        {
            if ( categoryIds.contains( categoryId ) ) score += categoryWeight
        }
    }

    class DisambiguationAlternative( val startIndex : Int, val endIndex : Int, val validPhrase : Boolean, val phraseWeight : Double, val phrase : List[String] )
    {
        var topicDetails = List[TopicDetails]()
        var children = List[DisambiguationAlternative]()
        var topicLinkCount = 0
        
        def overlaps( da : DisambiguationAlternative ) =
            (startIndex >= da.startIndex && endIndex <= da.endIndex) ||
            (da.startIndex >= startIndex && da.endIndex <= endIndex)

        def reportAlternatives() : List[(Double, List[String], String)] =
        {
            var res = List[(Double, List[String], String)]()
            for ( topic <- topicDetails )
            {
                res = (topic.score * phraseWeight, phrase, topic.topicName) :: res
            }
            
            children.foldLeft( res )( _ ++ _.reportAlternatives() )
        }
           
        def addTopicDetails( td : TopicDetails )
        {
            topicDetails = td::topicDetails
            topicLinkCount += td.linkCount
        }
        
        def addAlternative( da : DisambiguationAlternative )
        {
            assert( overlaps( da ) )
            
            val allOverlap = children.foldLeft( true )( _ && _.overlaps(da) )
            
            if ( allOverlap )
            {
            	children = da :: children
            }
            else
            {
				for ( child <- children )
				{
				    if ( child != da && child.overlaps(da) )
				    {
				        child.addAlternative(da)
				    }
				}
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
        
        def weightedCategories( localWeights : JTreeMap[Int, Double] )
        {
            for ( topicDetail <- topicDetails )
            {
                val topicWeight = (topicDetail.linkCount+0.0) / (topicLinkCount+0.0)
                for ( categoryId <- topicDetail.categoryIds )
                {
                    val oldWeight = if (localWeights.containsKey(categoryId)) localWeights.get(categoryId) else 0.0
                    localWeights.put( categoryId, max(oldWeight, phraseWeight*topicWeight) )
                }
            }

            for ( child <- children )
            {
                child.weightedCategories( localWeights )
            }
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
            categoryIds
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
            val debugPrunedTopics = topicDetails.filter( !_.categoryIds.contains(assertedCategoryId ) )
            
            var categoryAlive = false
            topicDetails = topicDetails.filter( _.categoryIds.contains(assertedCategoryId) )
            
            if ( topicDetails != Nil ) categoryAlive = true
            if ( debugPrunedTopics != Nil )
            {
                println( "  Pruning " + phrase )
                if ( topicDetails == Nil ) println( "  --> Phrase topics empty. Removed." )
                //for ( pt <- debugPrunedTopics ) println( "    " + pt.topicId )
            }
            
            children = children.filter( _.assertCategory( assertedCategoryId ) )
            
            if ( children != Nil ) categoryAlive = true

            
            categoryAlive
        }
        
        def assertCategory( assertedCategoryId : Int ) = !containsCategory( assertedCategoryId ) || assertCategoryImpl( assertedCategoryId )
        
        def assertCategoryWeighted( categoryId : Int, weight : Double )
        {
            for ( topic <- topicDetails ) topic.assertCategory( categoryId, weight )
            for ( child <- children ) child.assertCategoryWeighted( categoryId, weight )
        }
    }
    
    class Interactive( dbFileName : String )
    {
        val db = new SQLiteWrapper( new File(dbFileName) )
        
        def disambiguate( str : String, maxAlternatives : Int )
        {
            val words = Utils.luceneTextTokenizer( Utils.normalize( str ) )
            val disambiguator = new Disambiguator( words, new SQLiteWrapper( new File(dbFileName) ) )
            disambiguator.build()
            disambiguator.resolve( maxAlternatives )
        }
    }

    class Disambiguator( val wordList : List[String], db : SQLiteWrapper )
    {
        var daSites = List[DisambiguationAlternative]()
        var topicNameMap = new JTreeMap[Int, String]()
        var categoryNameMap = new JTreeMap[Int, String]()
        
        def build()
        {
            db.exec( "BEGIN" )
            
            // Will need some indices
            db.exec( "CREATE TEMPORARY TABLE textWords( id INTEGER PRIMARY KEY, name TEXT, wordId INTEGER, inTopicCount INTEGER )" )
            db.exec( "CREATE TEMPORARY TABLE phraseLinks( level INTEGER, twId INTEGER, phraseTreeNodeId INTEGER )" )
            db.exec( "CREATE TEMPORARY TABLE phrasesAndTopics( startIndex INTEGER, endIndex INTEGER, topicId INTEGER, count INTEGER )" )
            db.exec( "CREATE TEMPORARY TABLE topicCategories( topicId INTEGER, categoryId INTEGER, topicName TEXT, categoryName TEXT )" )
            db.exec( "CREATE INDEX phraseLinkLevel ON phraseLinks(level)" )
            
            val insertQuery = db.prepare( "INSERT INTO textWords VALUES( NULL, ?, (SELECT id FROM words WHERE name=?), (SELECT count FROM words WHERE name=?) )", HNil )
            for ( word <- wordList )
            {
                insertQuery.exec( word, word, word )
            }
            
            val wordWeights = db.prepare( "SELECT 10000000.0 / inTopicCount FROM textWords ORDER BY id", Col[Double]::HNil ).toList
            
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
            db.exec( "INSERT INTO phrasesAndTopics SELECT t1.twId-t1.level, t1.twId, t2.topicId, t2.count FROM phraseLinks AS t1 INNER JOIN phraseTopics AS t2 ON t1.phraseTreeNodeId=t2.phraseTreeNodeId WHERE t2.count > 1 ORDER BY t1.twId-t1.level, t1.twId" )
            
            // TODO: Refactor for new datastructure
            println( "Crosslinking categories." )
            
            db.exec( "INSERT INTO topicCategories SELECT DISTINCT t1.topicId, t2.contextTopicId, t3.name, t4.name FROM phrasesAndTopics AS t1 INNER JOIN categoriesAndContexts AS t2 ON t1.topicId=t2.topicId INNER JOIN topics AS t3 on t1.topicId=t3.id INNER JOIN topics AS t4 ON t2.contextTopicId=t4.id INNER JOIN topicCountAsContext AS t5 ON t2.contextTopicId=t5.topicId WHERE t5.count < 50000 ORDER BY t1.topicId, t2.contextTopicId" )
            
            println( "Building index." )
            db.exec( "CREATE INDEX topicCategoriesIndex ON topicCategories(topicId)" )
            
            println( "Retrieving data." )
            val getPhrases = db.prepare( "SELECT DISTINCT t1.startIndex-1, t1.endIndex-1, t1.topicId, t1.count, t2.categoryId, t2.topicName, t2.categoryName FROM phrasesAndTopics AS t1 INNER JOIN topicCategories AS t2 ON t1.topicId=t2.topicId ORDER BY t1.endIndex DESC, t1.startIndex ASC, t1.topicId, t2.categoryId", Col[Int]::Col[Int]::Col[Int]::Col[Int]::Col[Int]::Col[String]::Col[String]::HNil )
            
            println( "Building disambiguation alternative forest." )
            var currDA : DisambiguationAlternative = null
            var topicDetails : TopicDetails = null
            
            var categories = TreeSet[Int]()
            getPhrases.foreach( row =>
            {
                val startIndex          = _1( row ).get
                val endIndex            = _2( row ).get
                val topicId             = _3( row ).get
                val linkCount           = _4( row ).get
                val categoryId          = _5( row ).get
                val topicName		    = _6( row ).get
                val categoryName        = _7( row ).get
                
                val phraseWords = wordList.slice( startIndex, endIndex+1 )
                //val phraseWeight = log( wordWeights.slice( startIndex, endIndex+1 ).foldLeft(0.0)( _ + _1( _ ).get ) )
                val phraseWeight = wordWeights.slice( startIndex, endIndex+1 ).foldLeft(0.0)( _ + _1( _ ).get )
                //println( "Site details: " + phraseWords + " (" + phraseWeight + ")" )
                
                if ( !categories.contains(categoryId) ) categories = categories + categoryId
                
                //println( startIndex, endIndex, topicId, categoryId, topicName, categoryName )
                
                if ( currDA == null || currDA.startIndex != startIndex || currDA.endIndex != endIndex )
                {
                	
                    currDA = new DisambiguationAlternative( startIndex, endIndex, validPhrase(phraseWords), phraseWeight, phraseWords )
                    
                    if ( daSites == Nil || !daSites.head.overlaps( currDA ) )
                    {
                    	//println( "Adding new site: " + phraseWords )
                        daSites = currDA :: daSites
                    }
                    else
                    {
                    	//println( "Adding site to existing: ", wordList.slice(daSites.head.startIndex, daSites.head.endIndex+1), wordList.slice(currDA.startIndex, currDA.endIndex+1) )
                        daSites.head.addAlternative( currDA )
                    }
                }
                
                if ( currDA.topicDetails == Nil || currDA.topicDetails.head.topicId != topicId )
                {
                    currDA.addTopicDetails( new TopicDetails( topicId, linkCount, new TreeSet[Int](), topicName ) )
//                    println( "--> " + phraseWords + " - "  + topicName + " " + linkCount )
                    //currDA.topicDetails = new TopicDetails( topicId, linkCount, new TreeSet[Int]() )::currDA.topicDetails
                }
                val curTopicDetails = currDA.topicDetails.head
                
                if ( validCategory( categoryName ) )
                {
                	curTopicDetails.categoryIds = curTopicDetails.categoryIds + categoryId
				}
            } )

            //println( "  complete... " + categories.size )
            
            /*for ( da <- daSites )
            {
                println( da.startIndex, da.endIndex, da.topicDetails.length )
            }*/

            //println( "Pulling topic and category names." )
            db.prepare( "SELECT DISTINCT topicId, topicName FROM topicCategories", Col[Int]::Col[String]::HNil ).
            	foreach( row => topicNameMap.put( _1(row).get, _2(row).get ) )
            db.prepare( "SELECT DISTINCT categoryId, categoryName FROM topicCategories", Col[Int]::Col[String]::HNil ).
            	foreach( row => categoryNameMap.put( _1(row).get, _2(row).get ) )

            db.exec( "ROLLBACK" )
            db.dispose()
        }

        private def getCategoryCounts( daSites : List[DisambiguationAlternative] ) : JTreeMap[Int, Double] =
        {
            val categoryCount = new JTreeMap[Int, Double]()
            var count = 0
            for ( alternatives <- daSites )
            {
            	//println( "BOOO: " + alternatives.children.length )
            	
                val tokenCategoryIds = alternatives.allCategoryIds()
                val topicAlternatives = alternatives.numTopicAlternatives()
                //println( count + "  " + tokenCategoryIds.size + " " + topicAlternatives )
                count += 1
                
                for ( categoryId <- tokenCategoryIds )
                {
                    if ( !categoryCount.containsKey(categoryId) )
                    {
                        categoryCount.put(categoryId, 0.0)
                    }
                    categoryCount.put(categoryId, categoryCount.get(categoryId) + 1.0)
                }
            }
            categoryCount
        }
        
        private def getCategoryWeights( daSites : List[DisambiguationAlternative] ) : JTreeMap[Int, Double] =
        {
            val categoryWeights = new JTreeMap[Int, Double]()
            val categoryCounts = new JTreeMap[Int, Int]()
            for ( alternative <- daSites )
            {
                val localWeights = new JTreeMap[Int, Double]
                alternative.weightedCategories( localWeights )
            
            
                val mapIt = localWeights.entrySet().iterator()
                while ( mapIt.hasNext() )
                {
                    val next = mapIt.next()
                    val categoryId = next.getKey()
                    val weight = next.getValue()
                    
                    val oldWeight = if (categoryWeights.containsKey(categoryId)) categoryWeights.get(categoryId) else 0.0
                    categoryWeights.put( categoryId, weight+oldWeight )
                    
                    var currCount = 0
                    if ( categoryCounts.containsKey(categoryId) )
                    {
                        currCount = categoryCounts.get(categoryId)
                    }
                    categoryCounts.put( categoryId, currCount + 1 )
                }
            }
            val filteredCategoryWeights = new JTreeMap[Int, Double]()
            
            // Filter out any categories that only exist at one DA site
            val mapIt = categoryWeights.entrySet().iterator()
            while ( mapIt.hasNext() )
            {
                val next = mapIt.next()
                val categoryId = next.getKey()
                val weight = next.getValue()
                if ( categoryCounts.get( categoryId ) > 1 )
                {
                    filteredCategoryWeights.put( categoryId, weight )
                }
            }
            filteredCategoryWeights
        }
        
        def resolve( maxAlternatives : Int ) : List[List[(Double, List[String], String)]] =
        {
            val weightFn = getCategoryWeights _
            //val weightFn = getCategoryCounts _
            
            val categoryWeights = weightFn( daSites )
            var sortedCategoryList = List[(Double, Int)]()
            
            //daSites.foreach( x => println( wordList.slice( x.startIndex, x.endIndex+1 ) ) )
            
            println( "Pruning invalid phrases" )
            daSites = daSites.filter( _.validPhrase )
            
            daSites.foreach( x => println( wordList.slice( x.startIndex, x.endIndex+1 ) ) )
            
            val mapIt = categoryWeights.entrySet().iterator()
            while ( mapIt.hasNext() )
            {
                val next = mapIt.next()
                val categoryId = next.getKey()
                val weight = next.getValue()
                
                sortedCategoryList = (weight, categoryId) :: sortedCategoryList
            }
            
            // Sort so that most numerous categories come first
            sortedCategoryList = sortedCategoryList.sortWith( _._1 > _._1 ) 
            
            println( "Number of words: " + wordList.length )
            println( "Number of categories before: " + categoryWeights.size() )
            println( "DA sites before: " + daSites.length )
            
            // TODO: Iterate over categories asserting them one by one
            for ( (weight, categoryId) <- sortedCategoryList )
            {
                println( "Asserting : " + categoryNameMap.get(categoryId) + ", " + weight )
                for ( site <- daSites ) site.assertCategoryWeighted( categoryId, weight )
                
                /*if ( weight > 1.0 )
                {
                    println( "Asserting : " + categoryNameMap.get(categoryId) + ", " + weight )
                    daSites = daSites.filter( _.assertCategory( categoryId ) )
                }*/
            }
            
            var disambiguation = List[List[(Double, List[String], String)]]()
            for ( site <- daSites )
            {
                // Weight, phrase, topic
                val res = site.reportAlternatives()
                
                val sorted = res.sortWith( _._1 > _._1 )
                
                println( " >> " + site.phrase )
                val alts = sorted.slice(0, maxAlternatives)
                for ( v <- alts ) println( "    " + v )
                disambiguation = alts :: disambiguation
            }

            disambiguation
            
            /*val newCategoryCounts = weightFn( daSites )
            println( "Number of categories after: " + newCategoryCounts.size() )
            println( "DA sites after pruning: " + daSites.length )
            
            {
                val mapIt = newCategoryCounts.entrySet().iterator()
                while ( mapIt.hasNext() )
                {
                    val next = mapIt.next()
                    val categoryId = next.getKey()
                    val count = next.getValue()
                    
                    //println( "|| " + count + ": " + categoryNameMap.get(categoryId) )
                }
            }
            var count = 0
            for ( alternatives <- daSites )
            {
                val tokenCategoryIds = alternatives.allCategoryIds()
                val topicAlternatives = alternatives.numTopicAlternatives()
                val topicDetails = alternatives.getTopicDetails()
                
                val topicAndStats = topicDetails.map( x => (topicNameMap.get( x.topicId ), alternatives.phraseWeight * ((x.linkCount*1.0) / (alternatives.topicLinkCount*1.0)) ) )
                
                println( wordList.slice(alternatives.startIndex, alternatives.endIndex+1).toString() + ": " + count + "  " + tokenCategoryIds.size + " " + topicAlternatives + " " + topicAndStats )
                count += 1
            }*/
        }
    }
}

class Disambiguator2( phraseMapFileName : String, topicFileName : String )
{
    val lookup = new PhraseMapLookup()
    lookup.load( new DataInputStream( new FileInputStream( new File( phraseMapFileName ) ) ) )
    
    val topicDb = new SQLiteWrapper( new File(topicFileName) )
    
    def phraseQuery( phrase : String ) =
    {
        val id = lookup.getIter().find( phrase )
        val sfQuery = topicDb.prepare( "SELECT phraseCount FROM phraseCounts WHERE phraseId=?", Col[Int]::HNil )
        sfQuery.bind(id)
        val relevance = _1(sfQuery.onlyRow).get
        
        val topicQuery = topicDb.prepare( "SELECT t2.name, t1.topicId, t1.count FROM phraseTopics AS t1 INNER JOIN topics AS t2 ON t1.topicId=t2.id WHERE t1.phraseTreeNodeId=? ORDER BY t1.count DESC", Col[String]::Col[Int]::Col[Int]::HNil )
        topicQuery.bind(id)

        val categoryQuery = topicDb.prepare( "SELECT t1.name FROM topics AS t1 INNER JOIN categoriesAndContexts AS t2 ON t1.id=t2.contextTopicId WHERE t2.topicId=? ORDER BY t1.name ASC", Col[String]::HNil )

        var totalOccurrences = 0
        var topicIds = List[(String, Int, Int)]()
        for ( el <- topicQuery )
        {
            val topicName = _1(el).get
            val topicId = _2(el).get
            val topicCount = _3(el).get
            topicIds = (topicName, topicId, topicCount) :: topicIds
            totalOccurrences += topicCount
        }
        
        println( "Word id: " + id + ", relevance: " + ((totalOccurrences+0.0) / (relevance+0.0)) + " (" + relevance + ")" )
        for ( (topicName, topicId, topicCount) <- topicIds.reverse )
        {
            println( "  " + topicName + ", " + topicCount )
            
            categoryQuery.bind( topicId )
            for ( category <- categoryQuery )
            {
                println( "    " + _1(category).get )
            }
        }
    }
        
    def disambiguate( text : String )
    {
        val words = Utils.luceneTextTokenizer( Utils.normalize( text ) )
        
        val phraseCountQuery = topicDb.prepare( "SELECT phraseCount FROM phraseCounts WHERE phraseId=?", Col[Int]::HNil )
        val topicQuery = topicDb.prepare( "SELECT topicId, count FROM phraseTopics WHERE phraseTreeNodeId=? ORDER BY count DESC", Col[Int]::Col[Int]::HNil )
        val topicCategoryQuery = topicDb.prepare( "SELECT contextTopicId FROM categoriesAndContexts WHERE topicId=?", Col[Int]::HNil )
        //sfQuery.bind(id)
        //val relevance = _1(sfQuery.toList.head).get
        
        var possiblePhrases = List[(Int, Int, Int, List[(Int, Int)])]()
        var activePhrases = List[(Int, PhraseMapLookup#PhraseMapIter)]()
        
        var wordIndex = 0
        var topicSet = TreeSet[Int]()
        var topicCategoryMap = TreeMap[Int, List[Int]]()
        println( "Parsing text:" )
        for ( word <- words )
        {
            println( "  " + word )
            val wordLookup = lookup.lookupWord( word )
                    
            wordLookup match
            {
                case Some(wordId) =>
                {
                    val newPhrase = lookup.getIter()
                    activePhrases = (wordIndex, newPhrase) :: activePhrases
                    
                    var newPhrases = List[(Int, PhraseMapLookup#PhraseMapIter)]()
                    for ( (fromIndex, phrase) <- activePhrases )
                    {
                        val phraseId = phrase.update(wordId)
                        if ( phraseId != -1 )
                        {
                            topicQuery.bind(phraseId)
                            val sfTopics = topicQuery.toList
                            
                            if ( sfTopics != Nil )
                            {
                                // This surface form has topics. Query phrase relevance and push back details
                                phraseCountQuery.bind(phraseId)
                                
                                val phraseCount = _1(phraseCountQuery.onlyRow).get
                                val toIndex = wordIndex
                                
                                // TODO: Do something with this result
                                //(fromIndex, toIndex, phraseCount, [(TopicId, TopicCount])
                                
                                
                                val topicDetails = for ( td <- sfTopics ) yield (_1(td).get, _2(td).get)
                                for ( (topicId, topicCount) <- topicDetails )
                                {
                                    topicSet = topicSet + topicId
                                    if ( !topicCategoryMap.contains(topicId) )
                                    {
                                        topicCategoryQuery.bind( topicId )
                                        val topicCategoryIds = for ( cid <- topicCategoryQuery ) yield _1(cid).get
                                        topicCategoryMap = topicCategoryMap.updated(topicId, topicCategoryIds.toList )
                                        
                                        for ( topicCategoryId <- topicCategoryIds )
                                        {
                                            topicSet = topicSet + topicCategoryId
                                        }
                                    }
                                }
                                
                                
                                possiblePhrases = (fromIndex, toIndex, phraseCount, topicDetails) :: possiblePhrases
                                
                                newPhrases = (fromIndex, phrase) :: newPhrases
                            }
                        }
                    }
                    activePhrases = newPhrases
                }
                case _ =>
            }
            wordIndex += 1
        }
        
        println( "Looking up topic names." )
        val topicNameQuery = topicDb.prepare( "SELECT name FROM topics WHERE id=?", Col[String]::HNil )
        var topicNameMap = TreeMap[Int, String]()
        for ( topicId <- topicSet )
        {
            topicNameQuery.bind(topicId)
            val topicName = _1(topicNameQuery.onlyRow).get
            
            topicNameMap = topicNameMap.updated( topicId, topicName )
        }
        
        // ORDER BY t1.endIndex DESC, t1.startIndex ASC, t1.topicId, t2.categoryId
        val possiblePhrasesSorted = possiblePhrases.sortWith( (x, y) =>
        {
            if ( x._2 != y._2 )
            {
                x._2 < y._2
            }
            if ( x._1 != y._1 )
            {
                x._1 > y._1
            }
            x._3 > y._3
        } )
        
        println( "Building disambiguation forest." )
        for ( (fromIndex, toIndex, phraseCount, topicDetails) <- possiblePhrasesSorted )
        {
            for ( (topicId, topicCount) <- topicDetails )
            {
                val topicCategories = topicCategoryMap(topicId)
                
                // Now build the disambiguation forest (as before in Disambiguator)
            }
        }
    }
}


