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

// Steve ideas:
// * Context weighted according to the inverse of their frequency of occurrence
// * Weight context proportional to (sum of) the (word) distance from the sf being asserted over. Allows local contexts.

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
	
    class TopicDetails( val topicId : Int, val topicWeight : Double, var categoryIds : TreeSet[Int], val topicName : String )
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
        
        /*def overlaps( da : DisambiguationAlternative ) =
            (startIndex >= da.startIndex && endIndex <= da.endIndex) ||
            (da.startIndex >= startIndex && da.endIndex <= endIndex)*/
        def overlaps( da : DisambiguationAlternative ) =
        {
            def within( x : Int, s : Int, e : Int ) = x >= s && x <= e
            
            within( startIndex, da.startIndex, da.endIndex ) ||
            within( endIndex, da.startIndex, da.endIndex ) ||
            within( da.startIndex, startIndex, endIndex ) ||
            within( da.endIndex, startIndex, endIndex )
        }

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
                val topicWeight = topicDetail.topicWeight
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
    

    class Disambiguator2( phraseMapFileName : String, topicFileName : String )
    {
        val lookup = new PhraseMapLookup()
        lookup.load( new DataInputStream( new FileInputStream( new File( phraseMapFileName ) ) ) )
        
        val topicDb = new SQLiteWrapper( new File(topicFileName) )
        
        var daSites = List[DisambiguationAlternative]()
        
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
            
        class Builder( val text : String )
        {
            val wordList = Utils.luceneTextTokenizer( Utils.normalize( text ) )
            
            var topicCategoryMap = TreeMap[Int, TreeSet[Int]]()
            var topicNameMap = TreeMap[Int, String]()
            var contextWeightMap = TreeMap[Int, Double]()
            
            def build()
            {
                val phraseCountQuery = topicDb.prepare( "SELECT phraseCount FROM phraseCounts WHERE phraseId=?", Col[Int]::HNil )
                // numTopicsForWhichThisIsAContext
                val topicQuery = topicDb.prepare( "SELECT topicId, count FROM phraseTopics WHERE phraseTreeNodeId=? ORDER BY count DESC", Col[Int]::Col[Int]::HNil )
                val topicCategoryQuery = topicDb.prepare( "SELECT contextTopicId FROM categoriesAndContexts WHERE topicId=?", Col[Int]::HNil )
                
                val normalisationQuery = topicDb.prepare( "SELECT MAX(count) FROM numTopicsForWhichThisIsAContext", Col[Int]::HNil )
                
                var possiblePhrases = List[(Int, Int, Int, List[(Int, Int)])]()
                var activePhrases = List[(Int, PhraseMapLookup#PhraseMapIter)]()
                
                var wordIndex = 0
                var topicSet = TreeSet[Int]()
                
                val maxCategoryCount = _1(normalisationQuery.onlyRow).get
                
                println( "Parsing text:" )
                for ( word <- wordList )
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
                                    
                                    //println( ":: " + phraseId)
                                    if ( sfTopics != Nil )
                                    {
                                        // This surface form has topics. Query phrase relevance and push back details
                                        phraseCountQuery.bind(phraseId)
                                        
                                        val phraseCount = _1(phraseCountQuery.onlyRow).get
                                        val toIndex = wordIndex
                                        
                                        // TODO: Do something with this result
                                        //(fromIndex, toIndex, phraseCount, [(TopicId, TopicCount])
                                        
                                        
                                        val topicDetails = for ( td <- sfTopics.toList ) yield (_1(td).get, _2(td).get)
                                        for ( (topicId, topicCount) <- topicDetails )
                                        {
                                            assert( topicId != 0 )
                                            topicSet = topicSet + topicId
                                            if ( !topicCategoryMap.contains(topicId) )
                                            {
                                                topicCategoryQuery.bind( topicId )
                                                val topicIdList = for ( cid <- topicCategoryQuery ) yield _1(cid).get
                                                val topicCategoryIds = topicIdList.foldLeft( TreeSet[Int]() )( _ + _ )
                                                topicCategoryMap = topicCategoryMap.updated(topicId, topicCategoryIds )
                                                
                                                //println( topicId + ": " + topicCategoryIds )
                                                for ( topicCategoryId <- topicCategoryIds )
                                                {
                                                    assert( topicCategoryId != 0 )
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
                val topicNameQuery = topicDb.prepare( "SELECT t1.name, t2.count FROM topics AS t1 LEFT JOIN numTopicsForWhichThisIsAContext AS t2 on t1.id=t2.topicId WHERE id=?", Col[String]::Col[Int]::HNil )
                
                for ( topicId <- topicSet )
                {
                    topicNameQuery.bind(topicId)
                    val theRow = topicNameQuery.onlyRow
                    val topicName = _1(theRow).get
                    val topicWeightAsContext = 1.0 - (_2(theRow).get.toDouble / maxCategoryCount.toDouble)
                    
                    topicNameMap = topicNameMap.updated( topicId, topicName )
                    contextWeightMap = contextWeightMap.updated( topicId, topicWeightAsContext )
                }
                
                val possiblePhrasesSorted = possiblePhrases.sortWith( (x, y) =>
                {
                    if ( x._2 != y._2 )
                    {
                        x._2 > y._2
                    }
                    else if ( x._1 != y._1 )
                    {
                        x._1 < y._1
                    }
                    else
                    {
                        x._3 > y._3
                    }
                } )
                
                println( "Building disambiguation forest." )
                for ( (startIndex, endIndex, phraseCount, topicDetails) <- possiblePhrasesSorted )
                {
                    val phraseWords = wordList.slice( startIndex, endIndex+1 )
                    println( "++ " + startIndex + ", " + endIndex + ", " + phraseWords )
                    val sfCount = topicDetails.foldLeft(0.0)( _ + _._2 )
                    val sfWeight = (sfCount.toDouble / phraseCount.toDouble)
                    
                    val da = new DisambiguationAlternative( startIndex, endIndex, true, sfWeight, phraseWords )
                    if ( daSites != Nil )
                    {
                        println( daSites.head.startIndex + ", " + daSites.head.endIndex + ", " + da.startIndex + ", " + da.endIndex + ": " + daSites.head.overlaps( da ))
                    }
                        
                    if ( daSites == Nil || !daSites.head.overlaps( da ) )
                    {
                        daSites = da :: daSites
                    }
                    else
                    {
                        daSites.head.addAlternative( da )
                    }
                    
                    for ( (topicId, topicCount) <- topicDetails )
                    {
                        val topicWeight = topicCount.toDouble / sfCount.toDouble
                        //val topicWeight = topicCount.toDouble / phraseCount.toDouble
                        val topicCategories = topicCategoryMap(topicId)
                        val topicName = topicNameMap(topicId)
                        
                        da.addTopicDetails( new TopicDetails( topicId, topicWeight, topicCategories, topicName ) )
                    }
                }
                
                println( "Number of da sites: " + daSites.length )
                for ( da <- daSites ) println( " " + wordList.slice(da.startIndex, da.endIndex+1) + ", " + da.phraseWeight )
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
                
                    //var contextWeightMap = TreeMap[Int, Double]()
                    val mapIt = localWeights.entrySet().iterator()
                    while ( mapIt.hasNext() )
                    {
                        val next = mapIt.next()
                        val categoryId = next.getKey()
                        val categoryWeight = contextWeightMap(categoryId)
                        val weight = next.getValue() * categoryWeight
                        
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
                    if ( weight > 0.1 )
                    {
                        println( "Asserting : " + topicNameMap(categoryId) + ", " + weight )
                        for ( site <- daSites ) site.assertCategoryWeighted( categoryId, weight )
                    }
                    
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
            }
        }
    }
}


