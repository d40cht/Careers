package org.seacourt.disambiguator

import scala.collection.immutable.{TreeSet, TreeMap}
import java.util.{TreeMap => JTreeMap}
import math.{max, log}
import java.io.{File, DataInputStream, FileInputStream}

import scala.util.matching.Regex

import org.seacourt.sql.SqliteWrapper._
import org.seacourt.utility.StopWords.stopWordSet
import org.seacourt.utility._

import org.seacourt.utility.{Graph, Node}

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

class SurfaceForm( val phraseId : Int, val phraseWeight : Double, val topics : List[(Int, Double)] )
{
    var topicWeights = TreeMap[Int, Double]()
}

class AmbiguityAlternative( val sites : List[(Int, Int, SurfaceForm)] )
{
    var weight = 0.0
}

class AmbiguitySite( var start : Int, var end : Int )
{
    var els = List[(Int, Int, SurfaceForm)]()
    var combs = List[AmbiguityAlternative]()
    
    def overlaps( from : Int, to : Int ) =
    {
        val overlap = (i : Int, s : Int, e : Int) => i >= s && i <= e
        
        overlap( from, start, end ) || overlap( to, start, end ) || overlap( start, from, to ) || overlap( end, from, to )
    }
    
    def extend( from : Int, to : Int, sf : SurfaceForm )
    {
        start = start min from
        end = end max to

        els = (from, to, sf)::els
    }
    
    def combinations()
    {
        var seenStartSet = Set[Int]()
     
        val ordered = els.sortWith( (x, y) =>
        {
            if ( x._1 != y._1 ) x._1 < y._1
            else x._2 < y._2
        })
        
        //println( ordered )
        val asArr = ordered.toArray
        
        
        for ( i <- 0 until asArr.length )
        {
            if ( !seenStartSet.contains(i) )
            {
                var nextUpdate = i
                var done = false
                
                var stack = List[Int]()
                while (!done)
                {
                    // Populate out
                    var found = false
                    var valid = true
                    var j = nextUpdate
                    for ( j <- nextUpdate until asArr.length )
                    {
                        val nextEl = asArr(j)
                        if ( stack == Nil || nextEl._1 > asArr(stack.head)._2 )
                        {
                            // Check there's no fillable gap
                            var gapStart = asArr(0)._1
                            if ( stack != Nil )
                            {
                                gapStart = asArr(stack.head)._2 + 1
                            }
                            
                            val gapEnd = nextEl._1
                            if ( gapEnd - gapStart > 0 )
                            {
                                for ( (start, end, sf) <- asArr )
                                {
                                    if ( start >= gapStart && start< gapEnd && end <= gapEnd )
                                    {
                                        //println( "::: " + stack + ", " + j + ", " + start + ", " + end + ", " + gapStart + ", " + gapEnd )
                                        valid = false
                                    }
                                }
                            }
                            
                            stack = j :: stack
                            //seenStartSet = seenStartSet + j
                            found = true
                        }
                    }
                    
                    //println( "? " + stack )
                    
                    if ( found && valid )
                    {
                        val alternative = new AmbiguityAlternative( stack.map(asArr(_)).reverse )
                        combs = alternative :: combs
                    }

                    
                    
                    if ( stack.length <= 1 ) done = true
                    else
                    {
                        nextUpdate = stack.head+1
                        stack = stack.tail
                    }
                }
            }
        }
    
        combs = combs.reverse
    }
}

class AmbiguityForest( val words : List[String], val topicNameMap : TreeMap[Int, String] )
{
    var sites = List[AmbiguitySite]()
    var contextWeights = TreeMap[Int, Double]()
    
    def addTopics( sortedPhrases : List[(Int, Int, Int, SurfaceForm, List[(Int, Int)])] )
    {
        for ( (startIndex, endIndex, phraseCount, surfaceForm, topicDetails) <- sortedPhrases )
        {
            val sfCount = topicDetails.foldLeft(0.0)( _ + _._2 )
            val sfWeight = (sfCount.toDouble / phraseCount.toDouble)
            
            //if ( sfWeight > 0.01 )
            {
                if ( sites == Nil || !sites.head.overlaps( startIndex, endIndex ) )
                {
                    sites = new AmbiguitySite( startIndex, endIndex ) :: sites
                }
                
                sites.head.extend( startIndex, endIndex, surfaceForm )
            }
        }
        sites = sites.reverse
    }
    
    def buildContextWeights( contextsForTopic : (Int) => Map[Int, Double] )
    {
        var siteOrigination = TreeMap[Int, TreeSet[(Int, Int)]]()
        var contextOrigination = TreeMap[Int, TreeSet[Int]]()
        for ( site <- sites )
        {
            site.combinations()
         
            var siteList = TreeSet[(Int,Int)]()
            var siteWeights = TreeMap[Int, Double]()   
            for ( alternative <- site.combs )
            {
                var sfWeights = TreeMap[Int, Double]()
                for ( (startIndex, endIndex, sf) <- alternative.sites )
                {
                    //val phraseWeight : Double, val topics : List[(Int, Int)]
                    val surfaceFormWeight = sf.phraseWeight
                    for ( (topicId, topicWeight) <- sf.topics )
                    {
                        var contexts = contextsForTopic( topicId )
                        
                        // This topic itself is a context
                        contexts = contexts.updated( topicId, 1.0 )
                        for ( (contextId, contextWeight) <- contexts )
                        {
                            val aggWeight = surfaceFormWeight * topicWeight * contextWeight
                            sfWeights = sfWeights.updated( contextId, aggWeight )
                            
                            var sfList = contextOrigination.getOrElse( contextId, TreeSet[Int]() )
                            sfList += sf.phraseId
                            contextOrigination = contextOrigination.updated( contextId, sfList )
                            
                            var siteList = siteOrigination.getOrElse( contextId, TreeSet[(Int, Int)]() )
                            siteList += ( (site.start, site.end) )
                            siteOrigination = siteOrigination.updated( contextId, siteList )
                        }
                    }
                }
                
                for ( (id, weight) <- sfWeights )
                {
                    var sw = 0.0
                    if ( siteWeights.contains(id) )
                    {
                        sw = siteWeights(id)
                    }
                    siteWeights = siteWeights.updated( id, sw max weight )
                }
            }
            
            for ( (id, weight) <- siteWeights )
            {
                var sw = 0.0
                if ( contextWeights.contains(id) )
                {
                    sw = contextWeights(id)
                }
                contextWeights = contextWeights.updated( id, sw+weight )
            }
        }
        
        /*for ( (id, w) <- contextWeights )
        {
            println( "~~~~ " + topicNameMap(id) + ", " + w + ": " + siteOrigination(id) + " | " + contextOrigination(id) )
        }*/
        
        // Filter out any contexts originating only from one surface form or site
        contextWeights = contextWeights.filter( x => siteOrigination(x._1).size > 1 && contextOrigination(x._1).size > 1 )
    }
    
    def applyContexts( contextsForTopic : (Int) => Map[Int, Double] )
    {
        val reversed = contextWeights.toList.map( (v) => (v._2, v._1) ).sortWith( (x,y) => (x._1 > y._1) )

        for ( (weight, contextId) <- reversed.slice(0,200) )
        {
            println( "Applying: " + topicNameMap(contextId) + ": " + weight )
            for ( site <- sites )
            {   
                for ( alternative <- site.combs )
                {
                    for ( (startIndex, endIndex, sf) <- alternative.sites )
                    {
                        for ( (topicId, topicWeight) <- sf.topics )
                        {
                            val contexts = contextsForTopic( topicId )
                            
                            if ( contexts.contains( contextId ) )
                            {
                                val contextWeight = contexts(contextId)
                                val weightIncrement = weight * /*sf.phraseWeight **/ topicWeight * contextWeight
                                // Weight this based on other weightings?
                                alternative.weight += weightIncrement
                                
                                var tpw = sf.topicWeights.getOrElse( topicId, 0.0 )
                                sf.topicWeights = sf.topicWeights.updated(topicId, tpw + weightIncrement)
                            }
                        }
                    }
                }
            }
        }
        
        class SureSites( val start : Int, val end : Int, val topicId : Int, val weight : Double )
        {
        }
        
        
        
        var disambiguated = List[SureSites]()
        for ( site <- sites )
        {
            val weightedAlternatives = site.combs.sortWith( _.weight > _.weight )
            
            println( "Site: " + site.start + ", " + site.end )
            for ( wa <- weightedAlternatives )
            {
                // x._3 is the surface form. Pull out the highest weight topic too
                println( "  Site weight: " + wa.weight )
                for ( site <- wa.sites )
                {
                    val sf = site._3

                    println( "    Element: " + words.slice( site._1, site._2+1 ) )
                    for ( (weight, id) <- sf.topicWeights.map(x =>(-x._2, x._1)).toList.slice(0, 5) )
                    {
                        println( "       ::: " + topicNameMap(id) + ": " + (-weight) )
                    }
                }
            }
            
            val canonicalAlternative = weightedAlternatives.first
            for ( site <- canonical )
            {
                val canonicalTopic = sf.topicWeights.map( x =>(x._2, x._1) ).last
                disambiguated = new SureSites( site.start, site.end, canonicalTopic._2, canonicalTopic._1 ) :: disambiguated
            }
        }
        
        // Now build a context set for these disambiguated sites and do the SCC building.        
        var nodesById = TreeMap[Int, Node]()
        val g = new Graph()
        for ( sureSite <- disambiguated )
        {
            val topicId = sureSite.topicId
            val weight = sureSite.weight
            
            nodesById = nodesById.updated( topicId, g.addNode( topicNameMap(topicId) ) )
            for ( (contextId, contextWeight) <- contextsForTopic(topicId) )
            {
                if ( !nodesById.contains( contextId ) )
                {
                    nodesById = nodesById.updated( contextId, g.addNode( topicNameMap(contextId) ) )
                }
            }
        }
        
        // Lookup these additional contexts and add them into contextsForTopic map
        val topicCategoryQuery = topicDb.prepare( "SELECT contextTopicId, weight1, weight2 FROM linkWeights WHERE topicId=?", Col[Int]::Col[Double]::Col[Double]::HNil )
        for ( (id, node) <- nodesById )
        {
            if ( !contextsForTopic.contains( id ) )
            {
                topicCategoryQuery.bind( id )
                
                contextsForTopic = contex
            }
        }
        
        for ( (id, node) <- nodesById )
        {
            val children = contextsForTopic( id )
            
            for ( (contextId, weight) <- children )
            {
                val toNode = nodesById(contextId)
                
                if ( weight > 0.2 )
                {
                    g.addEdge( fromNode, toNode )
                }
            }
        }
        
        val sccs = g.connected()
        println( "Canonical SCCs: " )
        for ( els <- sccs )
        {
            val elnames = for( el <- els ) yield el.name
            println( "  >> " + elnames.mkString(", ") )
        }
    }
}

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
	
    class TopicDetails( val topicId : Int, val topicWeight : Double, var contextWeights : TreeMap[Int, Double], val topicName : String )
    {
        var score = 0.0
        
        def assertCategory( contextId : Int, weight : Double )
        {
            if ( contextWeights.contains( contextId ) ) score += weight
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
                for ( (categoryId, contextWeight) <- topicDetail.contextWeights )
                {
                    val oldWeight = if (localWeights.containsKey(categoryId)) localWeights.get(categoryId) else 0.0
                    localWeights.put( categoryId, max(oldWeight, phraseWeight*topicWeight*contextWeight) )
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
                categoryIds = categoryIds ++ (for ( (id, weight) <- topicDetail.contextWeights ) yield id)
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
                if ( td.contextWeights.contains( categoryId ) )
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
            val debugPrunedTopics = topicDetails.filter( !_.contextWeights.contains(assertedCategoryId ) )
            
            var categoryAlive = false
            topicDetails = topicDetails.filter( _.contextWeights.contains(assertedCategoryId) )
            
            if ( topicDetails != Nil ) categoryAlive = true
            if ( debugPrunedTopics != Nil )
            {
                println( "  Pruning " + phrase )
                //if ( topicDetails == Nil ) println( "  --> Phrase topics empty. Removed." )
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
            
            var topicCategoryMap = TreeMap[Int, TreeMap[Int, Double]]()
            var topicNameMap = TreeMap[Int, String]()
            var contextWeightMap = TreeMap[Int, Double]()
            
            def build()
            {
                val phraseCountQuery = topicDb.prepare( "SELECT phraseCount FROM phraseCounts WHERE phraseId=?", Col[Int]::HNil )
                // numTopicsForWhichThisIsAContext
                val topicQuery = topicDb.prepare( "SELECT topicId, count FROM phraseTopics WHERE phraseTreeNodeId=? ORDER BY count DESC", Col[Int]::Col[Int]::HNil )
                val topicCategoryQuery = topicDb.prepare( "SELECT contextTopicId, weight1, weight2 FROM linkWeights WHERE topicId=?", Col[Int]::Col[Double]::Col[Double]::HNil )
                
                //val normalisationQuery = topicDb.prepare( "SELECT MAX(count) FROM numTopicsForWhichThisIsAContext", Col[Int]::HNil )
                
                var possiblePhrases = List[(Int, Int, Int, SurfaceForm, List[(Int, Int)])]()
                var activePhrases = List[(Int, PhraseMapLookup#PhraseMapIter)]()
                
                var wordIndex = 0
                var topicSet = TreeSet[Int]()
                
                //val maxCategoryCount = _1(normalisationQuery.onlyRow).get
                
                println( "Parsing text:" )
                //var surfaceFormMap = TreeMap[Int, SurfaceForm]()
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
                                        val sfCount = topicDetails.foldLeft(0.0)( (x,y) => x + y._2 )
                                        val sfWeight = (sfCount.toDouble / phraseCount.toDouble)
                                        
                                        /*var thisSf : SurfaceForm = null
                                        if ( !surfaceFormMap.contains( phraseId ) )
                                        {
                                            val weightedTopics = for ( td <- topicDetails ) yield (td._1, td._2.toDouble / sfCount.toDouble)
                                            thisSf = new SurfaceForm( phraseId, sfWeight, weightedTopics )
                                            surfaceFormMap = surfaceFormMap.updated( phraseId, thisSf )
                                        }
                                        else
                                        {
                                            thisSf = surfaceFormMap( phraseId )
                                        }*/
                                        val weightedTopics = for ( td <- topicDetails ) yield (td._1, td._2.toDouble / sfCount.toDouble)
                                        val thisSf = new SurfaceForm( phraseId, sfWeight, weightedTopics )
                                        
                                        for ( (topicId, topicCount) <- topicDetails )
                                        {
                                            assert( topicId != 0 )
                                            topicSet = topicSet + topicId
                                            if ( !topicCategoryMap.contains(topicId) )
                                            {
                                                topicCategoryQuery.bind( topicId )

                                                var contextWeights = TreeMap[Int, Double]()
                                                for ( row <- topicCategoryQuery )
                                                {
                                                    val cid = _1(row).get
                                                    val weight1 = _2(row).get
                                                    val weight2 = _3(row).get
                                                    
                                                    contextWeights = contextWeights.updated( cid, weight1 )
                                                }

                                                topicCategoryMap = topicCategoryMap.updated(topicId, contextWeights)
                                                
                                                //println( topicId + ": " + topicCategoryIds )
                                                for ( (topicCategoryId, weight) <- contextWeights )
                                                {
                                                    assert( topicCategoryId != 0 )
                                                    topicSet = topicSet + topicCategoryId
                                                }
                                            }
                                        }
                                        
                                        
                                        possiblePhrases = (fromIndex, toIndex, phraseCount, thisSf, topicDetails) :: possiblePhrases
                                        
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
                //val topicNameQuery = topicDb.prepare( "SELECT t1.name, t2.count FROM topics AS t1 LEFT JOIN numTopicsForWhichThisIsAContext AS t2 on t1.id=t2.topicId WHERE id=?", Col[String]::Col[Int]::HNil )
                val topicNameQuery = topicDb.prepare( "SELECT name FROM topics WHERE id=?", Col[String]::HNil )
                
                for ( topicId <- topicSet )
                {
                    topicNameQuery.bind(topicId)
                    val theRow = topicNameQuery.onlyRow
                    val topicName = _1(theRow).get
                    //val topicWeightAsContext = 1.0 - (_2(theRow).get.toDouble / maxCategoryCount.toDouble)
                    val topicWeightAsContext = 1.0
                    
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
                
                /* ************************************************** */
                println( "Building new ambiguity forest" )
                
                
                
                val f = new AmbiguityForest( wordList, topicNameMap )
                f.addTopics( possiblePhrasesSorted )
                f.buildContextWeights( x => topicCategoryMap(x) )
                f.applyContexts( x => topicCategoryMap(x) )
                
                // Dump out the resolved text as XML along with a list of topics and contexts
                
                /* ************************************************** */
                
                println( "Building disambiguation forest." )
                for ( (startIndex, endIndex, phraseCount, surfaceForm, topicDetails) <- possiblePhrasesSorted )
                {
                    val phraseWords = wordList.slice( startIndex, endIndex+1 )
                    //println( "++ " + startIndex + ", " + endIndex + ", " + phraseWords )
                    val sfCount = topicDetails.foldLeft(0.0)( _ + _._2 )
                    val sfWeight = (sfCount.toDouble / phraseCount.toDouble)
                    
                    val da = new DisambiguationAlternative( startIndex, endIndex, true, sfWeight, phraseWords )
                    if ( daSites != Nil )
                    {
                        //println( daSites.head.startIndex + ", " + daSites.head.endIndex + ", " + da.startIndex + ", " + da.endIndex + ": " + daSites.head.overlaps( da ))
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
                
                //println( "Number of da sites: " + daSites.length )
                //for ( da <- daSites ) println( " " + wordList.slice(da.startIndex, da.endIndex+1) + ", " + da.phraseWeight )
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
                
                //println( "Pruning invalid phrases" )
                daSites = daSites.filter( _.validPhrase )
                
                //daSites.foreach( x => println( wordList.slice( x.startIndex, x.endIndex+1 ) ) )
                
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
                var assertedCount = 0
                var assertedLimit = 200
                
                val g = new Graph()
                
                var relevantCategorySet = TreeMap[Int, Int]()
                var nodes = new Array[Node](assertedLimit)
                for ( (weight, categoryId) <- sortedCategoryList )
                {
                    if ( assertedCount < assertedLimit )
                    {
                        relevantCategorySet = relevantCategorySet.updated( categoryId, assertedCount )
                        val name = topicNameMap(categoryId)
                        println( "Asserting : " + name + ", " + weight )
                        for ( site <- daSites ) site.assertCategoryWeighted( categoryId, weight )
                        
                        nodes(assertedCount) = g.addNode( name )
                        assertedCount += 1
                    }
                }
                
                // Build graph
                val topicCategoryQuery = topicDb.prepare( "SELECT contextTopicId, weight1, weight2 FROM linkWeights WHERE topicId=?", Col[Int]::Col[Double]::Col[Double]::HNil )
                for ( (categoryId, index) <- relevantCategorySet )
                {
                    topicCategoryQuery.bind( categoryId )
                    for ( row <- topicCategoryQuery )
                    {
                        val contextId = _1(row).get
                        val weight1 = _2(row).get
                        val weight2 = _3(row).get
                        
                        if ( relevantCategorySet.contains( contextId ) )
                        {
                            val fromNode = nodes(index)
                            val toNode = nodes( relevantCategorySet(contextId) )
                            
                            //println( "From " + fromNode.name + " to " + toNode.name + ": " + weight1 + ", " + weight2 )
                            
                            if ( weight1 > 0.2 )
                            {
                                g.addEdge( fromNode, toNode )
                            }
                        }
                    }
                }
                
                // Dump SCCS
                val sccs = g.connected()
                for ( els <- sccs )
                {
                    val elnames = for( el <- els ) yield el.name
                    //println( "### " + elnames.mkString(", ") )
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

                disambiguation.reverse
            }
        }
    }
}


