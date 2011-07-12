package org.seacourt.disambiguator

import scala.collection.mutable.{MultiMap, Stack, ArrayBuffer}
import scala.collection.immutable.{TreeSet, TreeMap, HashSet, HashMap}
import java.util.{TreeMap => JTreeMap}
import math.{max, log, pow, sqrt}
import java.io.{File, DataInputStream, FileInputStream}
import java.util.regex.Pattern

import org.apache.commons.math.distribution.NormalDistributionImpl

import scala.xml.XML

import scala.util.matching.Regex
import scala.collection.mutable.{ListBuffer, Queue}

import org.seacourt.sql.SqliteWrapper._
import org.seacourt.utility.StopWords.stopWordSet
import org.seacourt.utility._
import org.seacourt.disambiguator.Community._

import org.seacourt.utility.{Graph}

// Thoughts:
// * Testing: are the various tables actually correct. Are there really 55331 articles that reference 'Main:Voivodeships of Poland'?
// *    Indeed, are there really 466132 articles that mention the word 'voivodeships'?
// * Get some test documents and test disambig at the paragraph level, then at the doc level and compare the results
// * Mark up the test documents manually with expected/desired results
// * Count at each site whenever an assertion has been applied (i.e. the category was relevant). Disable sites where only a few were relevant? Or something else!

// Steve ideas:
// * Weight context proportional to (sum of) the (word) distance from the sf being asserted over. Allows local contexts.

// Richard Armitage, 'The Vulcans', Scooter Libby

// NOTE, SCC may not be as appropriate as other 'Community structure' algorithms: http://en.wikipedia.org/wiki/Community_structure
// NOTE: Might be able to merge topics that are very similar to each other to decrease context space (Fast unfolding of communities in large networks, or Modularity maximisation).

class AmbiguitySiteBuilder( var start : Int, var end : Int )
{
    var els = List[AmbiguityForest.SurfaceFormDetails]()
    
    def overlaps( from : Int, to : Int ) =
    {
        val overlap = (i : Int, s : Int, e : Int) => i >= s && i <= e
        
        overlap( from, start, end ) || overlap( to, start, end ) || overlap( start, from, to ) || overlap( end, from, to )
    }
    
    def extend( sf : AmbiguityForest.SurfaceFormDetails )
    {
        val from = sf.start
        val to = sf.end
        start = start min from
        end = end max to

        els = sf::els
    }
    
    def buildSite() =
    {
        val site = new AmbiguitySite( start, end )
        
        var seenStartSet = Set[Int]()
     
        val ordered = els.sortWith( (x, y) =>
        {
            if ( x.start != y.start ) x.start < y.start
            else x.end < y.end
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
                        if ( stack == Nil || nextEl.start > asArr(stack.head).end )
                        {
                            // Check there's no fillable gap
                            var gapStart = asArr(0).start
                            if ( stack != Nil )
                            {
                                gapStart = asArr(stack.head).end + 1
                            }
                            
                            val gapEnd = nextEl.start
                            if ( gapEnd - gapStart > 0 )
                            {
                                for ( sf <- asArr )
                                {
                                    if ( sf.start >= gapStart && sf.start < gapEnd && sf.end <= gapEnd )
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
                        site.addAlternative( new site.AmbiguityAlternative( stack.map(asArr(_)).reverse ) )
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
    
        //combs = combs.reverse
        
        site
    }

}


class Disambiguator( phraseMapFileName : String, topicFileName : String/*, categoryHierarchyFileName : String*/ )
{
    val lookup = new PhraseMapLookup()
    lookup.load( new DataInputStream( new FileInputStream( new File( phraseMapFileName ) ) ) )
    
    val topicDb = new SQLiteWrapper( new File(topicFileName) )
    topicDb.exec( "PRAGMA cache_size=2000000" )

    class CategoryHierarchy( fileName : String )
    {
        val hierarchy = new EfficientArray[EfficientIntPair](0)
        hierarchy.load( new DataInputStream( new FileInputStream( new File(fileName) ) ) )
        
        def toTop( categoryIds : List[Int] ) =
        {
            val q = Stack[Int]()
            for ( id <- categoryIds ) q :+ id
            var seen = HashSet[Int]()
            
            var edgeList = ArrayBuffer[(Int, Int)]()
            while ( !q.isEmpty )
            {
                val first = q.pop()

                val it = Utils.lowerBound( new EfficientIntPair( first, 0 ), hierarchy, (x:EfficientIntPair, y:EfficientIntPair) => x.less(y) )                
                while ( it < hierarchy.size && hierarchy(it).first == first )
                {
                    val parentId = hierarchy(it).second
                    
                    edgeList.append( (first, parentId) )
                    if ( !seen.contains( parentId ) )
                    {
                        q :+ parentId
                        seen = seen + parentId
                    }
                }
            }
        }
    }
    
    //val categoryHierarchy = new CategoryHierarchy( categoryHierarchyFileName )
    
    def phraseQuery( phrase : String ) =
    {
        val id = lookup.getIter().find( phrase )
        val sfQuery = topicDb.prepare( "SELECT phraseCount FROM phraseCounts WHERE phraseId=?", Col[Int]::HNil )
        sfQuery.bind(id)
        val relevance = _1(sfQuery.onlyRow).get
        
        val topicQuery = topicDb.prepare( "SELECT t2.name, t1.topicId, t1.count FROM phraseTopics AS t1 INNER JOIN topics AS t2 ON t1.topicId=t2.id WHERE t1.phraseTreeNodeId=? ORDER BY t1.count DESC", Col[String]::Col[Int]::Col[Int]::HNil )
        topicQuery.bind(id)

        
        //val categoryQuery = topicDb.prepare( "SELECT t1.name, t3.weight1, t3.weight2 FROM topics AS t1 INNER JOIN categoriesAndContexts AS t2 ON t1.id=t2.contextTopicId INNER JOIN linkWeights AS t3 ON t3.topicId=t2.topicId AND t3.contextTopicId=t2.contextTopicId WHERE t2.topicId=? ORDER BY t1.name ASC", Col[String]::Col[Double]::Col[Double]::HNil )
        val categoryQuery = topicDb.prepare( "SELECT t1.name, t2.weight FROM topics AS t1 INNER JOIN linkWeights2 AS t2 ON t1.id=t2.contextTopicId WHERE t2.topicId=? ORDER BY t1.name ASC", Col[String]::Col[Double]::HNil )

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
                val name = _1(category).get
                val weight = _2(category).get
                println( "    " + name + ": " + weight )
            }
        }
    }
    
    def contextQuery( topics : List[String] )
    {
        var contextWeights = TreeMap[String, Double]()
        for ( t <- topics )
        {
            val categoryQuery = topicDb.prepare( "SELECT t3.name, t2.weight FROM topics AS t1 INNER JOIN linkWeights2 AS t2 ON t1.id=t2.topicId INNER JOIN topics AS t3 ON t3.id=t2.contextTopicId WHERE t1.name=? AND t1.id != t2.contextTopicId", Col[String]::Col[Double]::HNil )
            categoryQuery.bind( t )
            
            for ( row <- categoryQuery )
            {
                val contextName = _1(row).get
                val contextWeight = _2(row).get
                val oldWeight = contextWeights.getOrElse( contextName, 0.0 )
                contextWeights = contextWeights.updated( contextName, oldWeight + contextWeight )
            }
        }
        
        val sorted = contextWeights.toList.sortWith( _._2 > _._2 )
        for ( (name, weight) <- sorted )
        {
            println( name + ": " + weight )
        }
    }

    object Builder
    {
        val disallowedCategories = HashSet[String](
            "Category:Greek loanwords",
            "Category:Philosophy redirects",
            "Category:Protected redirects",
            "Category:American websites",
            "Category:1995 introductions",
            "Category:Articles including recorded pronunciations (UK English)",
            "Category:American inventions",
            "Category:Article Feedback Pilot",
            "Category:Swedish-speaking Finns",
            "Category:Acronyms",
            "Category:Articles with example code",
            "Category:Articles with example pseudocode",
            "Category:Living people",
            "Category:Discovery and invention controversies",
            "Category:Categories named after universities and colleges",
            "Category:Computing acronyms",
            "Category:Articles with inconsistent citation formats",
            "Category:Organizations established in 1993",
            "Category:Lists by country",
            "Category:Redirects from Japanese-language terms",
            "Category:Non-transitive categories",
            "Category: Disambiguation pages",
            "Category:Arabic words and phrases",
            "Category:All articles lacking sources",
            "Categories: Letter-number combination disambiguation pages" )
            
        val dateMatcher = Pattern.compile( "[0-9]{4,4}" )
    }
        
    class Builder( val text : String )
    {
        
    
        val wordList = Utils.luceneTextTokenizer( Utils.normalize( text ) )
        
        var topicCategoryMap = TreeMap[Int, TreeMap[Int, Double]]()
        var topicNameMap = TreeMap[Int, String]()
        var contextWeightMap = TreeMap[Int, Double]()
        
        // All of these should be pre-processed into the db.
        def allowedPhrase( words : List[String] ) =
        {
            val allNumbers = words.foldLeft( true )( _ && _.matches( "[0-9]+" ) )
            !allNumbers
        }
        
        def allowedTopic( topicName : String ) =
        {
            val listOf = topicName.startsWith( "Main:List of" ) || topicName.startsWith( "Main:Table of" )
            val isCategory = topicName.startsWith( "Category:" )
            val postcodeArea = topicName.endsWith( "postcode area" )
            val isNovel = topicName.contains("(novel)")
            val isSong = topicName.contains("(song)")
            val isBand = topicName.contains("(band)")
            val isTVSeries = topicName.contains("TV series")
            val isAlbum = topicName.contains("(album)")
            
            !listOf && !isCategory && !postcodeArea && !isNovel && !isSong && !isBand && !isTVSeries && !isAlbum
        }
        
        def allowedContext( contextName : String ) =
        {
            val allowed = !Builder.disallowedCategories.contains( contextName ) && !Builder.dateMatcher.matcher(contextName).find()

            allowed
        }
        
        
        def build() : AmbiguityForest =
        {
            val phraseCountQuery = topicDb.prepare( "SELECT phraseCount FROM phraseCounts WHERE phraseId=?", Col[Int]::HNil )
            val topicQuery = topicDb.prepare( "SELECT t1.topicId, t1.count, t2.name FROM phraseTopics AS t1 INNER JOIN topics AS t2 on t1.topicId=t2.id WHERE phraseTreeNodeId=? ORDER BY count DESC LIMIT 50", Col[Int]::Col[Int]::Col[String]::HNil )
            val topicCategoryQuery = topicDb.prepare( "SELECT t1.contextTopicId, t1.weight, t2.name FROM linkWeights2 AS t1 INNER JOIN topics AS t2 ON t1.contextTopicId=t2.id WHERE topicId=? ORDER BY weight DESC LIMIT 50", Col[Int]::Col[Double]::Col[String]::HNil )
            val topicCategoryQueryReverse = topicDb.prepare( "SELECT topicId, weight FROM linkWeights2 AS t1 INNER JOIN topics AS t2 ON t1.contextTopicId=t2.id WHERE contextTopicId=? ORDER BY weight DESC LIMIT 50", Col[Int]::Col[Double]::Col[String]::HNil )
            
            var possiblePhrases = List[AmbiguityForest.SurfaceFormDetails]()
            var activePhrases = List[(Int, PhraseMapLookup#PhraseMapIter)]()
            
            var wordIndex = 0
            var topicSet = TreeSet[Int]()
            
            println( "Parsing text and building topic and category maps." )
            for ( word <- wordList )
            {
                println( "  " + word )
                val wordLookup = lookup.lookupWord( word )
                        
                wordLookup match
                {
                    case Some(wordId) =>
                    {
                        //println( "  matched" )
                        val newPhrase = lookup.getIter()
                        activePhrases = (wordIndex, newPhrase) :: activePhrases
                        
                        var newPhrases = List[(Int, PhraseMapLookup#PhraseMapIter)]()
                        for ( (fromIndex, phrase) <- activePhrases )
                        {
                            val phraseId = phrase.update(wordId)
                            if ( phraseId != -1 )
                            {
                                topicQuery.bind(phraseId)
                                val sfTopics = topicQuery.toList.filter( x => allowedTopic(_3(x).get) )
                                val toIndex = wordIndex
                                
                                //println( ":: " + phraseId)
                                if ( sfTopics != Nil && allowedPhrase(wordList.slice(fromIndex, toIndex+1)) ) 
                                {
                                    // This surface form has topics. Query phrase relevance and push back details
                                    phraseCountQuery.bind(phraseId)
                                    
                                    val phraseCount = _1(phraseCountQuery.onlyRow).get
                                    
                                    // TODO: Do something with this result
                                    //(fromIndex, toIndex, phraseCount, [(TopicId, TopicCount])
                                    val topicDetails = for ( td <- sfTopics.toList ) yield (_1(td).get, _2(td).get)
                                    
                                    
                                    
                                    val sfCount = topicDetails.foldLeft(0.0)( (x,y) => x + y._2 )
                                    val sfWeight = (sfCount.toDouble / phraseCount.toDouble)

                                    val topicDownWeight = if (AmbiguityForest.normaliseTopicWeightings) sfCount else 1.0
                                    val filteredTopics = topicDetails.map ( v => (v._1, v._2.toDouble / topicDownWeight.toDouble ) ).filter( v => v._2 > AmbiguityForest.minTopicRelWeight )
                                    val weightedTopics = filteredTopics.foldLeft( TreeMap[AmbiguityForest.TopicId, AmbiguityForest.Weight]() )( (x, y) => x.updated( y._1, y._2 ) )
                                    //val weightedTopics = for ( td <- topicDetails ) yield (td._1, td._2.toDouble / sfCount.toDouble)
                                    val thisSf = new AmbiguityForest.SurfaceFormDetails( fromIndex, toIndex, phraseId, sfWeight, weightedTopics )
                                    
                                    
                                    for ( (topicId, topicCount) <- topicDetails )
                                    {
                                        //println( "Querying context for: " + topicId )
                                        assert( topicId != 0 )
                                        topicSet = topicSet + topicId
                                        
                                        if ( !topicCategoryMap.contains(topicId) )
                                        {
                                            topicCategoryQuery.bind( topicId )

                                            // First order contexts
                                            var contextWeights = TreeMap[Int, Double]()
                                            
                                            var categorySet = HashSet[Int]()
                                            
                                            // Love closures
                                            def addContext( cid : Int, _weight : Double, name : String )
                                            {
                                                var weight = _weight
                                                if ( allowedContext(name) )
                                                {
                                                    val isCategory = name.startsWith( "Category:")
                                                    if ( isCategory ) categorySet += cid
                                                    if ( AmbiguityForest.upweightCategories && isCategory )
                                                    {
                                                        
                                                        //weight = 1.0
                                                        weight = sqrt( weight )
                                                    }
                                                    
                                                    if ( cid != topicId )
                                                    {
                                                        contextWeights = contextWeights.updated( cid, weight )
                                                    }
                                                }
                                            }
                                            
                                            for ( row <- topicCategoryQuery )
                                            {
                                                addContext( _1(row).get, _2(row).get, _3(row).get )
                                            }
                                            
                                            // Second order contexts
                                            if ( AmbiguityForest.secondOrderContexts && contextWeights.size < AmbiguityForest.secondOrderKickin )
                                            {
                                                var secondOrderWeights = TreeMap[Int, Double]()
                                                for ( (contextId, contextWeight) <- contextWeights.toList )
                                                {
                                                    // Don't follow categories for second order contexts. They generalise too quickly.
                                                    if ( !AmbiguityForest.secondOrderExcludeCategories || !categorySet.contains( contextId ) )
                                                    {
                                                        topicCategoryQuery.bind( contextId )
                                                        
                                                        for ( row <- topicCategoryQuery )
                                                        {
                                                            val cid = _1(row).get
                                                            val rawWeight = _2(row).get
                                                            val name = _3(row).get
                                                            
                                                            if ( !contextWeights.contains(cid) )
                                                            {
                                                                val weight = AmbiguityForest.secondOrderContextDownWeight * contextWeight * rawWeight
                                                                addContext( cid, weight, name )
                                                            }
                                                        }
                                                    }
                                                }
                                            }
                                            
                                            val topContextsByWeight = contextWeights.toList.sortWith( _._2 > _._2 ).slice(0, AmbiguityForest.numAllowedContexts)
                                            val reducedContextWeightMap = topContextsByWeight.foldLeft( TreeMap[Int, Double]() )( (m, v) => m.updated(v._1, v._2) )

                                            topicCategoryMap = topicCategoryMap.updated(topicId, reducedContextWeightMap)
                                            
                                            //println( topicId + ": " + topicCategoryIds )
                                            for ( (topicCategoryId, weight) <- contextWeights )
                                            {
                                                assert( topicCategoryId != 0 )
                                                topicSet = topicSet + topicCategoryId
                                            }
                                        }
                                    }

                                    //println( "-> " + wordList.slice(fromIndex, toIndex+1) )                              
                                    possiblePhrases = thisSf :: possiblePhrases
                                }
                                
                                newPhrases = (fromIndex, phrase) :: newPhrases
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
                if ( x.end != y.end )
                {
                    x.end > y.end
                }
                else
                {
                    x.start < y.start
                }
            } )
            
            println( "Building new ambiguity forest" )

            val f = new AmbiguityForest( wordList, topicNameMap, topicCategoryMap, topicDb )
            f.addTopics( possiblePhrasesSorted )
            //f.buildContextWeights()
            //f.applyContexts( topicDb )
            
            f.alternativeBasedResolution()
            //f.alternativeBasedResolution2()
            
            f
        }
                
        /*def resolve( maxAlternatives : Int ) : List[List[(Double, List[String], String)]] =
        {
            disambiguation.reverse
        }*/
    }
    
    def execute( inputText : String, debugOutFile : String, htmlOutFile : String )
    {
        val b = new Builder(inputText)
        val forest = b.build()
        forest.dumpDebug( debugOutFile )
        forest.htmlOutput( htmlOutFile )
    }
}


