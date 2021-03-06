package org.seacourt.disambiguator

import scala.collection.mutable.{MultiMap, Stack, ArrayBuffer}
import scala.collection.immutable.{TreeSet, TreeMap, HashSet, HashMap}
import java.util.{TreeMap => JTreeMap}
import math.{max, log, pow, sqrt}
import java.io.{File, DataInputStream, FileInputStream}
import java.util.regex.Pattern

import org.apache.commons.math.distribution.NormalDistributionImpl

import scala.xml.XML
import com.weiglewilczek.slf4s.{Logging}

import scala.util.matching.Regex
import scala.collection.mutable.{ListBuffer, Queue}

import org.seacourt.sql.SqliteWrapper._
import org.seacourt.utility.StopWords.stopWordSet
import org.seacourt.utility._
import org.seacourt.disambiguator.Community._
import org.seacourt.disambiguator.CategoryHierarchy.{CategoryHierarchy, Builder => CHBuilder}

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

object Disambiguator
{
    val disallowedCategories = HashSet[String](
        "Category:Main topic classifications",
        "Category:Main topic classifications",
        "Category:Fundamental categories",
        "Category:Categories",
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
        val allowed = !disallowedCategories.contains( contextName ) && !dateMatcher.matcher(contextName).find() && !contextName.contains("redirect")

        allowed
    }
}

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
                                        valid = false
                                    }
                                }
                            }
                            
                            stack = j :: stack
                            //seenStartSet = seenStartSet + j
                            found = true
                        }
                    }
                    
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


class Disambiguator( phraseMapFileName : String, topicFileName : String, categoryHierarchyFileName : String )
{
    val lookup = new PhraseMapLookup()
    lookup.load( new DataInputStream( new FileInputStream( new File( phraseMapFileName ) ) ) )
    
    val topicDb = new SQLiteWrapper( new File(topicFileName) )
    topicDb.exec( "PRAGMA cache_size=2000000" )

    val phraseTopicsDb = new EfficientArray[EfficientIntIntInt](0)
    val phraseCountsDb = new EfficientArray[EfficientIntPair](0)
    val linkWeightDb = new EfficientArray[EfficientIntIntDouble](0)
    
    println( "Loading all data in..." )
    phraseTopicsDb.load( new DataInputStream( new FileInputStream( new File( "./DisambigData/phraseTopics.bin" ) ) ) )
    phraseCountsDb.load( new DataInputStream( new FileInputStream( new File( "./DisambigData/phraseCounts.bin" ) ) ) )
    linkWeightDb.load( new DataInputStream( new FileInputStream( new File( "./DisambigData/linkWeights.bin" ) ) ) )
    println( "   complete." )    
    
    val categoryHierarchy = new CategoryHierarchy( categoryHierarchyFileName, topicDb )
    
    def phraseQuery( phrase : String ) =
    {
        val id = lookup.getIter().find( phrase )
        val sfQuery = topicDb.prepare( "SELECT phraseCount FROM phraseCounts WHERE phraseId=?", Col[Int]::HNil )
        sfQuery.bind(id)
        val relevance = _1(sfQuery.onlyRow).get
        
        val topicQuery = topicDb.prepare( "SELECT t2.name, t1.topicId, t1.count FROM phraseTopics AS t1 INNER JOIN topics AS t2 ON t1.topicId=t2.id WHERE t1.phraseTreeNodeId=? ORDER BY t1.count DESC", Col[String]::Col[Int]::Col[Int]::HNil )
        topicQuery.bind(id)

        
        //val categoryQuery = topicDb.prepare( "SELECT t1.name, t3.weight1, t3.weight2 FROM topics AS t1 INNER JOIN categoriesAndContexts AS t2 ON t1.id=t2.contextTopicId INNER JOIN linkWeights AS t3 ON t3.topicId=t2.topicId AND t3.contextTopicId=t2.contextTopicId WHERE t2.topicId=? ORDER BY t1.name ASC", Col[String]::Col[Double]::Col[Double]::HNil )
        val categoryQuery = topicDb.prepare( "SELECT t1.name, t2.weight1, t2.weight2 FROM topics AS t1 INNER JOIN linkWeights AS t2 ON t1.id=t2.contextTopicId WHERE t2.topicId=? ORDER BY t1.name ASC", Col[String]::Col[Double]::Col[Double]::HNil )

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
                val weight = _2(category).get min _3(category).get
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

        
    class Builder( val text : String ) extends Logging
    {
        val wordList = TextUtils.luceneTextTokenizer( TextUtils.normalize( text ) )
        
        var topicCategoryMap = HashMap[Int, HashMap[Int, Double]]()
        var topicNameMap = HashMap[Int, String]()
        var contextWeightMap = HashMap[Int, Double]()
        
        
        private val phraseCountQuery = topicDb.prepare( "SELECT phraseCount FROM phraseCounts WHERE phraseId=?", Col[Int]::HNil )
        private val topicQuery = topicDb.prepare( "SELECT t1.topicId, t1.count, t2.name FROM phraseTopics AS t1 INNER JOIN topics AS t2 on t1.topicId=t2.id WHERE phraseTreeNodeId=? ORDER BY count DESC LIMIT 50", Col[Int]::Col[Int]::Col[String]::HNil )
        private val topicCategoryQuery = topicDb.prepare( "SELECT t1.contextTopicId, MIN(t1.weight1, t1.weight2) AS weight, t2.name FROM linkWeights AS t1 INNER JOIN topics AS t2 ON t1.contextTopicId=t2.id WHERE topicId=? ORDER BY weight DESC LIMIT 50", Col[Int]::Col[Double]::Col[String]::HNil )
        
        private def getPhraseTopics( phraseId : Int ) =
        {
            if ( false )
            {
                topicQuery.bind(phraseId)
                val sfTopics = topicQuery.toList.filter( x => Disambiguator.allowedTopic(_3(x).get) )
                sfTopics.toList.map( row => (_1(row).get, _2(row).get ) )
            }
            else
            {
                var index = Utils.lowerBound( new EfficientIntIntInt( phraseId, 0, 0 ), phraseTopicsDb, (x:EfficientIntIntInt, y:EfficientIntIntInt) => x.less(y) )
                var res = ListBuffer[(Int, Int)]()
                while ( phraseTopicsDb(index).first == phraseId )
                {
                    val row = phraseTopicsDb(index)
                    res.append( (row.second, row.third) )
                    index += 1
                }
                
                res.toList
            }
        }
        
        private def getPhraseCount( phraseId : Int ) =
        {
            if ( false )
            {
                phraseCountQuery.bind(phraseId)
                _1(phraseCountQuery.onlyRow).get
            }
            else
            {
                var index = Utils.lowerBound( new EfficientIntPair( phraseId, 0 ), phraseCountsDb, (x:EfficientIntPair, y:EfficientIntPair) => x.less(y) )
                val res = phraseCountsDb(index)
                assert( res.first == phraseId )
                res.second
            }
        }
        
        private def getTopicContexts( topicId : Int ) =
        {
            if ( false )
            {
                topicCategoryQuery.bind( topicId )
                
                val res = ListBuffer[(Int, Double)]()
                for ( row <- topicCategoryQuery )
                {
                    val contextId = _1(row).get
                    var weight = _2(row).get
                    val name = _3(row).get
                    
                    if ( Disambiguator.allowedContext(name) )
                    {
                        val isCategory = name.startsWith( "Category:")
                        if ( AmbiguityForest.upweightCategories && isCategory )
                        {
                            weight = sqrt( weight )
                        }
                        
                        res.append( (contextId, weight) )
                    }
                }
                
                res.toList
            }
            else
            {
                var index = Utils.lowerBound( new EfficientIntIntDouble( topicId, 0, 0.0 ), linkWeightDb, (x:EfficientIntIntDouble, y:EfficientIntIntDouble) => x.less(y) )
                var res = ListBuffer[(Int, Double)]()
                while ( linkWeightDb(index).first == topicId )
                {
                    val row = linkWeightDb(index)
                    res.append( (row.second, row.third) )
                    index += 1
                }
                
                res.toList
            }
        }
        
        def build() : AmbiguityForest =
        {
            var possiblePhrases = List[AmbiguityForest.SurfaceFormDetails]()
            var activePhrases = List[(Int, PhraseMapLookup#PhraseMapIter)]()
            
            var wordIndex = 0
            var topicSet = TreeSet[Int]()
            
            logger.info( "Parsing text and building topic and category maps." )
            for ( word <- wordList.slice(0, AmbiguityForest.maxNumberOfWords) )
            {
                //logger.info( "Reading: " + word )
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
                                val sfTopics = getPhraseTopics(phraseId)
                                val toIndex = wordIndex
                                
                                //println( ":: " + phraseId)
                                if ( sfTopics != Nil && Disambiguator.allowedPhrase(wordList.slice(fromIndex, toIndex+1)) ) 
                                {
                                    // This surface form has topics. Query phrase relevance and push back details
                                    val phraseCount = getPhraseCount(phraseId)
                                    
                                    // TODO: Do something with this result
                                    //(fromIndex, toIndex, phraseCount, [(TopicId, TopicCount])
                                    val topicDetails = for ( td <- sfTopics.toList ) yield (td._1, td._2)
                                    
                                    
                                    
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
                                            

                                            // First order contexts
                                            var contextWeights = TreeMap[Int, Double]()

                                            // Love closures
                                            def addContext( cid : Int, _weight : Double )
                                            {
                                                var weight = _weight
                                                
                                                if ( cid != topicId )
                                                {
                                                    contextWeights = contextWeights.updated( cid, weight )
                                                }
                                            }
                                                                                        
                                            getTopicContexts( topicId ).foreach( x => addContext( x._1, x._2 ) )
                                            
                                            // Second order contexts
                                            if ( AmbiguityForest.secondOrderContexts && contextWeights.size < AmbiguityForest.secondOrderKickin )
                                            {
                                                var secondOrderWeights = TreeMap[Int, Double]()
                                                for ( (contextId, contextWeight) <- contextWeights.toList )
                                                {
                                                    // Don't follow categories for second order contexts. They generalise too quickly.
                                                    getTopicContexts( contextId ).foreach( row =>
                                                    {
                                                        val cid = row._1
                                                        val rawWeight = row._2
                                                        //val name = row._3
                                                        
                                                        // Second order contexts cannot be categories
                                                        if ( !contextWeights.contains(cid) )//&& !name.startsWith("Category:") )
                                                        {
                                                            val weight = AmbiguityForest.secondOrderContextDownWeight * contextWeight * rawWeight
                                                            addContext( cid, weight )
                                                        }
                                                    } )
                                                }
                                            }
                                            
                                            val topContextsByWeight = contextWeights.toList.sortWith( _._2 > _._2 ).slice(0, AmbiguityForest.numAllowedContexts)
                                            val reducedContextWeightMap = topContextsByWeight.foldLeft( HashMap[Int, Double]() )( (m, v) => m.updated(v._1, v._2) )

                                            topicCategoryMap = topicCategoryMap.updated(topicId, reducedContextWeightMap)
                                            
                                            //println( topicId + ": " + topicCategoryIds )
                                            for ( (topicCategoryId, weight) <- topContextsByWeight )
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
            
            logger.info( "Looking up topic names for " + topicSet.size + " topics." )
            //val topicNameQuery = topicDb.prepare( "SELECT t1.name, t2.count FROM topics AS t1 LEFT JOIN numTopicsForWhichThisIsAContext AS t2 on t1.id=t2.topicId WHERE id=?", Col[String]::Col[Int]::HNil )
            val topicNameQuery = topicDb.prepare( "SELECT name FROM topics WHERE id=?", Col[String]::HNil )
           
            if ( false )
            { 
                logger.info( "Build category graph." )
                val fullGraph = categoryHierarchy.toTop( topicSet, (f, t, w) => w )
                logger.info( "  complete..." )
                
                logger.info( "Build and run category hierarchy" )
                val b = new CHBuilder( topicSet, fullGraph, id => id.toString )
                logger.info( "  run..." )
                val maxTopicDistance = 6.0
                b.run( (x,y) => 1, maxTopicDistance )
                logger.info( "  complete..." )
            }
            
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
            
            logger.info( "Building new ambiguity forest" )

            val f = new AmbiguityForest( wordList, topicNameMap, topicCategoryMap, topicDb, categoryHierarchy )
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
    
    def execute( inputText : String, debugOutFile : String, htmlOutFile : String, resOutFile : String )
    {
        val b = new Builder(inputText)
        val forest = b.build()
        forest.dumpDebug( debugOutFile )
        forest.output( htmlOutFile, resOutFile )
    }
}


