package org.seacourt.disambiguator

import scala.collection.immutable.{TreeSet, TreeMap}
import java.util.{TreeMap => JTreeMap}
import math.{max, log, pow}
import java.io.{File, DataInputStream, FileInputStream}

import org.apache.commons.math.distribution.NormalDistributionImpl

import scala.xml.XML

import scala.util.matching.Regex
import scala.collection.mutable.{ListBuffer}

import org.seacourt.sql.SqliteWrapper._
import org.seacourt.utility.StopWords.stopWordSet
import org.seacourt.utility._

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

object AmbiguityForest
{
    val minPhraseWeight         = 0.005
    val minContextEdgeWeight    = 1.0e-6
    val numAllowedContexts      = 100
    val secondOrderContexts     = true
}


class SurfaceForm( val phraseId : Int, val phraseWeight : Double, val topics : List[(Int, Double)] )
{
    var topicWeights = TreeMap[Int, Double]()
}

class AltSite( val start : Int, val end : Int, val sf : SurfaceForm )
{
}

class AmbiguityAlternative( val sites : List[AltSite] )
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
    
    def buildCombinations()
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
                        val alternative = new AmbiguityAlternative( stack.map(asArr(_)).reverse.map( x => new AltSite( x._1, x._2, x._3 ) ) )
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



class AmbiguityForest( val words : List[String], val topicNameMap : TreeMap[Int, String], topicCategoryMap : TreeMap[Int, TreeMap[Int, Double]] )
{
    class SureSite( val start : Int, val end : Int, val topicId : Int, val weight : Double, val name : String ) {}
    
    var sites = List[AmbiguitySite]()
    var contextWeights = TreeMap[Int, Double]()
    var disambiguated = List[SureSite]()
    
    var debug = List[scala.xml.Elem]()
    
    def addTopics( sortedPhrases : List[(Int, Int, SurfaceForm)] )
    {
        debug =
            <surfaceForms>
                { for ( (startIndex, endIndex, sf) <- sortedPhrases ) yield
                    <element>
                        <phrase>{words.slice( startIndex, endIndex+1 ).mkString(" ")} </phrase>
                        <from>{startIndex}</from>
                        <to>{endIndex}</to>
                        <weight>{sf.phraseWeight}</weight>
                        <topics>
                            { for ((topicId, weight) <- sf.topics.toList.sortWith( _._2 > _._2 ).slice(0,4)) yield
                                <topic>
                                    <name>{topicNameMap(topicId)}</name>
                                    <weight>{weight}</weight>
                                    <contexts>
                                        {
                                            val elements = topicCategoryMap(topicId).toList.sortWith( _._2 > _._2 ).slice(0,4)
                                            for ( (contextId, weight) <- elements ) yield
                                                <context>
                                                    <name>{topicNameMap(contextId)}</name>
                                                    <weight>{weight}</weight>
                                                </context>
                                        }
                                    </contexts>
                                </topic>
                            }
                        </topics>
                    </element>
                }
            </surfaceForms> :: debug

        for ( (startIndex, endIndex, surfaceForm) <- sortedPhrases )
        {
            if ( surfaceForm.phraseWeight > AmbiguityForest.minPhraseWeight )
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
    
    def alternativeBasedResolution()
    {
        for ( site <- sites )
        {
            site.buildCombinations()
        }
        
        debug =
            <sites>
            {
                for ( site <- sites ) yield
                    <site>
                    {
                        for ( alternative <- site.combs ) yield
                        <alternative>
                        {
                            for ( altSite <- alternative.sites ) yield
                            <surfaceForm>
                                <name>{words.slice(altSite.start, altSite.end+1)}</name>
                                <topics>
                                {
                                    for ( (topicId, topicWeight) <- altSite.sf.topics.toList.sortWith( _._2 > _._2 ).slice(0,5) ) yield
                                    <element>
                                        <name>{topicNameMap(topicId)}</name>
                                        <weight>{topicWeight}</weight>
                                        <contexts>
                                        {
                                            for ( (contextId, contextWeight) <- topicCategoryMap(topicId).toList.sortWith( _._2 > _. _2 ).slice(0,5) ) yield
                                            <element>
                                                <name>{topicNameMap(contextId)}</name>
                                                <weight>{contextWeight}</weight>
                                            </element>
                                        }
                                        </contexts>
                                    </element>
                                }
                                </topics>
                            </surfaceForm>
                        } 
                        </alternative>
                    }
                    </site>
            }
            </sites> :: debug
        
        type ContextId = Int
        type SiteCentre = Double
        type EdgeWeight = Double
                
        // Build a map from contexts to weighted ambiguity alternatives
        class REdge( val site : AmbiguitySite, val alt : AmbiguityAlternative, val altSite : AltSite, val topicId : Int, val weight : Double ) {}
        
        println( "Building weighted context edges." )
        var reverseContextMap = TreeMap[ContextId, List[REdge]]()
        for ( site <- sites )
        {
            for ( alternative <- site.combs )
            {
                for ( altSite <- alternative.sites )
                {
                    val altWeight = alternative.sites.foldLeft(1.0)( _ * _.sf.phraseWeight )
                    for ( (topicId, topicWeight) <- altSite.sf.topics )
                    {
                        for ( (contextId, contextWeight) <- topicCategoryMap(topicId) )
                        {
                            val weight = altWeight * topicWeight * contextWeight
                            
                            
                            if ( weight > AmbiguityForest.minContextEdgeWeight )
                            {
                                val updatedList = new REdge(site, alternative, altSite, topicId, weight) :: reverseContextMap.getOrElse(contextId, List[REdge]() )
                                reverseContextMap = reverseContextMap.updated(contextId, updatedList)
                            }
                        }
                    }
                }
            }
        }
        
        // Shouldn't allow links from one alternative in an
        // SF to another alternative in the same SF. Also, within an alternative SHOULD allow
        // links between sites in that alternative

        // Weight each topic based on shared contexts (and distances to those contexts)
        println( "Building weighted topic edges." )
        val distWeighting = new NormalDistributionImpl( 0.0, 5.0 )
        
        var count = 0
        for ( (contextId, alternatives) <- reverseContextMap )
        {
            for ( edge1 <- alternatives )
            {
                count += 1
            }
        }
        println( "Contexts in play: " + reverseContextMap.size + ", inner count: " + count )
        
        for ( (contextId, alternatives) <- reverseContextMap )
        {
            for ( edge1 <- alternatives )
            {
                for ( edge2 <- alternatives )
                {
                    if ( (edge1.site != edge2.site) || (edge1.alt == edge2.alt && edge1.altSite.sf != edge2.altSite.sf) )
                    {
                        val center1 = (edge1.altSite.start + edge1.altSite.end).toDouble / 2.0
                        val center2 = (edge2.altSite.start + edge2.altSite.end).toDouble / 2.0
                        val distance = (center1 - center2)
                        val distanceWeight = 1.0 + 1000.0*distWeighting.density( distance )
                        //println( ":: " + distance + ", " + distanceWeight )
                        val totalWeight = (edge1.weight * edge2.weight) * distanceWeight
                        edge1.alt.weight += totalWeight
                        edge2.alt.weight += totalWeight
                        
                        edge1.altSite.sf.topicWeights = edge1.altSite.sf.topicWeights.updated( edge1.topicId, edge1.altSite.sf.topicWeights.getOrElse( edge1.topicId, 0.0 ) + totalWeight )
                        edge2.altSite.sf.topicWeights = edge2.altSite.sf.topicWeights.updated( edge2.topicId, edge2.altSite.sf.topicWeights.getOrElse( edge2.topicId, 0.0 ) + totalWeight )
                    }
                }
            }
        }
        
        println( "Dumping resolutions." )
        dumpResolutions()
        
        for ( site <- sites )
        {
            val weightedAlternatives = site.combs.sortWith( _.weight > _.weight )
            val canonicalAlternative = weightedAlternatives.head
            for ( altSite <- canonicalAlternative.sites.reverse )
            {
                if ( altSite.sf.topicWeights.size > 0 )
                {
                    val canonicalTopic = altSite.sf.topicWeights.map( x =>(x._2, x._1) ).last
                    val topicId = canonicalTopic._2
                    val weight = canonicalTopic._1
                    disambiguated = new SureSite( altSite.start, altSite.end, topicId, weight, topicNameMap(topicId) ) :: disambiguated
                }
            }
        }
        
        println( "Building topic and context association graph." )
        //var reverseContextMap = TreeMap[ContextId, List[REdge]]()
        /*var weightMap = TreeMap[ContextId, List[(ContextId, Double)]]()
        for ( ss <- disambiguated )
        {
            for ( (contextId, weight) <- topicCategoryMap(ss.topicId) )
            {
                weightMap = weightMap.updated( ss., weightMap.getOrElse( (contextId, weight), List[(Int, Double)]() ) )
            }
        }
        
        for ( ss <- disambiguated )
        {
            for ( fromId <- topicCategoryMap(ss.topicId) )
            {
                for ( (
            }
        }*/
    }
    
    
    def dumpResolutions()
    {
        debug =
            <resolutions>
            {
                for ( site <- sites.reverse ) yield
                {
                    val weightedAlternatives = site.combs.sortWith( _.weight > _.weight )
                    <site>
                        <phrase>{ words.slice( site.start, site.end+1 ).mkString(" ") }</phrase>
                        {
                            for ( wa <- weightedAlternatives ) yield
                                <alternative>
                                    <weight>{wa.weight}</weight>
                                    {
                                        for ( altSite <- wa.sites ) yield
                                            <element>
                                                <text>{words.slice( altSite.start, altSite.end+1 ).mkString(" ")}</text>
                                                <topics>
                                                {
                                                    for ( (id, weight) <- altSite.sf.topicWeights.toList.sortWith( _._2 > _._2 ).slice(0, 5) ) yield
                                                        <element>
                                                            <name>{topicNameMap(id)}</name>
                                                            <weight>{weight}</weight>
                                                        </element>
                                                }
                                                </topics>
                                            </element>
                                    }
                                </alternative>
                        }
                    </site>
                }
            }
            </resolutions> :: debug
    }
    
    
    def dumpDebug( fileName : String )
    {
        val fullDebug =
            <root>
            { for ( el <-debug.reverse ) yield el }
            </root>

        XML.save( fileName, fullDebug, "utf8" )
    }
    
    def htmlOutput( fileName : String )
    {
        val wordArr = words.toArray
        var i = 0
        var phraseIt = disambiguated
                
        val l = ListBuffer[scala.xml.Elem]()
        while ( i < wordArr.size )
        {
            if ( phraseIt != Nil && phraseIt.head.start == i )
            {
                val resolution = phraseIt.head
                val topicLink = if (resolution.name.startsWith("Main:")) resolution.name.drop(5) else resolution.name
                val size = 18.0 + log(resolution.weight)
                val fs = "font-size:"+size.toInt
                l.append(
                    <a href={ "http://en.wikipedia.org/wiki/" + topicLink } title={"Weight: " + resolution.weight } style={fs} >
                        {"[" + words.slice( resolution.start, resolution.end+1 ).mkString(" ") + "] "}
                    </a> )
                i = resolution.end+1
                phraseIt = phraseIt.tail
            }
            else
            {
                l.append( <span>{wordArr(i) + " " }</span> )
                i += 1
            }
        }
        
        val output =
            <html>
                <head></head>
                <body> { l.toList } </body>
            </html>
            
        XML.save( fileName, output, "utf8" )
    }

}

class Disambiguator( phraseMapFileName : String, topicFileName : String )
{
    val lookup = new PhraseMapLookup()
    lookup.load( new DataInputStream( new FileInputStream( new File( phraseMapFileName ) ) ) )
    
    val topicDb = new SQLiteWrapper( new File(topicFileName) )
    topicDb.exec( "PRAGMA cache_size=2000000" )

    
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
                val weight1 = _2(category).get
                val weight2 = _3(category).get
                println( "    " + name + ": " + weight1 + ", " + weight2 )
            }
        }
    }

        
    class Builder( val text : String )
    {
        val wordList = Utils.luceneTextTokenizer( Utils.normalize( text ) )
        
        var topicCategoryMap = TreeMap[Int, TreeMap[Int, Double]]()
        var topicNameMap = TreeMap[Int, String]()
        var contextWeightMap = TreeMap[Int, Double]()
        
        def build() : AmbiguityForest =
        {
            val phraseCountQuery = topicDb.prepare( "SELECT phraseCount FROM phraseCounts WHERE phraseId=?", Col[Int]::HNil )
            val topicQuery = topicDb.prepare( "SELECT topicId, count FROM phraseTopics WHERE phraseTreeNodeId=? ORDER BY count DESC LIMIT 50", Col[Int]::Col[Int]::HNil )
            val topicCategoryQuery = topicDb.prepare( "SELECT contextTopicId, weight FROM linkWeights2 WHERE topicId=? ORDER BY weight DESC LIMIT 50", Col[Int]::Col[Double]::HNil )
            
            var possiblePhrases = List[(Int, Int, SurfaceForm)]()
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

                                    val weightedTopics = for ( td <- topicDetails ) yield (td._1, td._2.toDouble / sfCount.toDouble)
                                    val thisSf = new SurfaceForm( phraseId, sfWeight, weightedTopics )
                                    
                                    for ( (topicId, topicCount) <- topicDetails )
                                    {
                                        assert( topicId != 0 )
                                        topicSet = topicSet + topicId
                                        if ( !topicCategoryMap.contains(topicId) )
                                        {
                                            topicCategoryQuery.bind( topicId )

                                            // First order contexts
                                            var contextWeights = TreeMap[Int, Double]()
                                            for ( row <- topicCategoryQuery )
                                            {
                                                val cid = _1(row).get
                                                val weight1 = _2(row).get
                                                //val weight2 = _3(row).get
                                                
                                                contextWeights = contextWeights.updated( cid, weight1 )
                                            }
                                            
                                            // Second order contexts
                                            if ( AmbiguityForest.secondOrderContexts )
                                            {
                                                var secondOrderWeights = TreeMap[Int, Double]()
                                                for ( (contextId, contextWeight) <- contextWeights.toList )
                                                {
                                                    topicCategoryQuery.bind( contextId )
                                                    
                                                    for ( row <- topicCategoryQuery )
                                                    {
                                                        val cid = _1(row).get
                                                        val weight1 = _2(row).get
                                                        //val weight2 = _3(row).get
                                                        
                                                        if ( !contextWeights.contains(cid) )
                                                        {
                                                            // 2nd order context weights are lowered by the weight of the first link
                                                            contextWeights = contextWeights.updated( cid, contextWeight * weight1 )
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
                                                                        
                                    possiblePhrases = (fromIndex, toIndex, thisSf) :: possiblePhrases
                                    
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
                else
                {
                    x._1 < y._1
                }
            } )
            
            println( "Building new ambiguity forest" )

            val f = new AmbiguityForest( wordList, topicNameMap, topicCategoryMap )
            f.addTopics( possiblePhrasesSorted )
            //f.buildContextWeights()
            //f.applyContexts( topicDb )
            
            f.alternativeBasedResolution()
            
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


