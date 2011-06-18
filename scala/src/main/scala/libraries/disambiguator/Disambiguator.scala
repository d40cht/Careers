package org.seacourt.disambiguator

import scala.collection.immutable.{TreeSet, TreeMap}
import java.util.{TreeMap => JTreeMap}
import math.{max, log}
import java.io.{File, DataInputStream, FileInputStream}

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
                        <weight>{sf.phraseWeight}</weight>
                    </element>
                }
            </surfaceForms> :: debug

        for ( (startIndex, endIndex, surfaceForm) <- sortedPhrases )
        {
            if ( surfaceForm.phraseWeight > 0.005 )
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
    
    def buildContextWeights( topicCategoryMap : TreeMap[Int, TreeMap[Int, Double]] )
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
                        var contexts = topicCategoryMap(topicId)
                        
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
        
        debug =
            <contexts>
                {
                    val reversed = contextWeights.toList.map( (v) => (v._2, v._1) ).sortWith( (x,y) => (x._1 > y._1) )
                    for ( (weight, contextId) <- reversed.slice(0, 200) ) yield
                        <element>
                            <name>{topicNameMap(contextId)}</name>
                            <weight>{weight}</weight>
                            <origins>
                                { for ( x <- siteOrigination(contextId) ) yield <element>{words.slice(x._1, x._2+1).mkString(" ")}</element> }
                            </origins>
                        </element>
                }
            </contexts> :: debug
    }
    
    def applyContexts( topicCategoryMap : TreeMap[Int, TreeMap[Int, Double]], topicDb : SQLiteWrapper )
    {
        val reversed = contextWeights.toList.map( (v) => (v._2, v._1) ).sortWith( (x,y) => (x._1 > y._1) )

        for ( (weight, contextId) <- reversed.slice(0,200) )
        {
            //println( "Applying: " + topicNameMap(contextId) + ": " + weight )
            for ( site <- sites )
            {   
                for ( alternative <- site.combs )
                {
                    for ( (startIndex, endIndex, sf) <- alternative.sites )
                    {
                        for ( (topicId, topicWeight) <- sf.topics )
                        {
                            val contexts = topicCategoryMap( topicId )
                            
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
        
        debug =
            <resolutions>
            {
                for ( site <- sites ) yield
                {
                    val weightedAlternatives = site.combs.sortWith( _.weight > _.weight )
                    <site>
                        <phrase>{ words.slice( site.start, site.end+1 ).mkString(" ") }</phrase>
                        {
                            for ( wa <- weightedAlternatives ) yield
                                <alternative>
                                    <weight>{wa.weight}</weight>
                                    {
                                        for ( site <- wa.sites ) yield
                                            <text>{words.slice( site._1, site._2+1 ).mkString(" ")}</text>
                                            <topics>
                                            {
                                                for ( (weight, id) <- site._3.topicWeights.map( x =>(-x._2, x._1) ).toList.slice(0, 5) ) yield
                                                    <element>
                                                        <name>{topicNameMap(id)}</name>
                                                        <weight>{-weight}</weight>
                                                    </element>
                                            }
                                            </topics>
                                    }
                                </alternative>
                        }
                    </site>
                }
            }
            </resolutions> :: debug
            
        for ( site <- sites )
        {
            val weightedAlternatives = site.combs.sortWith( _.weight > _.weight )
            val canonicalAlternative = weightedAlternatives.head
            for ( site <- canonicalAlternative.sites )
            {
                val start = site._1
                val end = site._2
                val sf = site._3
                if ( sf.topicWeights.size > 0 )
                {
                    val canonicalTopic = sf.topicWeights.map( x =>(x._2, x._1) ).last
                    val topicId = canonicalTopic._2
                    val weight = canonicalTopic._1
                    disambiguated = new SureSite( start, end, topicId, weight, topicNameMap(topicId) ) :: disambiguated
                }
            }
        }
        
        // Now build a context set for these disambiguated sites and do the SCC building.
        var localContextMap = topicCategoryMap
        
        type SCCGraph = Graph[(Int, String, Double)]
        type SCCNode = SCCGraph#Node
        
        var nodesById = TreeMap[Int, SCCNode]()
        val g = new SCCGraph()
        for ( sureSite <- disambiguated )
        {
            val topicId = sureSite.topicId
            val weight = sureSite.weight
            
            if ( !nodesById.contains( topicId ) )
            {
                nodesById = nodesById.updated( topicId, g.addNode( (topicId, topicNameMap(topicId), weight) ) )
                for ( (contextId, contextWeight) <- topicCategoryMap(topicId) )
                {
                    if ( !nodesById.contains( contextId ) )
                    {
                        nodesById = nodesById.updated( contextId, g.addNode( contextId, topicNameMap(contextId), weight * contextWeight ) )
                    }
                }
            }
        }
        
        // Lookup these additional contexts and add them into localContextMap
        val topicCategoryQuery = topicDb.prepare( "SELECT contextTopicId, weight1, weight2 FROM linkWeights WHERE topicId=?", Col[Int]::Col[Double]::Col[Double]::HNil )
        var contextOrigination = TreeMap[Int, TreeSet[Int]]()
        for ( (id, node) <- nodesById )
        {
            //if ( !localContextMap.contains( id ) )
            {
                topicCategoryQuery.bind( id )
                
                var contexts = TreeMap[Int, Double]()
                for ( row <- topicCategoryQuery )
                {
                    val contextId = _1(row).get
                    val weight = _2(row).get
                    contexts = contexts.updated( contextId, weight )
                    
                    val prev = contextOrigination.getOrElse( contextId, TreeSet[Int]() )
                    
                    contextOrigination = contextOrigination.updated( contextId, prev + id )
                }
                
                localContextMap = localContextMap.updated( id, contexts )
            }
        }
        
        for ( (id, fromNode) <- nodesById )
        {
            val children = localContextMap( id )
            
            for ( (contextId, weight) <- children )
            {
                if ( nodesById.contains(contextId) && contextOrigination(contextId).size > 1 )
                {
                    val toNode = nodesById(contextId)
                    
                    if ( weight > 0.1 )
                    {
                        g.addEdge( fromNode, toNode )
                    }
                }
            }
        }
        
        debug =
            <contextGraph>
                <nodes>
                {
                    for ( node <- g.nodes ) yield
                        <topic>
                            <id>{node.info._1}</id>
                            <name>{node.info._2}</name>
                            <weight>{node.info._3}</weight>
                            <sinks>
                            {
                                for ( s <- node.sinks ) yield <element>{s.info._1}</element>
                            }
                            </sinks>
                        </topic>
                }
                </nodes>
            </contextGraph> :: debug
        
        // Get SCCs
        val sccs = g.connected( 1 )
        var report = List[(Double, String)]()
        for ( els <- sccs )
        {           
            if ( els.size > 1 )
            {
                var maxWeight = 0.0
                var names = List[(String, Double)]()

                for ( el <- els )
                {
                    val (id, name, weight) = el.info
                    maxWeight = maxWeight max weight
                    
                    names = (name, weight) :: names
                }
                
                report = (maxWeight, names.sortWith(_._2 > _._2).map( x => x._1 + ": " + x._2 ).mkString(", ")) :: report
            }
        }
        
        // Order by weight
        val sortedReport = report.sortWith( _._1 < _._1 )
        
        println( "Canonical non-unit SCCs: " )
        for ( (weight, names) <- sortedReport )
        {
            println( "))) " + weight + ": " + names )
        }   
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
            val resolution = phraseIt.head
            if ( resolution.start == i )
            {
                val topicLink = if (resolution.name.startsWith("Main:")) resolution.name.drop(5) else resolution.name
                l.append(
                    <a href={ "http://en.wikipedia.org/wiki/" + topicLink }>
                        {words.slice( resolution.start, resolution.end+1 ).mkString(" ")}
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
        
        def build() : AmbiguityForest =
        {
            val phraseCountQuery = topicDb.prepare( "SELECT phraseCount FROM phraseCounts WHERE phraseId=?", Col[Int]::HNil )
            // numTopicsForWhichThisIsAContext
            val topicQuery = topicDb.prepare( "SELECT topicId, count FROM phraseTopics WHERE phraseTreeNodeId=? ORDER BY count DESC", Col[Int]::Col[Int]::HNil )
            val topicCategoryQuery = topicDb.prepare( "SELECT contextTopicId, weight1, weight2 FROM linkWeights WHERE topicId=?", Col[Int]::Col[Double]::Col[Double]::HNil )
            
            //val normalisationQuery = topicDb.prepare( "SELECT MAX(count) FROM numTopicsForWhichThisIsAContext", Col[Int]::HNil )
            
            var possiblePhrases = List[(Int, Int, SurfaceForm)]()
            var activePhrases = List[(Int, PhraseMapLookup#PhraseMapIter)]()
            
            var wordIndex = 0
            var topicSet = TreeSet[Int]()
            
            //val maxCategoryCount = _1(normalisationQuery.onlyRow).get
            
            println( "Parsing text:" )
            //var surfaceFormMap = TreeMap[Int, SurfaceForm]()
            for ( word <- wordList )
            {
                //println( "  " + word )
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
            val f = new AmbiguityForest( wordList, topicNameMap )
            f.addTopics( possiblePhrasesSorted )
            f.buildContextWeights( topicCategoryMap )
            f.applyContexts( topicCategoryMap, topicDb )
            
            f
        }
        
        
        /*def resolve( maxAlternatives : Int ) : List[List[(Double, List[String], String)]] =
        {
            disambiguation.reverse
        }*/
    }
}


