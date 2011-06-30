package org.seacourt.disambiguator

import scala.collection.mutable.MultiMap
import scala.collection.immutable.{TreeSet, TreeMap, HashSet, HashMap}
import java.util.{TreeMap => JTreeMap}
import math.{max, log, pow}
import java.io.{File, DataInputStream, FileInputStream}




import org.apache.commons.math.distribution.NormalDistributionImpl

import scala.xml.XML

import scala.util.matching.Regex
import scala.collection.mutable.{ListBuffer, Queue}

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


class SurfaceForm( val phraseId : Int, val phraseWeight : Double, var topics : TreeMap[Int, Double] )
{
    // Used by first alternative algo
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


class RunQ[V]()
{    
    type K = Double
    private var container = TreeMap[K, HashSet[V]]()
    
    def add( k : K, v : V )
    {
        val el = container.getOrElse( k, HashSet[V]() )
        container = container.updated( k, el + v )
    }
    
    def remove( k : K, v : V )
    {
        val el = container(k)
        if ( el.size == 1 )
        {
            container = container - k
        }
        else
        {
            container = container.updated( k, el - v )
        }   
    }
    def first() : (K, V) =
    {
        val h = container.head
        
        (h._1, h._2.head)
    }
    def isEmpty : Boolean = container.isEmpty
}

class WeightedTopic( val site : AmbiguitySite, val alt : AmbiguityAlternative, val altSite : AltSite, val topicId : Int )
{
    var weight = 0.0
    var enabled = true
    var alone = false
    var linked = List[(WeightedTopic, Double)]()
    
    def addLink( other : WeightedTopic, linkWeight : Double )
    {
        weight += linkWeight
        linked = (other, linkWeight) :: linked
    }
}

class WeightedAlternative( val site : AmbiguitySite, val alternative : AmbiguityAlternative )
{
    var weight = 0.0
    var enabled = true
    var linked = HashMap[WeightedAlternative, Double]()
    
    def addLink( to : WeightedAlternative, weightinc : Double )
    {
        val oldWeight = linked.getOrElse( to, 0.0 )
        linked = linked.updated( to, oldWeight + weight )
        weight += weightinc
        to.weight += weightinc
    }
}

class AmbiguityForest( val words : List[String], val topicNameMap : TreeMap[Int, String], topicCategoryMap : TreeMap[Int, TreeMap[Int, Double]], val topicDb : SQLiteWrapper )
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
    
    def alternativeBasedResolution2()
    {
        println( "Building site alternatives." )
        for ( site <- sites )
        {
            site.buildCombinations()
        }
        
        type ContextId = Int
        
        println( "Building weighted topics and context map." )
        var weightedTopics = List[WeightedTopic]()
        var reverseContextMap = Map[ContextId, List[(WeightedTopic, Double)]]()
        for ( site <- sites; alternative <- site.combs; altSite <- alternative.sites )
        {
            val altWeight = alternative.sites.foldLeft(1.0)( _ * _.sf.phraseWeight )
            for ( (topicId, topicWeight) <- altSite.sf.topics )
            {
                val currTopic = new WeightedTopic( site, alternative, altSite, topicId )
                weightedTopics = currTopic :: weightedTopics
                for ( (contextId, contextWeight) <- topicCategoryMap(topicId) )
                {
                    val weight = altWeight * topicWeight * contextWeight
                    
                    if ( weight > AmbiguityForest.minContextEdgeWeight )
                    {
                        val existingTopics = reverseContextMap.getOrElse( contextId, List[(WeightedTopic, Double)]() )
                        reverseContextMap = reverseContextMap.updated( contextId, (currTopic, weight) :: existingTopics )
                    }
                }
            }
        }
        
        println( "Building topic edge graph." )
        var edgeCount = 0
        for ( (contextId, tws) <- reverseContextMap )
        {
            for ( (wt1, weight1) <- tws; (wt2, weight2) <-tws )
            {
                if ( (wt1.site != wt2.site) || (wt1.alt == wt2.alt && wt1.altSite != wt2.altSite) )
                {
                    if ( wt1.topicId != wt2.topicId )
                    {
                        val combinedWeight = weight1 * weight2
                        wt1.addLink( wt2, combinedWeight )
                        wt2.addLink( wt1, combinedWeight )
                        edgeCount += 1
                    }
                }
            }
        }
        println( "Number of edges: " + edgeCount )
        
        
        {
            println( "Thinning down to one topic per alt site" )
            val q = new RunQ[WeightedTopic]()
            
            for ( w <- weightedTopics ) q.add( w.weight, w )
            
            // Thin down to one topic per altSite
            while ( !q.isEmpty )
            {
                // Get the current lowest weighted WeightTopic
                val (weight, wt) = q.first()
                q.remove( weight, wt )
                
                //println( "First weight: " + weight )
                
                val lastTopicInAltSite = wt.altSite.sf.topics.size == 1
                if ( !lastTopicInAltSite )
                {
                    // Remove its siblings and down-weight them
                    for ( (child, linkWeight) <- wt.linked )
                    {
                        if ( child.enabled && ! child.alone )
                        {
                            q.remove( child.weight, child )
                            child.weight -= linkWeight
                            q.add( child.weight, child )
                        }
                    }
                    
                    wt.altSite.sf.topics = wt.altSite.sf.topics - wt.topicId
                }
                else
                {
                    wt.alone = true
                }
                wt.enabled = false
            }
        }
        
        weightedTopics = weightedTopics.filter( _.alone )
        
        {
            println( "Resolving..." )
            var lookup = HashMap[AmbiguityAlternative, WeightedAlternative]()
            for ( w <- weightedTopics )
            {
                if ( !lookup.contains( w.alt ) )
                {
                    lookup = lookup.updated( w.alt, new WeightedAlternative( w.site, w.alt ) )
                }
                val fromAlt = lookup(w.alt)
                for ( (toW, weight) <- w.linked )
                {
                    if ( !lookup.contains( toW.alt ) )
                    {
                        lookup = lookup.updated( toW.alt, new WeightedAlternative( toW.site, toW.alt ) )
                    }
                    val toAlt = lookup(toW.alt)
                    
                    fromAlt.addLink( toAlt, weight )
                    toAlt.addLink( fromAlt, weight )
                }
            }
            
            val q = new RunQ[WeightedAlternative]()
            for ( (k, wa) <- lookup )
            {
                q.add( wa.weight, wa )
            }
            
            while ( !q.isEmpty )
            {
                val (weight, wa) = q.first()
                q.remove( weight, wa )
                
                println( "Weight: " + weight )
                
                val lastAltInSite = wa.site.combs.tail == Nil
                if ( !lastAltInSite )
                {
                    for ( (peer, linkWeight) <- wa.linked )
                    {
                        if ( peer.enabled )
                        {
                            q.remove( peer.weight, peer )
                            peer.weight -= linkWeight
                            q.add( peer.weight, peer )
                        }
                    }
                    
                    wa.site.combs = wa.site.combs.filter( _ != wa.alternative )
                    wa.enabled = false
                }
            }
        }
        
        for ( site <- sites )
        {
            assert( site.combs.length == 1 )
            val alternative = site.combs.head

            for ( altSite <- alternative.sites.reverse )
            {
                assert( altSite.sf.topics.size == 1 )
                val topicId = altSite.sf.topics.head._1
                val topicWeight = altSite.sf.topics.head._2
                
                disambiguated = new SureSite( altSite.start, altSite.end, topicId, topicWeight, topicNameMap(topicId) ) :: disambiguated
            }
        }
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
        for ( site <- sites; alternative <- site.combs; altSite <- alternative.sites )
        {
            val altWeight = alternative.sites.foldLeft(1.0)( _ * _.sf.phraseWeight )
            for ( (topicId, topicWeight) <- altSite.sf.topics; (contextId, contextWeight) <- topicCategoryMap(topicId) )
            {
                val weight = altWeight * topicWeight * contextWeight
                
                if ( weight > AmbiguityForest.minContextEdgeWeight )
                {
                    val updatedList = new REdge(site, alternative, altSite, topicId, weight) :: reverseContextMap.getOrElse(contextId, List[REdge]() )
                    reverseContextMap = reverseContextMap.updated(contextId, updatedList)
                }
            }
        }
        
        // Shouldn't allow links from one alternative in an
        // SF to another alternative in the same SF. Also, within an alternative SHOULD allow
        // links between sites in that alternative

        // Weight each topic based on shared contexts (and distances to those contexts)
        println( "Building weighted topic edges." )
        val distWeighting1 = new NormalDistributionImpl( 0.0, 5.0 )
        val distWeighting2 = new NormalDistributionImpl( 0.0, 10.0 )
        
        var count = 0
        for ( (contextId, alternatives) <- reverseContextMap )
        {
            for ( edge1 <- alternatives )
            {
                count += 1
            }
        }
        println( "Contexts in play: " + reverseContextMap.size + ", inner count: " + count )
        
        for ( (contextId, alternatives) <- reverseContextMap; edge1 <- alternatives; edge2 <- alternatives )
        {
            if ( (edge1.site != edge2.site) || (edge1.alt == edge2.alt && edge1.altSite.sf != edge2.altSite.sf) )
            {
                if ( edge1.topicId != edge2.topicId )
                {
                    val center1 = (edge1.altSite.start + edge1.altSite.end).toDouble / 2.0
                    val center2 = (edge2.altSite.start + edge2.altSite.end).toDouble / 2.0
                    val distance = (center1 - center2)
                    //val distanceWeight = 1.0 + 1000.0*distWeighting.density( distance )
                    val distanceWeight = (distWeighting1.density( distance )/distWeighting1.density(0.0)) + 1.0 + 0.0*(distWeighting2.density( distance )/distWeighting2.density(0.0))
                    //println( ":: " + distance + ", " + distanceWeight )
                    val totalWeight = (edge1.weight * edge2.weight)// * distanceWeight
                    edge1.alt.weight += totalWeight
                    edge2.alt.weight += totalWeight
                    
                    edge1.altSite.sf.topicWeights = edge1.altSite.sf.topicWeights.updated( edge1.topicId, edge1.altSite.sf.topicWeights.getOrElse( edge1.topicId, 0.0 ) + totalWeight )
                    edge2.altSite.sf.topicWeights = edge2.altSite.sf.topicWeights.updated( edge2.topicId, edge2.altSite.sf.topicWeights.getOrElse( edge2.topicId, 0.0 ) + totalWeight )
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
    }
    
    def dumpGraphOld( linkFile : String, nameFile : String )
    {
        println( "Building topic and context association graph." )
     
        var topicWeights = disambiguated.foldLeft( TreeMap[Int, Double]() )( (x, y) => x.updated( y.topicId, y.weight ) )
        
        
        var topicLinkCount = TreeMap[Int, Int]()
        var els = TreeMap[Int, Int]()
        var contextWeights = TreeMap[Int, Double]()
        var count = 0
        for ( ss <- disambiguated )
        {
            if ( !els.contains( ss.topicId ) )
            {
                els = els.updated( ss.topicId, count )
                count += 1
            }
            topicLinkCount = topicLinkCount.updated( ss.topicId, 1 + topicLinkCount.getOrElse( ss.topicId, 0 ) )
            
            for ( (contextId, weight) <- topicCategoryMap(ss.topicId) )
            {
                if ( !topicWeights.contains( contextId ) )
                {
                    val oldWeight = contextWeights.getOrElse( contextId, 0.0 )
                    contextWeights = contextWeights.updated( contextId, oldWeight max (ss.weight * weight) )
                    if ( !els.contains( contextId ) )
                    {
                        els = els.updated( contextId, count )
                        count += 1
                    }
                }
                topicLinkCount = topicLinkCount.updated( contextId, 1 + topicLinkCount.getOrElse( contextId, 0 ) )
            }
        }
        
        
        val n = new java.io.PrintWriter( new File(nameFile) )
        for ( (id, cid) <- els )
        {
            if ( topicWeights.contains( id ) )
            {
                n.println( cid + " " + topicWeights(id) + " " + topicNameMap(id) )
            }
            else
            {
                n.println( cid + " " + contextWeights(id) + " " + topicNameMap(id) )
            }
        }
        n.close()
        
        val p = new java.io.PrintWriter( new File(linkFile) )
        val topicCategoryQuery = topicDb.prepare( "SELECT contextTopicId, weight FROM linkWeights2 WHERE topicId=? ORDER BY weight DESC LIMIT 50", Col[Int]::Col[Double]::HNil )
        for ( (id, cid) <- els )
        {
            topicCategoryQuery.bind( id )
            for ( v <- topicCategoryQuery )
            {
                val targetId = _1(v).get
                val weight = _2(v).get
                
                if ( id != targetId && els.contains( targetId ) )
                {    
                    val targetCid = els( targetId )
                    p.println( cid + " " + targetCid + " " + weight )
                }
            }
        }
        p.close()
    }
    
    def dumpGraph( linkFile : String, nameFile : String )
    {
        var count = 0
        var idMap = TreeMap[Int, Int]()
        for ( ss <- disambiguated )
        {
            if ( !idMap.contains(ss.topicId) )
            {
                idMap = idMap.updated( ss.topicId, count )
                count += 1
            }
        }
        
        val n = new java.io.PrintWriter( new File(nameFile) )
        
        // A map from context ids to topic ids with weights
        var connections = TreeMap[Int, List[(Int, Double)]]()
        for ( ss <- disambiguated )
        {
            for ( (contextId, weight) <- topicCategoryMap(ss.topicId) )
            {
                val oldList = connections.getOrElse( contextId, List[(Int, Double)]() )
                connections = connections.updated( contextId, (ss.topicId, weight) :: oldList )
            }
            
            n.println( idMap(ss.topicId) + " " + ss.weight + " " + topicNameMap(ss.topicId) )
        }
        n.close()
        
        var localWeights = TreeMap[(Int, Int), Double]()
        for ( ss <- disambiguated )
        {
            for ( (contextId, weight1) <- topicCategoryMap(ss.topicId) )
            {
                for ( (topicId, weight2) <- connections( contextId ) )
                {
                    if ( contextId != topicId )
                    {
                        val fromId = idMap(ss.topicId)
                        val toId = idMap(topicId)
                        
                        if ( fromId != toId )
                        {
                            val minId = fromId min toId
                            val maxId = fromId max toId
                            
                            val weight = weight1 * weight2
                            val oldWeight = localWeights.getOrElse( (minId, maxId), 0.0 )
                            localWeights = localWeights.updated( (minId, maxId), oldWeight + weight )
                        }
                    }
                }
            }
        }
        
        val p = new java.io.PrintWriter( new File(linkFile) )
        for ( ((fromId, toId), weight) <- localWeights )
        {
            p.println( fromId + " " + toId + " " + weight )
        }
        
        p.close()
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
        val categoryQuery = topicDb.prepare( "SELECT t1.name, t2.weight FROM topics AS t1 INNER JOIN linkWeights2 AS t2 ON t1.id=t2.contextTopicId WHERE t2.topicId=? ORDER BY t1.name ASC", Col[String]::Col[Double]::Col[Double]::HNil )

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
                //println( "  " + word )
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

                                    val weightedTopics = topicDetails.foldLeft( TreeMap[Int, Double]() )( (x, y) => x.updated( y._1, y._2.toDouble / sfCount.toDouble ) )
                                    //val weightedTopics = for ( td <- topicDetails ) yield (td._1, td._2.toDouble / sfCount.toDouble)
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

                                    //println( "-> " + wordList.slice(fromIndex, toIndex+1) )                              
                                    possiblePhrases = (fromIndex, toIndex, thisSf) :: possiblePhrases
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

            val f = new AmbiguityForest( wordList, topicNameMap, topicCategoryMap, topicDb )
            f.addTopics( possiblePhrasesSorted )
            //f.buildContextWeights()
            //f.applyContexts( topicDb )
            
            //f.alternativeBasedResolution()
            f.alternativeBasedResolution2()
            
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


