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
    type TopicId = Int
    type PhraseId = Int
    type Weight = Double

    type TopicWeightDetails = TreeMap[TopicId, Weight]
    
    class SurfaceFormDetails( val start : Int, val end : Int, val phraseId : PhraseId, val weight : Weight, val topicDetails : TopicWeightDetails )
        
    val minPhraseWeight         = 0.01
    val minContextEdgeWeight    = 1.0e-6
    val numAllowedContexts      = 100
    
    val secondOrderContexts             = true
    val secondOrderContextDownWeight    = 0.1
    
    val reversedContexts                = false
    val topicDirectLinks                = true
    val upweightCategories              = true
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

class AmbiguitySite( val start : Int, val end : Int )
{
    type TopicDetailLink = AmbiguitySite#AmbiguityAlternative#AltSite#SurfaceForm#TopicDetail
    var combs = HashSet[AmbiguityAlternative]()
    
    class AmbiguityAlternative( siteDetails : List[AmbiguityForest.SurfaceFormDetails] )
    {
        val sites = siteDetails.map( x => new AltSite( x ) )
        val weight = sites.foldLeft(1.0)( _ * _.sf.phraseWeight )
        var activeSiteCount = sites.length
        var algoWeight = 0.0

        class AltSite( siteDetail : AmbiguityForest.SurfaceFormDetails )
        {
            val start = siteDetail.start
            val end = siteDetail.end
            
            val sf = new SurfaceForm( siteDetail.phraseId, siteDetail.weight, siteDetail.topicDetails )
            
            class SurfaceForm( val phraseId : Int, _phraseWeight : Double, topicDetails : AmbiguityForest.TopicWeightDetails )
            {
                val phraseWeight = if ( _phraseWeight > AmbiguityForest.minPhraseWeight ) _phraseWeight else 0.0

                class TopicDetail( val topicId : AmbiguityForest.TopicId, val topicWeight : Double )
                {
                    var algoWeight = 0.0
                    var active = true
                    var processed = false
                    
                    type TopicDetailLink = AmbiguitySite#AmbiguityAlternative#AltSite#SurfaceForm#TopicDetail
                    var peers = HashMap[TopicDetailLink, Double]()
                    
                    // Functions to access back up the nested object hierarchy
                    def sf = SurfaceForm.this
                    def altSite = AltSite.this
                    def alternative = AmbiguityAlternative.this
                    def site = AmbiguitySite.this

                    def removeTopic() =
                    {
                        if ( topics.size > 1 )
                        {
                            topics = topics - this
                            true
                        }
                        else
                        {
                            false
                        }
                    }
                    
                    def downWeightPeers( q : RunQ[TopicDetailLink] )
                    {
                        for ( (peer, linkWeight) <- peers )
                        {
                            if ( !peer.processed )
                            {
                                q.remove( peer.algoWeight, peer )
                            }
                            
                            algoWeight -= linkWeight
                            peer.algoWeight -= linkWeight
                            alternative.algoWeight -= linkWeight
                            peer.alternative.algoWeight -= linkWeight
                                
                            // Remove the peer from its parent sf. If it's the last one look up to the alt site
                            // and then the ambiguity alternative to decide whether to cull either
                            if ( !peer.processed )
                            {
                                q.add( peer.algoWeight, peer )
                            }
                        }
                    }
                }
                
                var topics : HashSet[TopicDetail] = topicDetails.foldLeft(HashSet[TopicDetail]())( (s, x) => s + new TopicDetail( x._1, x._2 ) )
            }
        }
        
        def cullUpwards( q : RunQ[TopicDetailLink] )
        {
            activeSiteCount -= 1
         
            if ( (activeSiteCount == 0) && (combs.size > 1) )
            {
                for ( site <- sites )
                {
                    assert( site.sf.topics.size == 1 )
                    val td = site.sf.topics.head
                    td.active = false
                    td.downWeightPeers( q )
                }
                combs = combs - this
            }
        }
    }
    
    def addAlternative( alt : AmbiguityAlternative )
    {
        combs = combs + alt
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


class AmbiguityForest( val words : List[String], val topicNameMap : TreeMap[Int, String], topicCategoryMap : TreeMap[Int, TreeMap[Int, Double]], val topicDb : SQLiteWrapper )
{
    class SureSite( val start : Int, val end : Int, val topicId : Int, val weight : Double, val name : String ) {}
    
    var sites = List[AmbiguitySite]()
    var contextWeights = TreeMap[Int, Double]()
    var disambiguated = List[SureSite]()
    
    var debug = List[scala.xml.Elem]()
    
    def addTopics( sortedPhrases : List[AmbiguityForest.SurfaceFormDetails] )
    {
        debug =
            <surfaceForms>
                { for ( sf <- sortedPhrases ) yield
                    <element>
                        <phrase>{words.slice( sf.start, sf.end+1 ).mkString(" ")} </phrase>
                        <from>{sf.start}</from>
                        <to>{sf.end}</to>
                        <weight>{sf.weight}</weight>
                        <topics>
                            { for ((topicId, topicWeight) <- sf.topicDetails.toList.sortWith( (x,y) => x._2 > y._2 ).slice(0,4)) yield
                                <topic>
                                    <name>{topicNameMap(topicId)}</name>
                                    <weight>{topicWeight}</weight>
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

        var asbs = List[AmbiguitySiteBuilder]()
        for ( sf <- sortedPhrases )
        {
            if ( asbs == Nil || !asbs.head.overlaps( sf.start, sf.end ) )
            {
                asbs = new AmbiguitySiteBuilder( sf.start, sf.end ) :: asbs
            }
            
            asbs.head.extend( sf )
        }
        
        println( "Building site alternatives." )
        sites = asbs.reverse.map( _.buildSite() )
    }
    
    def alternativeBasedResolution()
    {        
        debug =
            <sites>
            {
                for ( site <- sites ) yield
                    <site>
                    {
                        for ( alternative <- site.combs ) yield
                        <alternative>
                            <weight>{alternative.weight}</weight>
                        {
                            for ( altSite <- alternative.sites ) yield
                            <surfaceForm>
                                <name>{words.slice(altSite.start, altSite.end+1)}</name>
                                <weight>{altSite.sf.phraseWeight}</weight>
                                <topics>
                                {
                                    for ( topicDetail <- altSite.sf.topics.toList.sortWith( _.topicWeight > _.topicWeight ).slice(0,5) ) yield
                                    <element>
                                        <name>{topicNameMap(topicDetail.topicId)}</name>
                                        <weight>{topicDetail.topicWeight}</weight>
                                        <contexts>
                                        {
                                            for ( (contextId, contextWeight) <- topicCategoryMap(topicDetail.topicId).toList.sortWith( _._2 > _. _2 ).slice(0,5) ) yield
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
        type TopicDetailLink = AmbiguitySite#AmbiguityAlternative#AltSite#SurfaceForm#TopicDetail
                
        println( "Building weighted context edges." )
        var allTds = List[TopicDetailLink]()
        var reverseContextMap = TreeMap[ContextId, List[(TopicDetailLink, Double)]]()
        for ( site <- sites; alternative <- site.combs; altSite <- alternative.sites )
        {
            val altWeight = alternative.sites.foldLeft(1.0)( (x, y) => x * y.sf.phraseWeight )
            for ( topicDetail <- altSite.sf.topics )
            {
                allTds = topicDetail :: allTds
                
                val contexts = topicCategoryMap(topicDetail.topicId)
                for ( (contextId, contextWeight) <- contexts )
                {       
                    val weight = altWeight * topicDetail.topicWeight * contextWeight
                    
                    if ( weight > AmbiguityForest.minContextEdgeWeight )
                    {
                        val updatedList = (topicDetail, weight) :: reverseContextMap.getOrElse(contextId, List[(TopicDetailLink, Double)]() )
                        reverseContextMap = reverseContextMap.updated(contextId, updatedList)
                    }
                }
                allTds = topicDetail :: allTds
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
        
        def buildLinks( topicDetail1 : TopicDetailLink, topicDetail2 : TopicDetailLink, linkWeight: Double ) =
        {
            if ( (topicDetail1.site != topicDetail2.site) || (topicDetail1.alternative == topicDetail2.alternative && topicDetail1.sf != topicDetail2.sf) )
            {
                if ( topicDetail1.topicId != topicDetail2.topicId )
                {
                    val center1 = (topicDetail1.altSite.start + topicDetail1.altSite.end).toDouble / 2.0
                    val center2 = (topicDetail2.altSite.start + topicDetail2.altSite.end).toDouble / 2.0
                    val distance = (center1 - center2)
                    //val distanceWeight = 1.0 + 1000.0*distWeighting.density( distance )
                    val distanceWeight = (distWeighting1.density( distance )/distWeighting1.density(0.0)) + 1.0 + 0.0*(distWeighting2.density( distance )/distWeighting2.density(0.0))
                    //println( ":: " + distance + ", " + distanceWeight )
                    val totalWeight = linkWeight// * distanceWeight
                    topicDetail1.alternative.algoWeight += totalWeight
                    topicDetail2.alternative.algoWeight += totalWeight
                    
                    topicDetail1.algoWeight += totalWeight
                    topicDetail2.algoWeight += totalWeight
                    
                    val oldWeight1 = topicDetail1.peers.getOrElse( topicDetail2, 0.0 )
                    val oldWeight2 = topicDetail2.peers.getOrElse( topicDetail1, 0.0 )
                    topicDetail1.peers = topicDetail1.peers.updated( topicDetail2, oldWeight1 + totalWeight )
                    topicDetail2.peers = topicDetail2.peers.updated( topicDetail1, oldWeight2 + totalWeight )
                    
                    //println( "Linking: " + topicNameMap( topicDetail1.topicId ) + " to " + topicNameMap( topicDetail2.topicId ) + " weight: " + totalWeight )
                }
            }
        }
        
        // Topics linked to each other
        if ( AmbiguityForest.topicDirectLinks )
        {
            for ( site1 <- sites; alternative1 <- site1.combs; altSite1 <- alternative1.sites; topicDetail1 <- altSite1.sf.topics )
            {
                val contexts = topicCategoryMap(topicDetail1.topicId)
                for ( site2 <- sites; alternative2 <- site2.combs; altSite2 <- alternative2.sites; topicDetail2 <- altSite2.sf.topics )
                {
                    if ( contexts.contains( topicDetail2.topicId ) )
                    {
                        val linkWeight = contexts( topicDetail2.topicId )
                        val altWeight1 = alternative1.sites.foldLeft(1.0)( (x, y) => x * y.sf.phraseWeight )
                        val altWeight2 = alternative2.sites.foldLeft(1.0)( (x, y) => x * y.sf.phraseWeight )
                        
                        
                        //println( "Direct link: " + topicNameMap( topicDetail1.topicId ) + " to " + topicNameMap( topicDetail2.topicId ) + " weight: " + altWeight1 * altWeight2 * linkWeight )
                        buildLinks( topicDetail1, topicDetail2, altWeight1 * altWeight2 * linkWeight * linkWeight )
                    }
                }
            }
        }
        
        // Topics linked via contexts
        for ( (contextId, alternatives) <- reverseContextMap; (topicDetail1, weight1) <- alternatives; (topicDetail2, weight2) <- alternatives )
        {
            buildLinks( topicDetail1, topicDetail2, weight1 * weight2 )
        }
        
        dumpResolutions()
        if ( true )
        {
            println( "Running new algo framework" )
            
            // Add all interesting elements to the queue.
            val q = new RunQ[TopicDetailLink]()
            for ( td <- allTds ) q.add( td.algoWeight, td )
            
            while ( !q.isEmpty )
            {
                val (weight, td) = q.first
                q.remove( weight, td )
                td.processed = true
                
                // If it's the last then don't do the downweighting
                val topicRemoved = td.removeTopic()
                
                if ( topicRemoved )
                {
                    td.downWeightPeers( q )
                    td.active = false
                }
                else
                {
                    td.alternative.cullUpwards( q )
                }
            }
            
            for ( site <- sites )
            {
                assert( site.combs.size == 1 )
                for ( alt <- site.combs.head.sites ) assert( alt.sf.topics.size == 1 )
            }
        }
        
        println( "Dumping resolutions." )
        dumpResolutions()
        
        for ( site <- sites )
        {
            val weightedAlternatives = site.combs.toList.sortWith( _.algoWeight > _.algoWeight )
            val canonicalAlternative = weightedAlternatives.head
            for ( altSite <- canonicalAlternative.sites.reverse )
            {
                if ( altSite.sf.topics.size > 0 )
                {
                    val canonicalTopic = altSite.sf.topics.toList.sortWith( (x, y) => x.algoWeight > y.algoWeight ).head
                    if ( canonicalTopic.algoWeight > 0.0 )
                    {
                        disambiguated = new SureSite( altSite.start, altSite.end, canonicalTopic.topicId, canonicalTopic.algoWeight, topicNameMap(canonicalTopic.topicId) ) :: disambiguated
                    }
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
                    val weightedAlternatives = site.combs.toList.sortWith( _.algoWeight > _.algoWeight )
                    <site>
                        <phrase>{ words.slice( site.start, site.end+1 ).mkString(" ") }</phrase>
                        {
                            for ( wa <- weightedAlternatives ) yield
                                <alternative>
                                    <weight>{wa.algoWeight}</weight>
                                    {
                                        for ( altSite <- wa.sites ) yield
                                            <element>
                                                <text>{words.slice( altSite.start, altSite.end+1 ).mkString(" ")}</text>
                                                <topics>
                                                {
                                                    for ( topicDetail <- altSite.sf.topics.toList.sortWith( _.algoWeight > _.algoWeight ).slice(0, 5) ) yield
                                                        <element>
                                                            <name>{topicNameMap(topicDetail.topicId)}</name>
                                                            <weight>{topicDetail.algoWeight}</weight>
                                                            <processed>{topicDetail.processed}</processed>
                                                            {
                                                                for ( (peer, weight) <- topicDetail.peers.filter(_._1.active).toList.sortWith( _._2 > _._2 ).slice(0,5) ) yield
                                                                <peer>
                                                                    <name>{topicNameMap(peer.topicId)}</name>
                                                                    <weight>{weight}</weight>
                                                                </peer>
                                                            }
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
                val size = if (resolution.weight == 0.0) 3.0 else 18.0 + log(resolution.weight)
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

        
    class Builder( val text : String )
    {
        val wordList = Utils.luceneTextTokenizer( Utils.normalize( text ) )
        
        var topicCategoryMap = TreeMap[Int, TreeMap[Int, Double]]()
        var topicNameMap = TreeMap[Int, String]()
        var contextWeightMap = TreeMap[Int, Double]()
        
        def allowedPhrase( words : List[String] ) =
        {
            val allNumbers = words.foldLeft( true )( _ && _.matches( "[0-9]+" ) )
            !allNumbers
        }
        
        
        def build() : AmbiguityForest =
        {
            val phraseCountQuery = topicDb.prepare( "SELECT phraseCount FROM phraseCounts WHERE phraseId=?", Col[Int]::HNil )
            val topicQuery = topicDb.prepare( "SELECT topicId, count FROM phraseTopics WHERE phraseTreeNodeId=? ORDER BY count DESC LIMIT 50", Col[Int]::Col[Int]::HNil )
            val topicCategoryQuery = topicDb.prepare( "SELECT t1.contextTopicId, t1.weight, t2.name FROM linkWeights2 AS t1 INNER JOIN topics AS t2 ON t1.contextTopicId=t2.id WHERE topicId=? ORDER BY weight DESC LIMIT 50", Col[Int]::Col[Double]::Col[String]::HNil )
            val topicCategoryQueryReverse = topicDb.prepare( "SELECT topicId, weight FROM linkWeights2 WHERE contextTopicId=? ORDER BY weight DESC LIMIT 50", Col[Int]::Col[Double]::HNil )
            
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
                                val sfTopics = topicQuery.toList
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

                                    val weightedTopics = topicDetails.foldLeft( TreeMap[AmbiguityForest.TopicId, AmbiguityForest.Weight]() )( (x, y) => x.updated( y._1, y._2.toDouble / sfCount.toDouble ) )
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
                                            for ( row <- topicCategoryQuery )
                                            {
                                                val cid = _1(row).get
                                                var weight = _2(row).get
                                                val name = _3(row).get
                                                //val weight2 = _3(row).get
                                                
                                                if ( AmbiguityForest.upweightCategories && name.startsWith( "Category:") )
                                                {
                                                    weight = 1.0
                                                }
                                                
                                                if ( cid != topicId )
                                                {
                                                    contextWeights = contextWeights.updated( cid, weight )
                                                }
                                            }
                                            
                                            if ( AmbiguityForest.reversedContexts )
                                            {
                                                topicCategoryQueryReverse.bind( topicId )
                                                for ( row <- topicCategoryQueryReverse )
                                                {
                                                    val cid = _1(row).get
                                                    val weight = _2(row).get
                                                    contextWeights = contextWeights.updated( cid, weight )
                                                }
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
                                                        val weight = _2(row).get
                                                        //val weight2 = _3(row).get
                                                        
                                                        if ( !contextWeights.contains(cid) )
                                                        {
                                                            if ( cid != topicId )
                                                            {
                                                                // 2nd order context weights are lowered by the weight of the first link
                                                                contextWeights = contextWeights.updated( cid, AmbiguityForest.secondOrderContextDownWeight * contextWeight * weight )
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


