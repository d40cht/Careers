package org.seacourt.disambiguator

import scala.util.control.Breaks._
import scala.collection.immutable.{TreeSet, TreeMap, HashSet, HashMap}
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
import org.seacourt.disambiguator.CategoryHierarchy._


import org.seacourt.utility.{Graph, PriorityQ, AutoMap}

// TODO:
//
// 1) Having resolved down to a set of topics, construct a set of contexts using the 'component weights' field of PeerLink.
//   a) Think about which contexts to include and how. If someone has OO and Java on their CV, C++ should not neccessarily be a context.
//   b) How to choose a sensible default set, and then how to allow tuning?
//
// 2) Play around with cosine similarity metric.
//   a) Need to show which top N topics/contexts contributed to a match.
//   b) Need to be able to then punt some out and search again.
//   c) Efficient implementation of search online? Needs to be an O(N) comparison per search
//   d) Run a batch job overnight doing N^2 comparisons for email updates.
//
// 3) Consider how to organise the groups of skills. Fiddle with category hierarchies some more.
//   a) When clustering groups (currently using context distances)
//      When doing a + b, do a min depth labelling from all (a+b) to all (a+b). After each labelling update a max distance for each node.
//      Choose the category node with the minimum max distance as the parent?
//
// 4) Speedups
//   a) Pre-calculate second order contexts (and weights) in db.
//   b) Prune db based on appropriate weight thresholds.
//   c) Re-pack db info in binary files and load all into memory?
//
// 5) Data
//   a) Ask Globals for CVs.
//   b) Send wider email when site a bit more tested.

object AmbiguityForest
{
    type TopicId = Int
    type PhraseId = Int
    type Weight = Double

    type TopicWeightDetails = TreeMap[TopicId, Weight]
    
    class SurfaceFormDetails( val start : Int, val end : Int, val phraseId : PhraseId, val weight : Weight, val topicDetails : TopicWeightDetails )
        
    val minPhraseWeight         = 0.005
    val minContextEdgeWeight    = 1.0e-7
    val numAllowedContexts      = 20
    
    // Smith-waterman (and other sparse articles) require this
    val secondOrderContexts             = true
    
    // Fewer than this number of first order contexts, go wider
    
    // Quite sensitive. Drop to 10 but beware Cambridge Ontario
    val secondOrderKickin               = 10
    val secondOrderContextDownWeight    = 0.1
    val secondOrderExcludeCategories    = false
    
    // Reversed contexts run slowly and seem to perform badly
    //val reversedContexts                = false
    
    
    // Not clear that this has any particular power (if set to true). May even screw things up (if sets to 1.0)
    
    // Now defined as taking the sqrt of the category weight
    val upweightCategories              = true

    // Works for some things, but screws up 'cambridge united kingdom'
    // unless in conjuction with normaliseTopicWeightings = false
    val topicDirectLinks                = true

    
    // Upweights more frequently referenced topics if false
    // (problems with 'python palin' - Sarah dominates Michael)
    val normaliseTopicWeightings        = true
    val minTopicRelWeight               = 0.00001
    
    // Links in the top % of weight are included
    val linkPercentileFilter            = 0.95
    
    // Prune alternatives down before topics. Seems to be better set to true
    val pruneAlternativesBeforeTopics   = true
    
    val minClusterCoherence             = 1e-9
    
}

trait Clusterable[BaseType]
{
    def < (rhs : BaseType) : Boolean
    def equal (rhs : BaseType) : Boolean
    def select() : Unit
}

class PeerLink()
{
    var totalWeight = 0.0
    var componentWeights = new AutoMap[Int, Double]( x => 0.0 )
    
    def addLink( contextId : Int, weight : Double )
    {
        totalWeight += weight
        componentWeights.set( contextId, componentWeights(contextId) + weight )
    }
}

class AmbiguitySite( val start : Int, val end : Int )
{
    type TopicDetailLink = AmbiguitySite#AmbiguityAlternative#AltSite#SurfaceForm#TopicDetail
    var combs = HashSet[AmbiguityAlternative]()
    var numComplete = 0
    
    def complete( numAlternatives : Int ) = numComplete >= numAlternatives || numComplete == combs.size
    def resetMarkings()
    {
        numComplete = 0
        for ( c <- combs )
        {
            c.complete = false
            c.sites.foreach( _.complete = false )
        }
    }
    
    class AmbiguityAlternative( siteDetails : List[AmbiguityForest.SurfaceFormDetails] )
    {
        var sites = siteDetails.map( x => new AltSite( x ) )
        val weight = sites.foldLeft(1.0)( _ * _.sf.phraseWeight )
        var activeSiteCount = sites.length
        var altAlgoWeight = 0.0
        var complete = false
        
        def markClustered()
        {
            complete = sites.foldLeft(true)( _ && _.complete )
            
            if ( complete ) AmbiguitySite.this.numComplete += 1
        }
        
        def nonEmpty = sites.foldLeft(false)( _ || _.nonEmpty )
        
        // If a site is weight zero then it's not counted as part of the probability of this site overall so we
        // don't include it (by setting its weight to 1 in the below calculation).
        def altWeight = sites.foldLeft(1.0)( _ * _.sf.phraseWeight )

        class AltSite( siteDetail : AmbiguityForest.SurfaceFormDetails )
        {
            val start = siteDetail.start
            val end = siteDetail.end
            var complete = false
            
            def markClustered()
            {
                if ( !complete )
                {
                    complete = true
                    AmbiguityAlternative.this.markClustered()
                }
            }
            
            val sf = new SurfaceForm( siteDetail.phraseId, siteDetail.weight, siteDetail.topicDetails )
            
            def nonEmpty = sf.topics.size > 0
            
            class SurfaceForm( val phraseId : Int, _phraseWeight : Double, topicDetails : AmbiguityForest.TopicWeightDetails )
            {
                val phraseWeight = if (_phraseWeight > AmbiguityForest.minPhraseWeight) _phraseWeight else 1.0
                
                class TopicDetail( val topicId : AmbiguityForest.TopicId, val topicWeight : Double )
                {                
                    var algoWeight = 0.0
                    var active = true
                    var processed = false
                    
                    type TopicDetailLink = AmbiguitySite#AmbiguityAlternative#AltSite#SurfaceForm#TopicDetail
                    var peers = new AutoMap[TopicDetailLink, PeerLink]( x => new PeerLink() )
                    
                    // Functions to access back up the nested object hierarchy
                    def sf = SurfaceForm.this
                    def altSite = AltSite.this
                    def alternative = AmbiguityAlternative.this
                    def site = AmbiguitySite.this
                    
                    def markClustered() = altSite.markClustered()

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
                    
                    def downWeightPeers( q : PriorityQ[TopicDetailLink], topicNameMap : TreeMap[Int, String] )
                    {
                        for ( (peer, linkWeight) <- peers )
                        {
                            if ( !peer.processed )
                            {
                                q.remove( peer.algoWeight, peer )
                            }
                            
                            peer.algoWeight -= linkWeight.totalWeight
                            peer.alternative.altAlgoWeight -= linkWeight.totalWeight
                            
                            //assert( alternative.altAlgoWeight > -1e-10 )
                            //assert( peer.alternative.altAlgoWeight > -1e-10 )
                                
                            // Remove the peer from its parent sf. If it's the last one look up to the alt site
                            // and then the ambiguity alternative to decide whether to cull either
                            if ( !peer.processed )
                            {
                                q.add( peer.algoWeight, peer )
                            }
                        }
                    }
                }
                
                var topics : HashSet[TopicDetail] = topicDetails.foldLeft(HashSet[TopicDetail]())( (s, x) => s + new TopicDetail( x._1, if ( _phraseWeight > AmbiguityForest.minPhraseWeight ) x._2 else 0.0 ) )
            }
        }
        
        def remove( q : PriorityQ[TopicDetailLink], topicNameMap : TreeMap[Int, String] )
        {
            for ( site <- sites )
            {
                for( td <- site.sf.topics )
                {
                    q.remove( td.algoWeight, td )
                    td.processed = true
                    td.active = false
                    td.downWeightPeers( q, topicNameMap )
                }
                
                // Clear the topics out
                site.sf.topics = site.sf.topics.empty
            }
            combs = combs - this
        }
        
        def cullUpwards( q : PriorityQ[TopicDetailLink], topicNameMap : TreeMap[Int, String] )
        {
            activeSiteCount -= 1
         
            if ( (activeSiteCount == 0) && (combs.size > 1) )
            {
                for ( site <- sites )
                {
                    assert( site.sf.topics.size == 1 )
                    val td = site.sf.topics.head
                    assert( td.processed )
                    assert( td.active )
                    td.active = false
                    td.downWeightPeers( q, topicNameMap )
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




class AgglomClustering[NodeType <% Clusterable[NodeType]]
{
    type DJSet = DisjointSet[NodeType]
    
    var clusterDistances = HashMap[(NodeType, NodeType), Double]()
    var sets = HashMap[NodeType, DJSet]()
    
    private def keyPair( from : NodeType, to : NodeType ) =
    {
        val asIs = from < to
        var a = if (asIs) from else to
        var b = if (asIs) to else from
        (a, b)
    }

    def update( from : NodeType, to : NodeType, weight : Double )
    {
        val key = keyPair(from, to)

        val oldWeight = clusterDistances.getOrElse( key, 0.0 )
        clusterDistances = clusterDistances.updated( key, weight + oldWeight )
    }
    
    private def getSet( topicDetail : NodeType ) =
    {
        if ( sets.contains( topicDetail ) )
        {
            sets(topicDetail)
        }
        else
        {
            val newSet = new DJSet( topicDetail )
            sets = sets.updated( topicDetail, newSet )
            newSet
        }
    }
    
    def getSetDistance( first : DJSet, second : DJSet ) =
    {
        var totalCount = 0
        var connectedCount = 0
        var total = 0.0
        for ( m1 <- first.members(); m2 <- second.members() )
        {
            val key = keyPair(m1.value, m2.value)
            
            if ( clusterDistances.contains(key) )
            {
                total += clusterDistances(key)
                connectedCount += 1
            }
            totalCount += 1
        }
        
        // Average of distances for extant links, then upweight for missing links
        if ( connectedCount == 0 ) Double.MaxValue
        else
        {
            val asym = (totalCount.toDouble / connectedCount.toDouble)
            (total / connectedCount.toDouble) * asym * asym
        }
    }
    
    def getWeight( from : NodeType, to : NodeType ) =
    {
        val key = keyPair(from, to)
        if ( clusterDistances.contains(key) )
        {
            clusterDistances(key)
        }
        else
        {
            0.0
        }
    }
    
    def run( sites : Iterable[AmbiguitySite], compatibleForLink : (NodeType, NodeType) => Boolean, getName : NodeType => String, mopupOutliers : Boolean ) =
    {
        val weightOrdered = clusterDistances.toList.sortWith( _._2 > _._2 ).map( x => (getSet(x._1._1), getSet(x._1._2), x._2) )
        
        def completeCoverage(numAlternatives : Int) = sites.foldLeft(true)( (x, y) => x && y.complete(numAlternatives) )
        
        breakable
        {
            for ( (from, to, weight) <- weightOrdered )
            {
                if ( from.find() != to.find() )
                {
                    //println( " ****** " )
                    var compatible = true
                    
                    var count = 0
                    var sum = 0.0
                    breakable
                    {
                        
                        for ( set1ms <- from.members(); set2ms <- to.members() )
                        {
                            val key = keyPair(set1ms.value, set2ms.value)
                            val cp = compatibleForLink( set1ms.value, set2ms.value )
                            val weight = getWeight(set1ms.value, set2ms.value)
                            val linked = clusterDistances.contains( key )
                            
                            if ( !cp || !linked )
                            {
                                compatible = false
                                break
                            }
                            sum += weight
                            count += 1
                        }
                    }
                    
                    if ( compatible )
                    {
                        //println( "Merging " + getName( from.value ) + " into " + getName( to.value ) + " : " + weight )
                        from.join(to)
                        
                        from.value.select()
                        to.value.select()
                        
                        if ( completeCoverage(6) ) break
                    }
                }
            }
            
            if ( mopupOutliers )
            {
                for ( (from, to, weight) <- weightOrdered )
                {
                    if ( from.find() != to.find() )
                    {
                        if ( (from.size == 1 && to.size > 2) || (to.size == 1 && from.size > 2) )
                        {
                            from.join(to)
                        }
                    }
                }
            }
        }
        
        var visited = HashSet[DJSet]()
        for ( (tdl, djs) <- sets )
        {
            val root = djs.find()
            if ( !visited.contains( root ) )
            {
                visited += root
            }
        }
        
        var clusterList = List[(DJSet, Double)]()
        for ( dj <- visited )
        {
            var sum = 0.0
            var count = 0
            for ( m1 <- dj.members(); m2 <- dj.members() if !(m1.value equal m2.value) )//if m1.value.topicId != m2.value.topicId )
            {
                sum += getWeight( m1.value, m2.value )
                count += 1
            }
            if ( count > 1 )
            {
                val coherence = sum / count.toDouble
                
                if ( coherence > AmbiguityForest.minClusterCoherence )
                {
                    clusterList = (dj, coherence) :: clusterList
                }
            }
        }
        
        // Reset all markers
        sites.foreach( _.resetMarkings() )
        
        var used = 0
        
        var chosen = HashSet[NodeType]()
        breakable
        {
            for ( (dj, coherence) <- clusterList.sortWith( _._2 > _._2 ) )
            {
                println( "+++++++++++ " + dj.size + ", " + coherence + " +++++++++++" )
                for ( v <- dj.members() )
                {
                    v.value.select()
                    
                    var avail = chosen.foldLeft(true)( _ && compatibleForLink(v.value, _) )
                    if (avail)
                    {
                        chosen += v.value
                        println( "* " + getName( v.value ) )
                    }
                    else
                    {
                        println( "  " + getName( v.value ) )
                    }
                }
                
                if ( completeCoverage(3) ) break
                used += 1
            }
        }
        
        println( "Total: " + clusterList.size + ", used: " + used )
        
        val groupings = clusterList.map( x => x._1.members().map( y => y.value ) )
        
        groupings
    }
}

class WrappedTopicId( val id : Int ) extends Clusterable[WrappedTopicId]
{
    def < (rhs : WrappedTopicId) = id < rhs.id
    def equal (rhs : WrappedTopicId) = id == rhs.id
    def select() {}
}

class AmbiguityForest( val words : List[String], val topicNameMap : TreeMap[Int, String], topicCategoryMap : TreeMap[Int, TreeMap[Int, Double]], val topicDb : SQLiteWrapper, val categoryHierarchy : CategoryHierarchy )
{
    class SureSite( val start : Int, val end : Int, val topicId : Int, val weight : Double, val name : String ) {}
    
    class TopicGraphNode( val topicId : Int )
    {
        var weight = 0.0
        var peers = List[(TopicGraphNode, Double)]()
        
        def addLink( peer : TopicGraphNode, _weight : Double )
        {
            weight += _weight
            peers = (peer, _weight) :: peers
        }
        
        def numPeers = peers.length
        
        def name = topicNameMap(topicId)
    }

    
    var sites = List[AmbiguitySite]()
    var contextWeights = TreeMap[Int, Double]()
    var disambiguated = List[SureSite]()
    var communities : CommunityTreeBase[TopicGraphNode] = null
    
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
        sites = asbs.reverse.map( _.buildSite() ).filter( _.combs.size >= 1 )
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
        
        var contextWeightMap = TreeMap[ContextId, Double]()
        
        
        // Link via contexts
        for ( site <- sites; alternative <- site.combs; altSite <- alternative.sites )
        {
            val altWeight = alternative.altWeight
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
                        
                        contextWeightMap = contextWeightMap.updated( contextId, contextWeightMap.getOrElse( contextId, 0.0 ) + weight )
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


        def compatibleForLink( topicDetail1 : TopicDetailLink, topicDetail2 : TopicDetailLink ) =
            ((topicDetail1.site != topicDetail2.site) || (topicDetail1.alternative == topicDetail2.alternative && topicDetail1.sf != topicDetail2.sf))
            

        class TDClusterable( val value : TopicDetailLink ) extends Clusterable[TDClusterable]
        {
            def < (rhs : TDClusterable) = value.topicId < rhs.value.topicId
            def equal( rhs : TDClusterable ) = value.topicId == rhs.value.topicId
            def select = value.markClustered
        }

            
        val topicClustering = new AgglomClustering[TDClusterable]()
        val clusterableMap = new AutoMap[TopicDetailLink, TDClusterable]( x => new TDClusterable(x) )
        
        implicit def tdlToClusterable( tdl : TopicDetailLink ) = clusterableMap(tdl)
        
        var linkCount = 0
        def buildLinks( topicDetail1 : TopicDetailLink, topicDetail2 : TopicDetailLink, linkWeight: Double, contextId : Option[Int] ) =
        {
            if ( linkWeight > AmbiguityForest.minContextEdgeWeight )
            {
                val center1 = (topicDetail1.altSite.start + topicDetail1.altSite.end).toDouble / 2.0
                val center2 = (topicDetail2.altSite.start + topicDetail2.altSite.end).toDouble / 2.0
                val distance = (center1 - center2)
                //val distanceWeight = 1.0 + 1000.0*distWeighting.density( distance )
                val distanceWeight = 0.2 + (distWeighting1.density( distance )/distWeighting1.density(0.0)) + 0.0*(distWeighting2.density( distance )/distWeighting2.density(0.0))
                //println( ":: " + distance + ", " + distanceWeight )
                val totalWeight = linkWeight //* distanceWeight
                topicDetail1.alternative.altAlgoWeight += totalWeight
                topicDetail2.alternative.altAlgoWeight += totalWeight
                
                topicDetail1.algoWeight += totalWeight
                topicDetail2.algoWeight += totalWeight
                
                //val oldWeight1 = topicDetail1.peers.getOrElse( topicDetail2, 0.0 )
                //topicDetail1.peers = topicDetail1.peers.updated( topicDetail2, oldWeight1 + totalWeight )
                topicDetail1.peers( topicDetail2 ).addLink( if (contextId.isEmpty) topicDetail2.topicId else contextId.get, totalWeight )
                topicDetail2.peers( topicDetail1 ).addLink( if (contextId.isEmpty) topicDetail1.topicId else contextId.get, totalWeight )
                
                
                linkCount += 1

                //println( "Linking: " + topicNameMap( topicDetail1.topicId ) + " to " + topicNameMap( topicDetail2.topicId ) + " weight: " + totalWeight )
                
                topicClustering.update( topicDetail1, topicDetail2, linkWeight )
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
                        val contexts2 = topicCategoryMap( topicDetail2.topicId )
                        
                        val bidirectional = contexts2.contains( topicDetail1.topicId )
                        
                        val linkWeight = contexts( topicDetail2.topicId ) 
                        val altWeight1 = alternative1.altWeight * topicDetail1.topicWeight
                        val altWeight2 = alternative2.altWeight * topicDetail2.topicWeight
                        
                        
                        //println( "Direct link: " + topicNameMap( topicDetail1.topicId ) + " to " + topicNameMap( topicDetail2.topicId ) + " weight: " + altWeight1 * altWeight2 * linkWeight )
                        //println( linkWeight + ", " + altWeight1 + ", " + altWeight2 + ", " + topicDetail1.topicWeight + ", " + topicDetail2.topicWeight )
                        
                        
                        if ( compatibleForLink( topicDetail1, topicDetail2 ) )
                        {
                            val directWeight = altWeight1 * altWeight2 * linkWeight
                            if ( topicDetail1.topicId != topicDetail2.topicId )
                            {
                                buildLinks( topicDetail1, topicDetail2, directWeight, None )
                            }
                            //topicClustering.update( topicDetail1, topicDetail2, directWeight )
                        }
                    }
                }
            }
        }
        
        // Topics linked via contexts
        for ( (contextId, alternatives) <- reverseContextMap; (topicDetail1, weight1) <- alternatives; (topicDetail2, weight2) <- alternatives )
        {
            if ( compatibleForLink( topicDetail1, topicDetail2 ) )
            {
                if ( topicDetail1.topicId != topicDetail2.topicId )
                {
                    buildLinks( topicDetail1, topicDetail2, weight1 * weight2, Some(contextId) )
                }
                //topicClustering.update( topicDetail1, topicDetail2, weight1 * weight2 )
            }
        }
        
        // Use top-level aggregate clustering to prune 
        if ( true )
        {
            println( "Using aggregate clustering to prune network" )
            topicClustering.run( sites, (x, y) => compatibleForLink(x.value, y.value), x => topicNameMap(x.value.topicId), false )
            
            for ( site <- sites )
            {
                for ( c <- site.combs ) c.sites = c.sites.filter( _.complete )
                site.combs = site.combs.filter( _.sites.size > 0 )
            }
            sites = sites.filter( _.combs.size > 0 )
        }
        
        println( "Links in play: " + linkCount )
        
        dumpResolutions()
        
        // Prune out alternatives
        if ( true )
        {
            println( "Running new algo framework" )
            
            // Add all interesting elements to the queue.
            val q = new PriorityQ[TopicDetailLink]()
            for ( td <- allTds ) q.add( td.algoWeight, td )
            
            // Prune alternatives before topics
            if ( AmbiguityForest.pruneAlternativesBeforeTopics )
            {
                println( "Pruning down to one alternative per site." )
                var done = false
                
                sites.map( x => assert( x.combs.size >= 1 ) )
                while ( !done )
                {
                    val prunableSites = sites.filter( _.combs.size > 1 )
                    
                    if ( prunableSites != Nil )
                    {
                        val allAlternatives = for ( site <- prunableSites; alt <- site.combs.toList ) yield alt
                        val leastWeight = allAlternatives.reduceLeft( (x, y) => if (x.altAlgoWeight < y.altAlgoWeight) x else y )
                        leastWeight.remove( q, topicNameMap )
                    }
                    else
                    {
                        done = true
                    }
                }
                
                sites.map( x =>assert( x.combs.size == 1 ) )
            }
            
            
            sites = sites.filter( _.combs.size > 0 )
            
            println( "Pruning down to one topic per site." )
            while ( !q.isEmpty )
            {
                val (weight, td) = q.first
                
                q.remove( weight, td )
                td.processed = true
                
                // If it's the last then don't do the downweighting
                val topicRemoved = td.removeTopic()
                
                if ( topicRemoved )
                {
                    //println( "Downweighting: " + topicNameMap(td.topicId) + " " + td.algoWeight )
                    td.downWeightPeers( q, topicNameMap )
                    td.active = false
                }
                else
                {
                    //println( "Culling upwards: " + topicNameMap(td.topicId) + " " + td.algoWeight )
                    td.alternative.cullUpwards( q, topicNameMap )
                }
            }
            
            
            if ( false )
            {
                // De-novo topic and context weight calculation
                var idToTopicMap = TreeMap[Int, TopicGraphNode]()
                
                val allTopicIds = (for ( site <- sites; alternative <- site.combs; alt <- alternative.sites; topic <- alt.sf.topics ) yield topic.topicId).foldLeft(HashSet[Int]())( _ + _ )
                
                for ( site <- sites; alternative <- site.combs; alt <- alternative.sites; topic <- alt.sf.topics )
                {
                    val fromId = topic.topicId
                    val contexts = topicCategoryMap(fromId)
                    
                    val weightOrderedContexts = contexts.toList.sortWith( _._2 > _._2 )
                    val totalWeight = weightOrderedContexts.foldLeft(0.0)( _ + _._2 )
                    
                    var runningWeight = 0.0
                    for ( (toId, weight) <- contexts.toList.sortWith( _._2 > _._2 ) )
                    {
                        if ( runningWeight < totalWeight * 0.80 )
                        {
                            val fromNode = idToTopicMap.getOrElse( fromId, new TopicGraphNode( fromId ) )
                            val toNode = idToTopicMap.getOrElse( toId, new TopicGraphNode( toId ) )
                            idToTopicMap = idToTopicMap.updated( fromId, fromNode )
                            idToTopicMap = idToTopicMap.updated( toId, toNode )
                            
                            fromNode.addLink(toNode, weight)
                            
                            val destIsContext = !allTopicIds.contains( toNode.topicId )
                            toNode.addLink(fromNode, weight * (if ( destIsContext ) 0.1 else 1.0) )
                            
                            runningWeight += weight
                        }
                    }
                }
                
                var count = 0
                val v = new Louvain[TopicGraphNode]()
                for ( (id, fromNode) <- idToTopicMap )
                {
                    for ( (toNode, weight) <- fromNode.peers )
                    {
                        if ( fromNode.numPeers > 3 && toNode.numPeers > 3 )
                        {
                            assert( allTopicIds.contains(fromNode.topicId) || allTopicIds.contains(toNode.topicId) )
                            v.addEdge( fromNode, toNode, weight )
                            count += 1
                        }
                    }
                }
                println( "NUM EDGES: " + count )
                communities = v.run()
            }
            
            sites = sites.filter( _.combs.size > 0 )
            
            /*if ( false )
            {
                // Direct topic-topic community graph
                //val v = new Louvain()
                for ( site <- sites )
                {
                    assert( site.combs.size == 1 )
                    
                    val alternative = site.combs.head
                    
                    // No weights should be less than zero (modulo double precision rounding issues).
                    
                    //println( words.slice(site.start, site.end+1) + ": " + alternative.altAlgoWeight )
                    for ( alt <- alternative.sites )
                    {
                        assert( alt.sf.topics.size <= 1 )
                        //println( "  " + alt.sf.topics.head.algoWeight + " " + topicNameMap(alt.sf.topics.head.topicId) )
                        
                        val fromTopic = alt.sf.topics.head
                        
                        // Re-weight the topic peer links and trim down to the percentile
                        var totalWeight = 0.0
                        val reweighted = fromTopic.peers.filter( _._1.active ).map
                        { x =>
                            val toTopic = x._1
                            val weight = x._2 / ( fromTopic.topicWeight * toTopic.topicWeight * fromTopic.sf.phraseWeight * toTopic.sf.phraseWeight )
                            totalWeight += weight
                            (toTopic, weight )
                        }
                        
                        var runningWeight = 0.0
                        fromTopic.peers = fromTopic.peers.empty
                        for ( (toTopic, weight) <- reweighted.toList.sortWith( _._2 > _._2 ) )
                        {
                            if ( runningWeight < (totalWeight * AmbiguityForest.linkPercentileFilter) )
                            {
                                fromTopic.peers = fromTopic.peers.updated( toTopic, weight )
                                runningWeight += weight
                            }
                        }
                        
                        
                        for ( (toTopic, weight) <- fromTopic.peers.toList.sortWith( _._2 > _._2 ) )
                        {
                            //v.addEdge( fromTopic.topicId, toTopic.topicId, 1.0 )
                            //v.addEdge( fromTopic.topicId, toTopic.topicId, log( weight / AmbiguityForest.minContextEdgeWeight ) )
                            //v.addEdge( fromTopic.topicId, toTopic.topicId, weight )
                        }
                    }
                    
                    assert( alternative.altAlgoWeight > -1e-10 )
                }
                
                //communities = v.run()
            }*/
        }
        
        println( "Dumping resolutions." )
        dumpResolutions()
        
        for ( site <- sites )
        {
            assert( site.combs.size <= 1 )
            for ( alternative <- site.combs )
            {
                for ( altSite <- alternative.sites )
                {
                    assert( altSite.sf.topics.size <= 1 )
                }
            }
        }
       
        // Use top-level aggregate clustering to prune 
        if ( true )
        {
            var topicMap = new AutoMap[Int, WrappedTopicId]( id => new WrappedTopicId(id) )
            val linkMap = new AutoMap[(WrappedTopicId, WrappedTopicId), Double]( x => 0.0 )
            
            for ( site <- sites; c <- site.combs; alt <- c.sites; t <- alt.sf.topics; (p, w) <- t.peers )
            {
                val key = (topicMap(p.topicId), topicMap(t.topicId))
                linkMap.set( key, linkMap(key) + w.totalWeight )
            }
            
            val topicClustering2 = new AgglomClustering[WrappedTopicId]()
            
            for ( ((from, to), weight) <- linkMap ) topicClustering2.update( from, to, weight )
            for ( site <- sites; c <- site.combs; alt <- c.sites; t <- alt.sf.topics; (p, w) <- t.peers ) topicClustering2.update( topicMap(t.topicId), topicMap(p.topicId), w.totalWeight )
            
            println( "Using aggregate clustering to prune network" )
            
            
            if ( false )
            {
                val groupings = topicClustering2.run( sites, (x, y) => true, x => topicNameMap(x.id), false )
                
                val topicLinkUpweight = 5000.0
                val allTopicIds = groupings.flatMap( x => x.map( y => y.id ) ).foldLeft( HashSet[Int]() )( _ + _ )
                
                val catEdges = categoryHierarchy.toTop( allTopicIds, (fromId, toId, weight) =>
                {
                    val distance = -log(weight)
                    if ( allTopicIds.contains( fromId ) || allTopicIds.contains( toId ) )
                    {
                        distance + topicLinkUpweight
                    }
                    else
                    {
                        distance
                    }
                } )

                val topicNameQuery = topicDb.prepare( "SELECT name FROM topics WHERE id=?", Col[String]::HNil )
                val topicNameMap = new AutoMap[Int, String]( id => {
                    topicNameQuery.bind(id)
                    _1(topicNameQuery.toList(0)).get
                } )
                
                var rootIds = List[Int]()
                var nextId = allTopicIds.foldLeft(0)( _ max _ ) + 1
                for ( group <- groupings )
                {
                    group.foreach( wti => catEdges.append( (nextId, wti.id, 0.0) ) )
                    rootIds = nextId :: rootIds
                    topicNameMap.set( nextId, group.map( wti => topicNameMap( wti.id ) ).mkString(", ") )
                    nextId += 1
                }
                
                println( "Inserting " + catEdges.length + " edges into hierarchy builder with " + allTopicIds.size + " topic ids" )
                
                {
                    val hb = new CategoryHierarchy.Builder( allTopicIds, catEdges, x => topicNameMap(x) )
                    val trees = hb.run( (x, y) => 0.0, (topicLinkUpweight*2.0) + 25.0 )
                    for ( tree <- trees ) tree.print( x => topicNameMap(x) )
                }
            }
        } 
        
        for ( site <- sites )
        {
            val weightedAlternatives = site.combs.toList.sortWith( _.altAlgoWeight > _.altAlgoWeight )
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
                    val weightedAlternatives = site.combs.toList.sortWith( _.altAlgoWeight > _.altAlgoWeight )
                    <site>
                        <phrase>{ words.slice( site.start, site.end+1 ).mkString(" ") }</phrase>
                        <numComplete>{site.numComplete}</numComplete>
                        {
                            for ( wa <- weightedAlternatives ) yield
                                <alternative>
                                    <altWeight>{wa.weight}</altWeight>
                                    <algoWeight>{wa.altAlgoWeight}</algoWeight>
                                    <complete>{wa.complete}</complete>
                                    {
                                        for ( altSite <- wa.sites ) yield
                                            <element>
                                                <text>{words.slice( altSite.start, altSite.end+1 ).mkString(" ")}</text>
                                                <complete>{altSite.complete}</complete>
                                                <sfPhraseWeight>{altSite.sf.phraseWeight}</sfPhraseWeight>
                                                <topics>
                                                {
                                                    for ( topicDetail <- altSite.sf.topics.toList.sortWith( _.algoWeight > _.algoWeight ).slice(0, 10) ) yield
                                                        <element>
                                                            <name>{topicNameMap(topicDetail.topicId)}</name>
                                                            <weight>{topicDetail.algoWeight}</weight>
                                                            <processed>{topicDetail.processed}</processed>
                                                            {
                                                                for ( (peer, weight) <- topicDetail.peers.filter(_._1.active).toList.sortWith( _._2.totalWeight > _._2.totalWeight ).slice(0, 10) ) yield
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
    
    def output( htmlFileName : String, resolutionFileName : String )
    {
        val wordArr = words.toArray
        var i = 0
        var phraseIt = disambiguated

        def wikiLink( topicName : String ) =
        {
            val wikiBase = "http://en.wikipedia.org/wiki/"
            wikiBase + (if (topicName.startsWith("Main:")) topicName.drop(5) else topicName)
        }
                
        
        val l = ListBuffer[scala.xml.Elem]()
        while ( i < wordArr.size )
        {
            if ( phraseIt != Nil && phraseIt.head.start == i )
            {
                val resolution = phraseIt.head
                val topicLink = wikiLink( resolution.name )
                val size = (18.0 + log(resolution.weight max 1.0e-15)) max 4
                val fs = "font-size:"+size.toInt
                val fsSmall = "font-size:" + ( (size / 1.5).toInt max 3 )
                l.append(
                    <span style="position:relative;">
                    <span id={"hover_%d_%d".format(resolution.start, resolution.end)} style={"position:absolute;background-color:#ff7; color:#000; opacity:0.9; top:1.5em; z-index:1;border: 1px solid;%s".format(fsSmall)}></span>
                    <a id={"%d_%d".format(resolution.start, resolution.end)} href={ topicLink } title={"Weight: " + resolution.weight } style={fs} >
                        
                        {"[" + words.slice( resolution.start, resolution.end+1 ).mkString(" ") + "] "}
                    </a>
                    </span> )
                i = resolution.end+1
                phraseIt = phraseIt.tail
            }
            else
            {
                l.append( <span>{wordArr(i) + " " }</span> )
                i += 1
            }
        }
        
        val topicFromId = disambiguated.foldLeft(HashMap[Int, SureSite]())( (m, v) => m.updated( v.topicId, v ) )
        
        def buildHead() : scala.xml.Elem =
        {
            var highlights = List[String]()
            for ( site <- sites )
            {
                val alternative = site.combs.head
                
                for ( alt <- alternative.sites )
                {
                    var ons = List[String]()
                    var offs = List[String]()
                
                    val fromTopic = alt.sf.topics.head
                    for ( (toTopic, peerLink) <- fromTopic.peers )
                    {
                        val sortedWeights = peerLink.componentWeights.toList.sortWith( _._2 > _._2 ) 
                        val debugWeights = sortedWeights.map( x => ("%s: %2.2e".format( topicNameMap(x._1).replace("'", ""), x._2 ) ) ).mkString( ", " )
                        ons = "$('#%d_%d').addClass('hover');".format( toTopic.altSite.start, toTopic.altSite.end ) :: ons
                        ons = "$('#hover_%d_%d').text('%s');".format( toTopic.altSite.start, toTopic.altSite.end, debugWeights ) :: ons
                        offs = "$('#%d_%d').removeClass('hover');".format( toTopic.altSite.start, toTopic.altSite.end ) :: offs
                        offs = "$('#hover_%d_%d').text('');".format( toTopic.altSite.start, toTopic.altSite.end ) :: offs
                    }
                
                    highlights = "$('#%d_%d').mouseover( function() { %s } );".format( fromTopic.altSite.start, fromTopic.altSite.end, ons.mkString("\n") ) :: highlights
                    highlights = "$('#%d_%d').mouseout( function() { %s } );".format( fromTopic.altSite.start, fromTopic.altSite.end, offs.mkString("\n") ) :: highlights
                }
            }
            
            val res = "$(document).ready( function() { " + highlights.mkString("\n") + " } );"
            
            <head>
                <style type="text/css">{ ".hover { background-color:#0ff; }" }</style>
                <script type="text/javascript" src="https://ajax.googleapis.com/ajax/libs/jquery/1.6.2/jquery.min.js"></script>
                <script type="text/javascript"> { res } </script>
            </head>
        }
        
        def communityToHtml( structure : CommunityTreeBase[TopicGraphNode] ) : scala.xml.Elem =
        {
            structure match
            {
                case InternalNode( children )   =>
                <ul><li/>
                {
                    for ( c <- children.sortWith( _.size > _.size ) ) yield communityToHtml(c)
                }
                <li/></ul>
                case LeafNode( children )       =>
                <ul><li/>
                {
                    for ( c <- children.sortWith( (x,y) => x.weight > y.weight ) ) yield
                    <li>
                        <a href={wikiLink(c.name)}> { (if (topicFromId.contains(c.topicId)) "* " else "") + c.name + " " + c.weight} </a> 
                    </li>
                }
                <li/></ul>
            }
        }
        
        /*val allTopicIds = (for ( site <- sites; alternative <- site.combs; alt <- alternative.sites; topic <- alt.sf.topics if topic.algoWeight > 0.0 ) yield topic.topicId).foldLeft(HashSet[Int]())( _ + _ )
        val resolutions =
            <topics>
            {
                for ( topicId <- allTopicIds ) yield
                <topic>
                    <id>{topicId}</id>
                    <name>{topicNameMap(topicId)}</name>
                </topic>
            }
            </topics>*/

        type TopicDetailLink = AmbiguitySite#AmbiguityAlternative#AltSite#SurfaceForm#TopicDetail
        var lastTopicId = 0
        val topicDetailIds = new AutoMap[TopicDetailLink, Int]( x =>
        {
            val thisId = lastTopicId
            lastTopicId += 1
            thisId
        } )

        // Things that are topics (not just contexts)
        var realTopicIds = HashSet[Int]()
        val groupings =
        {
            val topicClustering2 = new AgglomClustering[WrappedTopicId]()
            var topicMap = new AutoMap[Int, WrappedTopicId]( id => new WrappedTopicId(id) )
            val linkMap = new AutoMap[(WrappedTopicId, WrappedTopicId), Double]( x => 0.0 )
            
            for ( site <- sites; c <- site.combs; alt <- c.sites; t <- alt.sf.topics if t.algoWeight > 0.0; (p, peerLink) <- t.peers; (contextId, contextWeight) <- peerLink.componentWeights )
            {
                assert( site.combs.size == 1 )
                val key1 = (topicMap(t.topicId), topicMap(contextId))
                linkMap.set( key1, linkMap(key1) + sqrt(contextWeight) )
                
                val key2 = (topicMap(contextId), topicMap(p.topicId))
                linkMap.set( key2, linkMap(key2) + sqrt(contextWeight) )
                
                val key3 = (topicMap(t.topicId), topicMap(p.topicId))
                linkMap.set( key3, linkMap(key3) + contextWeight )
                
                realTopicIds += t.topicId
                realTopicIds += p.topicId
            }
            
            for ( ((from, to), weight) <- linkMap ) topicClustering2.update( from, to, weight )
            //for ( site <- sites; c <- site.combs; alt <- c.sites; t <- alt.sf.topics; (p, w) <- t.peers ) topicClustering2.update( topicMap(t.topicId), topicMap(p.topicId), w.totalWeight )
            
            topicClustering2.run( sites, (x, y) => true, x => topicNameMap(x.id), true )
        }
        
        var allTopicIds = HashSet[Int]()
        
        val resolutions =
            <data>
                <sites>
                {
                    for ( site <- sites; alternative <- site.combs; alt <- alternative.sites; topic <- alt.sf.topics if topic.algoWeight > 0.0 ) yield
                    {
                        allTopicIds += topic.topicId
                        
                        <site>
                            <id>{topicDetailIds(topic)}</id>
                            <topicId>{topic.topicId}</topicId>
                            <weight>{topic.algoWeight}</weight>
                            <startIndex>{alt.start}</startIndex>
                            <endIndex>{alt.end}</endIndex>
                            <peers>
                            {
                                for ( (toTopic, peerLink) <- topic.peers ) yield
                                {
                                    <peer>
                                        <id>{topicDetailIds(toTopic)}</id>
                                        {
                                            for ( (contextTopicId, weight) <- peerLink.componentWeights.toList.sortWith( _._2 > _._2 ) ) yield
                                            {
                                                allTopicIds += contextTopicId
                                                
                                                <component>
                                                    <contextTopicId>{contextTopicId}</contextTopicId>
                                                    <weight>{weight}</weight>
                                                </component>
                                            }
                                        }
                                    </peer>
                                }
                            }
                            </peers>
                        </site>
                    }
                }
                </sites>
                <groups>
                {
                    for ( (topics, index) <- groupings.view.zipWithIndex ) yield
                    {
                        <group id={index.toString}>
                        {
                            for ( topic <- topics ) yield
                            {
                                <topicId>{topic.id}</topicId>
                            }
                        }
                        </group>
                    }
                }
                </groups>
                <topicNames>
                {
                    for ( id <- allTopicIds ) yield
                    {
                        <topic>
                            <id>{id}</id>
                            <name>{topicNameMap(id)}</name>
                            <primaryTopic>{realTopicIds.contains(id).toString}</primaryTopic>
                        </topic>
                    }
                }
                </topicNames>
            </data>
            
        XML.save( resolutionFileName, resolutions, "utf8" )
        
        
        val output =
            <html>
                {buildHead()}
                <body>
                    <div>{ l.toList }</div>
                    <div>{ if ( communities == null ) <span/> else communityToHtml( communities ) }</div>
                </body>
            </html>
            
            
        XML.save( htmlFileName, output, "utf8" )
    }

}
