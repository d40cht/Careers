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

import org.seacourt.serialization.SerializationProtocol._

import org.seacourt.utility.{Graph, PriorityQ, AutoMap}

import com.weiglewilczek.slf4s.{Logging}

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
    val minContextEdgeWeight    = 1.0e-9
    val numAllowedContexts      = 30
    
    // Smith-waterman (and other sparse articles) require this
    val secondOrderContexts             = true
    
    // Fewer than this number of first order contexts, go wider
    
    // Quite sensitive. Drop to 10 but beware Cambridge Ontario
    val secondOrderKickin               = 10
    val secondOrderContextDownWeight    = 0.1
    
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
    
    val maxNumberOfWords                = 3000
    val topicVectorMaxSize              = 100
    
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
                    
                    def downWeightPeers( enqueueFn : (Double, TopicDetailLink) => Unit, dequeueFn : (Double, TopicDetailLink) => Unit )
                    {
                        for ( (peer, linkWeight) <- peers )
                        {
                            if ( !peer.processed )
                            {
                                dequeueFn( peer.algoWeight, peer )
                            }
                            
                            peer.algoWeight -= linkWeight.totalWeight
                            peer.alternative.altAlgoWeight -= linkWeight.totalWeight
                            algoWeight -= linkWeight.totalWeight
                            alternative.altAlgoWeight -= linkWeight.totalWeight
                            
                            peer.peers.remove( this )
                            
                            //assert( alternative.altAlgoWeight > -1e-10 )
                            //assert( peer.alternative.altAlgoWeight > -1e-10 )
                                
                            // Remove the peer from its parent sf. If it's the last one look up to the alt site
                            // and then the ambiguity alternative to decide whether to cull either
                            if ( !peer.processed )
                            {
                                enqueueFn( peer.algoWeight, peer )
                            }
                        }
                    }
                }
                
                var topics : HashSet[TopicDetail] = topicDetails.foldLeft(HashSet[TopicDetail]())( (s, x) => s + new TopicDetail( x._1, if ( _phraseWeight > AmbiguityForest.minPhraseWeight ) x._2 else 0.0 ) )
            }
        }
        
        def remove( enqueueFn : (Double, TopicDetailLink) => Unit, dequeueFn : (Double, TopicDetailLink) => Unit )
        {
            for ( site <- sites )
            {
                for( td <- site.sf.topics )
                {
                    dequeueFn( td.algoWeight, td )
                    td.processed = true
                    td.active = false
                    td.downWeightPeers( enqueueFn, dequeueFn )
                }
                
                // Clear the topics out
                site.sf.topics = site.sf.topics.empty
            }
            combs = combs - this
        }
        
        def cullUpwards( enqueueFn : (Double, TopicDetailLink) => Unit, dequeueFn : (Double, TopicDetailLink) => Unit )
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
                    td.downWeightPeers( enqueueFn, dequeueFn )
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




class AgglomClustering[NodeType <% Clusterable[NodeType]] extends Logging
{
    type DJSet = DisjointSet[NodeType]
    
    var clusterDistances = HashMap[(NodeType, NodeType), Double]()
    var sets = HashMap[NodeType, DJSet]()
    
    def keyPair( from : NodeType, to : NodeType ) =
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
    
    def run( strictness : Double, completeCoverage: (Int) => Boolean, resetCoverage : () => Unit, compatibleForLink : (NodeType, NodeType) => Boolean, getName : NodeType => String, mopupOutliers : Boolean ) =
    {
        val weightOrdered = clusterDistances.toList.sortWith( _._2 > _._2 ).map( x => (getSet(x._1._1), getSet(x._1._2), x._2) )
        
        breakable
        {
            for ( (from, to, weight) <- weightOrdered )
            {
                if ( from.find() != to.find() )
                {
                    //println( " ****** " )
                    //var compatible = true
                    
                    var linkCount = 0
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
                            
                            if ( cp && linked )
                            {
                                //compatible = false
                                //break
                                linkCount += 1
                            }
                            sum += weight
                            count += 1
                        }
                    }
                    
                    val compatible = (linkCount.toDouble / count.toDouble) >= strictness
                    
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
                        //if ( (from.size == 1 && to.size > 2) || (to.size == 1 && from.size > 2) )
                        if ( (from.size == 1) || (to.size == 1) )
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
        resetCoverage()
        
        var used = 0
        
        var chosen = HashSet[NodeType]()
        breakable
        {
            for ( (dj, coherence) <- clusterList.sortWith( _._2 > _._2 ) )
            {
                //println( "+++++++++++ " + dj.size + ", " + coherence + " +++++++++++" )
                for ( v <- dj.members() )
                {
                    v.value.select()
                    
                    var avail = chosen.foldLeft(true)( _ && compatibleForLink(v.value, _) )
                    if (avail)
                    {
                        chosen += v.value
                        //println( "* " + getName( v.value ) )
                    }
                    else
                    {
                        //println( "  " + getName( v.value ) )
                    }
                }
                
                if ( completeCoverage(3) ) break
                used += 1
            }
        }
        
        logger.debug( "Total: " + clusterList.size + ", used: " + used )
        
        val groupings = clusterList.map( x => x._1.members().map( y => y.value ) )
        
        groupings
    }
    
    def runGrouped( strictness : Double, completeCoverage: (Int) => Boolean, resetCoverage : () => Unit, compatibleForLink : (NodeType, NodeType) => Boolean, getName : NodeType => String, mopupOutliers : Boolean ) =
    {
        val groupings = run( strictness, completeCoverage, resetCoverage, compatibleForLink, getName, mopupOutliers )
        var groupMembership = HashMap[NodeType, Int]()
        groupings.zipWithIndex.foreach( x => {
            val members = x._1
            val gid = x._2
            
            members.foreach( wid =>
            {
                groupMembership = groupMembership.updated( wid, gid )
            } )
        } )
        
        groupMembership
    }
}

class WrappedTopicId( val id : Int ) extends Clusterable[WrappedTopicId]
{
    def < (rhs : WrappedTopicId) = id < rhs.id
    def equal (rhs : WrappedTopicId) = id == rhs.id
    def select() {}
}

class AmbiguityForest( val words : List[String], val topicNameMap : HashMap[Int, String], topicCategoryMap : HashMap[Int, HashMap[Int, Double]], val topicDb : SQLiteWrapper, val categoryHierarchy : CategoryHierarchy ) extends Logging
{
    type TopicDetailLink = AmbiguitySite#AmbiguityAlternative#AltSite#SurfaceForm#TopicDetail
    
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
    
    var debugData = List[scala.xml.Elem]()
    def addDebug( entryFn : () => scala.xml.Elem )
    {
        debugData = entryFn() :: debugData
    }
    def debug = false
    
    def addTopics( sortedPhrases : List[AmbiguityForest.SurfaceFormDetails] )
    {
        addDebug( () =>
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
            </surfaceForms> )
            
        var asbs = List[AmbiguitySiteBuilder]()
        for ( sf <- sortedPhrases )
        {
            if ( asbs == Nil || !asbs.head.overlaps( sf.start, sf.end ) )
            {
                asbs = new AmbiguitySiteBuilder( sf.start, sf.end ) :: asbs
            }
            
            asbs.head.extend( sf )
        }
        
        logger.info( "Building site alternatives." )
        sites = asbs.reverse.map( _.buildSite() ).filter( _.combs.size >= 1 )
    }
    
    private def validate()
    {
        def close( a : Double, b : Double ) =
        {
            val diff = (a - b).abs
            diff < 1e-10 || diff <= (0.0001 * (a max b))
        }
        
        var tdlCount = 0
        var peerCount = 0
        val weightings = new AutoMap[TopicDetailLink, Double]( x => 0.0 )
        for
        (
            site <- sites;
            alternative <- site.combs;
            altSite <- alternative.sites;
            topicDetail <- altSite.sf.topics
        )
        {
            for ( peerL <- topicDetail.peers )
            {
                val (peer, peerLink) = peerL
                
                require( close( peerLink.totalWeight, peerLink.componentWeights.toList.foldLeft(0.0)(_ + _._2) ) )
                
                weightings.set(topicDetail, weightings(topicDetail) + peerLink.totalWeight)
                //weightings.set(peer, weightings(peer) + peerLink.totalWeight)
                
                peerCount += 1
            }
            tdlCount += 1
        }
        
        println( "Peer count: %d, tdl count: %d".format( peerCount, tdlCount ) )
        
        var allTopics = HashSet[TopicDetailLink]()
        for
        (
            site <- sites;
            alternative <- site.combs;
            altSite <- alternative.sites;
            topicDetail <- altSite.sf.topics
        )
        {
            //println( weightings(topicDetail), topicDetail.algoWeight )
            require( close( weightings(topicDetail), topicDetail.algoWeight ) )
            allTopics += topicDetail
        }
        
        for ( site <- sites; alternative <- site.combs; altSite <- alternative.sites; peer <- altSite.sf.topics.head.peers )
        {
            if ( !allTopics.contains( peer._1 ) ) println( "Missing topic: " + topicNameMap(peer._1.topicId) )
            require( allTopics.contains( peer._1 ) )
        }
    }
    
    def alternativeBasedResolution()
    {
        addDebug( () =>
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
            </sites> )
        
        type ContextId = Int
        type SiteCentre = Double
                
        logger.info( "Building weighted context edges." )
        var allTds = List[TopicDetailLink]()
        var reverseContextMap = TreeMap[ContextId, List[(TopicDetailLink, Double)]]()
        
        var contextWeightMap = TreeMap[ContextId, Double]()
        
        
        // Link via contexts
        var idToTopicMap = new AutoMap[Int, HashSet[TopicDetailLink]]( x => HashSet[TopicDetailLink]())
        def buildContextWeightMap() =
        {
            for ( site <- sites; alternative <- site.combs; altSite <- alternative.sites )
            {
                val altWeight = alternative.altWeight
                for ( topicDetail <- altSite.sf.topics )
                {
                    allTds = topicDetail :: allTds
                    
                    idToTopicMap.set( topicDetail.topicId, idToTopicMap(topicDetail.topicId) + topicDetail )
                    
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
        }
        buildContextWeightMap()
        
        // Shouldn't allow links from one alternative in an
        // SF to another alternative in the same SF. Also, within an alternative SHOULD allow
        // links between sites in that alternative

        // Weight each topic based on shared contexts (and distances to those contexts)
        logger.info( "Building weighted topic edges." )
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
        
        // Topics linked via contexts
        def buildContextLinks() =
        {
            logger.info( "Links via contexts" )
            for ( (contextId, alternatives) <- reverseContextMap )
            {
                if ( idToTopicMap.contains( contextId ) )
                {
                    // Direct link
                    val directLinks = idToTopicMap(contextId)
                    
                    for ( (topicDetail1, weight1) <- alternatives )
                    {
                        for ( topicDetail2 <- directLinks )
                        {
                            val combinedWeight = topicDetail2.alternative.altWeight * topicDetail2.topicWeight * weight1
                            if ( compatibleForLink( topicDetail1, topicDetail2 ) )
                            {
                                if ( topicDetail1.topicId != topicDetail2.topicId )
                                {
                                    buildLinks( topicDetail1, topicDetail2, combinedWeight, None )
                                }
                            }
                        }
                    }
                }
                
                for ( (topicDetail1, weight1) <- alternatives; (topicDetail2, weight2) <- alternatives )
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
            }
            
            validate()
        }
        
        buildContextLinks()
        
        // Use top-level aggregate clustering to prune. TODO: Evaluate how neccessary this is after all algo. bug fixes.
        def runAggregateClusterPruning() =
        {
            logger.info( "Using aggregate clustering to prune network" )
            def completeCoverage(numAlternatives : Int) = sites.foldLeft(true)( (x, y) => x && y.complete(numAlternatives) )
            def resetCoverage() = sites.foreach( _.resetMarkings() )
            
            topicClustering.run( 0.99, completeCoverage, resetCoverage, (x, y) => compatibleForLink(x.value, y.value), x => topicNameMap(x.value.topicId), false )
            
            for ( site <- sites )
            {
                for ( c <- site.combs )
                {
                    for ( altSite <- c.sites if !altSite.complete )
                    {
                        for ( td <- altSite.sf.topics )
                        {
                            td.removeTopic()
                            td.downWeightPeers( (x, y) => Unit, (x, y) => Unit )
                        }
                    }
                    c.sites = c.sites.filter( _.complete )
                }
                site.combs = site.combs.filter( _.sites.size > 0 )
            }
            sites = sites.filter( _.combs.size > 0 )
        }
        runAggregateClusterPruning()
        validate()
        
        logger.info( "Links in play: " + linkCount )
        
        dumpResolutions()
        
        // Prune out alternatives
        def pruneOutAlternatives() =
        {
            logger.info( "Running new algo framework" )
            
            // Add all interesting elements to the queue.
            val q = new NPriorityQ[TopicDetailLink]()
            for ( td <- allTds ) q.add( td.algoWeight, td )
            
            // Prune alternatives before topics
            if ( AmbiguityForest.pruneAlternativesBeforeTopics )
            {
                logger.info( "Pruning down to one alternative per site." )
                var done = false
                
                sites.map( x => assert( x.combs.size >= 1 ) )
                while ( !done )
                {
                    val prunableSites = sites.filter( _.combs.size > 1 )
                    
                    if ( prunableSites != Nil )
                    {
                        val allAlternatives = for ( site <- prunableSites; alt <- site.combs.toList ) yield alt
                        val leastWeight = allAlternatives.reduceLeft( (x, y) => if (x.altAlgoWeight < y.altAlgoWeight) x else y )
                        
                        leastWeight.remove( (w, p) => q.add(w, p), (w, p) => q.remove(w, p) )
                    }
                    else
                    {
                        done = true
                    }
                }
                
                sites.map( x =>assert( x.combs.size == 1 ) )
            }
            
            validate()
            
            logger.info( "Pruning down to one topic per site." )
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
                    td.downWeightPeers( (weight, peer) => q.add(weight, peer), (weight, peer) => q.remove(weight, peer) )
                    td.active = false
                }
                else
                {
                    //println( "Culling upwards: " + topicNameMap(td.topicId) + " " + td.algoWeight )
                    td.alternative.cullUpwards( (weight, peer) => q.add(weight, peer), (weight, peer) => q.remove(weight, peer) )
                }
            }
            
            validate()
            
            sites = sites.filter( _.combs.size > 0 )
            
            for ( site <- sites; alternative <- site.combs; alt <- alternative.sites )
            {
                assert( alt.sf.topics.size == 1 )
            }
        }
        pruneOutAlternatives()
        
        logger.info( "Dumping resolutions." )
        dumpResolutions()
        
        var allTopics = HashSet[TopicDetailLink]()
        for ( site <- sites )
        {
            assert( site.combs.size <= 1 )
            for ( alternative <- site.combs )
            {
                for ( altSite <- alternative.sites )
                {
                    assert( altSite.sf.topics.size <= 1 )
                    
                    if ( altSite.sf.topics.size == 1 )
                    {
                        allTopics += altSite.sf.topics.head
                    }
                }
            }
        }
        validate()
       
        // Use top-level aggregate clustering to prune 
        if ( false )
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
            
            logger.info( "Using aggregate clustering to prune network" )
            
            
            if ( false )
            {
                def completeCoverage(numAlternatives : Int) = sites.foldLeft(true)( (x, y) => x && y.complete(numAlternatives) )
                def resetCoverage() = sites.foreach( _.resetMarkings() )
                val groupings = topicClustering2.run( 0.99, completeCoverage, resetCoverage, (x, y) => true, x => topicNameMap(x.id), false )
                
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
                
                logger.info( "Inserting " + catEdges.length + " edges into hierarchy builder with " + allTopicIds.size + " topic ids" )
                
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
        logger.info( "Finished running alternative based resolution." )
    }
    
    def dumpResolutions()
    {
        addDebug( () =>
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
                                                                    <weight>{weight.totalWeight}</weight>
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
            </resolutions> )
    }
    
    
    def dumpDebug( fileName : String )
    {
        if ( debug )
        {
            val fullDebug =
                <root>
                { for ( el <-debugData.reverse ) yield el }
                </root>

            XML.save( fileName, fullDebug, "utf8" )
        }            
    }
    
    def getDocumentDigest( id : Int ) =
    {
        var topicWeights = new AutoMap[Int, Double]( x => 0.0 )
        var primaryTopicSet = new HashSet[Int]()
        val linkWeights = new AutoMap[(Int, Int), Double]( x => 0.0 )
        
        var topicMap = new AutoMap[Int, WrappedTopicId]( id => new WrappedTopicId(id) )
        val topicClustering = new AgglomClustering[WrappedTopicId]()
        for ( site <- sites; alternative <- site.combs; alt <- alternative.sites; fromTopic <- alt.sf.topics; link <- fromTopic.peers )
        {
            val (toTopic, peerLink) = link
            
            /*for ( (contextId, weight) <- peerLink.componentWeights )
            {
                topicWeights.set( contextId, topicWeights(contextId) + weight )
            }*/
            
            topicWeights.set( fromTopic.topicId, topicWeights(fromTopic.topicId) + peerLink.totalWeight )
            topicWeights.set( toTopic.topicId, topicWeights(toTopic.topicId) + peerLink.totalWeight )
        }
            
        val topNTopics = topicWeights.toList.sortWith( _._2 > _._2 ).slice(0, AmbiguityForest.topicVectorMaxSize).foldLeft(HashSet[Int]())( _ + _._1 )
        for ( site <- sites; alternative <- site.combs; alt <- alternative.sites; fromTopic <- alt.sf.topics; link <- fromTopic.peers )
        {
            val (toTopic, peerLink) = link
            
            if ( topNTopics.contains(toTopic.topicId) && topNTopics.contains(fromTopic.topicId) )
            {
                val key = if ( fromTopic.topicId < toTopic.topicId ) (fromTopic.topicId, toTopic.topicId) else (toTopic.topicId, fromTopic.topicId)
                linkWeights.set(key, linkWeights(key) + peerLink.totalWeight)
                
                topicClustering.update( topicMap(fromTopic.topicId), topicMap(toTopic.topicId), peerLink.totalWeight )
                
                primaryTopicSet += fromTopic.topicId
            }
        }
        
        val groupMembership = topicClustering.runGrouped( 0.7, x => false, () => Unit, (x, y) => true, x => topicNameMap(x.id), false )
        
        
        println( primaryTopicSet.size, groupMembership.size )
        
        val tv = new TopicVector()
        val prunedTopics = topNTopics.toList.map( x => (x, topicWeights(x)) )
        for ( (topicId, weight) <- prunedTopics )
        {
            val isPrimaryTopic = primaryTopicSet.contains(topicId)
            val wid = topicMap(topicId)
            val groupId = if (isPrimaryTopic && groupMembership.contains(wid)) groupMembership(wid) else -1
            
            tv.addTopic( topicId, weight, topicNameMap(topicId), groupId, isPrimaryTopic )
        }
        
        
        new DocumentDigest( id, tv, linkWeights.toList.map( x => (x._1._1, x._1._2, x._2) ) )
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
                //realTopicIds += p.topicId
            }
            
            for ( ((from, to), weight) <- linkMap ) topicClustering2.update( from, to, weight )
            //for ( site <- sites; c <- site.combs; alt <- c.sites; t <- alt.sf.topics; (p, w) <- t.peers ) topicClustering2.update( topicMap(t.topicId), topicMap(p.topicId), w.totalWeight )
            
            def completeCoverage(numAlternatives : Int) = sites.foldLeft(true)( (x, y) => x && y.complete(numAlternatives) )
            def resetCoverage() = sites.foreach( _.resetMarkings() )
            topicClustering2.run( 0.99, completeCoverage, resetCoverage, (x, y) => true, x => topicNameMap(x.id), true )
        }
        
        var allTopicIds = HashSet[Int]()
        
        val resolutions =
            <data>
                <sites>
                {
                    for ( site <- sites; alternative <- site.combs; alt <- alternative.sites; topic <- alt.sf.topics if topic.algoWeight > 0.0 ) yield
                    {
                        allTopicIds += topic.topicId
                        
                        assert( topic.active )
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
                                    assert( toTopic.active )
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
