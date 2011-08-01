package org.seacourt.disambiguator

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


import org.seacourt.utility.{Graph, PriorityQ}


object AmbiguityForest
{
    type TopicId = Int
    type PhraseId = Int
    type Weight = Double

    type TopicWeightDetails = TreeMap[TopicId, Weight]
    
    class SurfaceFormDetails( val start : Int, val end : Int, val phraseId : PhraseId, val weight : Weight, val topicDetails : TopicWeightDetails )
        
    val minPhraseWeight         = 0.005
    val minContextEdgeWeight    = 1.0e-10
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
    
}


class AmbiguitySite( val start : Int, val end : Int )
{
    type TopicDetailLink = AmbiguitySite#AmbiguityAlternative#AltSite#SurfaceForm#TopicDetail
    var combs = HashSet[AmbiguityAlternative]()
    
    // Ordered shortest distance first
    def orderedLinks = (for ( as <- combs.toList; site <- as.sites; td <- site.sf.topics; link <- td.peers ) yield link).sortWith( _._2 < _._2 )
    
    class AmbiguityAlternative( siteDetails : List[AmbiguityForest.SurfaceFormDetails] )
    {
        val sites = siteDetails.map( x => new AltSite( x ) )
        val weight = sites.foldLeft(1.0)( _ * _.sf.phraseWeight )
        var activeSiteCount = sites.length
        var altAlgoWeight = 0.0
        
        def nonEmpty = sites.foldLeft(false)( _ || _.nonEmpty )
        
        // If a site is weight zero then it's not counted as part of the probability of this site overall so we
        // don't include it (by setting its weight to 1 in the below calculation).
        def altWeight = sites.foldLeft(1.0)( _ * _.sf.phraseWeight )

        class AltSite( siteDetail : AmbiguityForest.SurfaceFormDetails )
        {
            val start = siteDetail.start
            val end = siteDetail.end
            
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
                    
                    def downWeightPeers( q : PriorityQ[TopicDetailLink], topicNameMap : TreeMap[Int, String] )
                    {
                        for ( (peer, linkWeight) <- peers )
                        {
                            if ( !peer.processed )
                            {
                                q.remove( peer.algoWeight, peer )
                            }
                            
                            peer.algoWeight -= linkWeight
                            peer.alternative.altAlgoWeight -= linkWeight
                            
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
        
        
        println( "Top N contexts" )
        for ( (id, weight) <- contextWeightMap.toList.sortWith( (x, y) => x._2 > y._2 ).slice(0, 100) )
        {
            //println( "  " + topicNameMap(id) + ": " + weight )
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
        
        var linkCount = 0
        def buildLinks( topicDetail1 : TopicDetailLink, topicDetail2 : TopicDetailLink, linkWeight: Double ) =
        {
            if ( linkWeight > AmbiguityForest.minContextEdgeWeight )
            {
                if ( (topicDetail1.site != topicDetail2.site) || (topicDetail1.alternative == topicDetail2.alternative && topicDetail1.sf != topicDetail2.sf) )
                {
                    if ( topicDetail1.topicId != topicDetail2.topicId )
                    {
                        val center1 = (topicDetail1.altSite.start + topicDetail1.altSite.end).toDouble / 2.0
                        val center2 = (topicDetail2.altSite.start + topicDetail2.altSite.end).toDouble / 2.0
                        val distance = (center1 - center2)
                        //val distanceWeight = 1.0 + 1000.0*distWeighting.density( distance )
                        val distanceWeight = 0.2 + (distWeighting1.density( distance )/distWeighting1.density(0.0)) + 0.0*(distWeighting2.density( distance )/distWeighting2.density(0.0))
                        //println( ":: " + distance + ", " + distanceWeight )
                        val totalWeight = linkWeight// * distanceWeight
                        topicDetail1.alternative.altAlgoWeight += totalWeight
                        topicDetail2.alternative.altAlgoWeight += totalWeight
                        
                        topicDetail1.algoWeight += totalWeight
                        topicDetail2.algoWeight += totalWeight
                        
                        val oldWeight1 = topicDetail1.peers.getOrElse( topicDetail2, 0.0 )
                        topicDetail1.peers = topicDetail1.peers.updated( topicDetail2, oldWeight1 + totalWeight )
                        
                        val oldWeight2 = topicDetail2.peers.getOrElse( topicDetail1, 0.0 )
                        topicDetail2.peers = topicDetail2.peers.updated( topicDetail1, oldWeight2 + totalWeight )
                        
                        linkCount += 1
                        
                        //println( "Linking: " + topicNameMap( topicDetail1.topicId ) + " to " + topicNameMap( topicDetail2.topicId ) + " weight: " + totalWeight )
                    }
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
                        val altWeight1 = alternative1.altWeight * topicDetail1.topicWeight
                        val altWeight2 = alternative2.altWeight * topicDetail2.topicWeight
                        
                        
                        //println( "Direct link: " + topicNameMap( topicDetail1.topicId ) + " to " + topicNameMap( topicDetail2.topicId ) + " weight: " + altWeight1 * altWeight2 * linkWeight )
                        //println( linkWeight + ", " + altWeight1 + ", " + altWeight2 + ", " + topicDetail1.topicWeight + ", " + topicDetail2.topicWeight )
                        buildLinks( topicDetail1, topicDetail2, altWeight1 * altWeight2 * linkWeight )
                    }
                }
            }
        }
        
        // Topics linked via contexts
        for ( (contextId, alternatives) <- reverseContextMap; (topicDetail1, weight1) <- alternatives; (topicDetail2, weight2) <- alternatives )
        {
            buildLinks( topicDetail1, topicDetail2, weight1 * weight2 )
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
            
            
            if ( true )
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
            
            if ( true )
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
            }
        }
        
        {
            val allTopicIds = (for ( site <- sites; alternative <- site.combs; alt <- alternative.sites; topic <- alt.sf.topics if topic.algoWeight > 0.0 ) yield topic.topicId).foldLeft(HashSet[Int]())( _ + _ )
            
            val topicNameQuery = topicDb.prepare( "SELECT name FROM topics WHERE id=?", Col[String]::HNil )
            
            println( "Getting category DAG" )
            var topicEdges = List[(Int, Int, Double)]()
            var categories = HashSet[Int]()
            for ( id <- allTopicIds )
            {
                for ( (contextId, weight) <- topicCategoryMap( id ) )
                {
                    val name = topicNameMap(contextId)
                    if ( name.startsWith( "Category:" ) )
                    {
                        //println( "Cat: " + name )
                        categories += contextId
                        
                        topicEdges = (id, contextId, weight) :: topicEdges
                    }
                }
            }
                
            val catEdges = categoryHierarchy.toTop( categories.toList )
            
            /*for ( (fromId, toId) <- catEdges )
            {
                topicNameQuery.bind( fromId )
                val fromName = _1(topicNameQuery.toList(0)).get
                topicNameQuery.bind( toId )
                val toName = _1(topicNameQuery.toList(0)).get
                //println( "  " + fromName + " --> " + toName )
            }*/
            
            val totalEdges = topicEdges ++ catEdges
            
            println( "Inserting " + totalEdges.length + " edges into hierarchy builder with " + allTopicIds.size + " topic ids" )
            
            /*val hb = new CategoryHierarchy.HierarchyBuilder( allTopicIds.toList, totalEdges )
            val merges = hb.run( _ => "" )
            
            for ( (intoId, (fromIds) ) <- merges )
            {
                topicNameQuery.bind( intoId )
                val intoName = _1(topicNameQuery.toList(0)).get
                
                println( "Merging into: " + intoName )
                for ( fromId <- fromIds )
                {
                    topicNameQuery.bind( fromId )
                    val fromName = _1(topicNameQuery.toList(0)).get
                    println( "  " + fromName )
                }
            }*/
        }
        
        println( "Dumping resolutions." )
        dumpResolutions()
        
        
        
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
                        {
                            for ( wa <- weightedAlternatives ) yield
                                <alternative>
                                    <altWeight>{wa.weight}</altWeight>
                                    <algoWeight>{wa.altAlgoWeight}</algoWeight>
                                    {
                                        for ( altSite <- wa.sites ) yield
                                            <element>
                                                <text>{words.slice( altSite.start, altSite.end+1 ).mkString(" ")}</text>
                                                <sfPhraseWeight>{altSite.sf.phraseWeight}</sfPhraseWeight>
                                                <topics>
                                                {
                                                    for ( topicDetail <- altSite.sf.topics.toList.sortWith( _.algoWeight > _.algoWeight ).slice(0, 10) ) yield
                                                        <element>
                                                            <name>{topicNameMap(topicDetail.topicId)}</name>
                                                            <weight>{topicDetail.algoWeight}</weight>
                                                            <processed>{topicDetail.processed}</processed>
                                                            {
                                                                for ( (peer, weight) <- topicDetail.peers.filter(_._1.active).toList.sortWith( _._2 > _._2 ).slice(0, 10) ) yield
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
                val size = if (resolution.weight == 0.0) 3.0 else 18.0 + log(resolution.weight)
                val fs = "font-size:"+size.toInt
                l.append(
                    <a id={"%d_%d".format(resolution.start, resolution.end)} href={ topicLink } title={"Weight: " + resolution.weight } style={fs} >
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
                    for ( (toTopic, weight) <- fromTopic.peers )
                    {
                        ons = "$('#%d_%d').addClass('hover');".format( toTopic.altSite.start, toTopic.altSite.end ) :: ons
                        offs = "$('#%d_%d').removeClass('hover');".format( toTopic.altSite.start, toTopic.altSite.end ) :: offs
                    }
                
                    highlights = "$('#%d_%d').mouseover( function() { %s } );".format( fromTopic.altSite.start, fromTopic.altSite.end, ons.mkString(" ") ) :: highlights
                    highlights = "$('#%d_%d').mouseout( function() { %s } );".format( fromTopic.altSite.start, fromTopic.altSite.end, offs.mkString(" ") ) :: highlights
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
        
        val allTopicIds = (for ( site <- sites; alternative <- site.combs; alt <- alternative.sites; topic <- alt.sf.topics if topic.algoWeight > 0.0 ) yield topic.topicId).foldLeft(HashSet[Int]())( _ + _ )
        val resolutions =
            <topics>
            {
                for ( topicId <- allTopicIds ) yield
                <topic>
                    <id>{topicId}</id>
                    <name>{topicNameMap(topicId)}</name>
                </topic>
            }
            </topics>
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
