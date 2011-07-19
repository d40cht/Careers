package org.seacourt.disambiguator

import scala.collection.immutable.{TreeMap, HashSet, HashMap}
import scala.collection.mutable.{ArrayBuffer, Stack, Queue}
import scala.xml.XML

import math.{log, pow, abs}
import java.io.{File, DataInputStream, FileInputStream}

import org.seacourt.utility.{NPriorityQ}
import org.seacourt.sql.SqliteWrapper._
import org.seacourt.utility._


object CategoryHierarchy
{
    type Weight = Double
    val overbroadCategoryCount = 20
    
    class CategoryTreeElement( val node : Node, val children : List[(Double, CategoryTreeElement)] )
    {
        var coherence = 0.0
        
        def print( getName : Int => String )
        {
            def rec( getName : Int => String, cte : CategoryTreeElement, weight : Double, indent : Int )
            {
                println( ("  " * indent) + "> " + getName(cte.node.id) + " (" + cte.node.id + ", " + weight + ", " + cte.node.distance + ")" + " --> " + cte.coherence )
                cte.children.map( en => rec( getName, en._2, en._1, indent+1) )
            }
            
            rec( getName, this, 0.0, 0 )
        }
    }
    
    // CREATE TABLE topicInboundLinks( topicId INTEGER, count INTEGER );
    // INSERT INTO topicInboundLinks SELECT contextTopicId, sum(1) FROM linkWeights2 GROUP BY contextTopicId;
    // CREATE INDEX topicInboundLinksIndex ON topicInboundLinks(topicId);
    class CategoryHierarchy( fileName : String, val topicDb : SQLiteWrapper )
    {
        val hierarchy = new EfficientArray[EfficientIntIntDouble](0)
        hierarchy.load( new DataInputStream( new FileInputStream( new File(fileName) ) ) )
        
        def inboundCounts =
        {
            val topicNameQuery = topicDb.prepare( "SELECT name FROM topics WHERE id=?", Col[String]::HNil )
            
            var pairCounts = TreeMap[Int, Int]()
            for ( pair <- hierarchy )
            {
                val fromTopic = pair.first
                val toTopic = pair.second
                val weight = pair.third
                
                topicNameQuery.bind(fromTopic)
                val fromName = _1(topicNameQuery.toList(0)).get
                
                if (fromName.startsWith("Category:") )
                {
                    val oldCount = pairCounts.getOrElse(toTopic, 1)
                    pairCounts = pairCounts.updated( toTopic, oldCount + 1 )
                }
            }
            
            pairCounts.toList.sortWith( _._2 > _._2 )
        }
            
        def debugDumpCounts()
        {
            val topicNameQuery = topicDb.prepare( "SELECT name FROM topics WHERE id=?", Col[String]::HNil )
            
            for ( (id, count) <- inboundCounts )
            {
                topicNameQuery.bind(id)
                val name = _1(topicNameQuery.toList(0)).get
                println( "   ::: " + name + ", " + count )
            }
        }
        
        //val tooFrequent = inboundCounts.filter( _._2 > overbroadCategoryCount ).foldLeft(HashSet[Int]())( _ + _._1 )
                
        def toTop( topicIds : Seq[Int] ) =
        {
            val q = Stack[Int]()
            for ( id <- topicIds ) q.push( id )
            var seen = HashSet[Int]()
            
            //println( "::::::::::::: " + categoryIds.length + " " + q.length )
            
            val bannedCategories = HashSet(
                6393409, // Category:Categories by association,
                366521, // Category:Articles
                8591947, // Category:Standards by organization
                8771811, // Category:Organizations by type
                5850429, // Category:People by occupation
                2750118, // Category:Writers by format
                4027554, // Category:Subfields by academic discipline
                6761185, // Category:Metaphysics
                4572804, // Category:Philosophy by field
                5302049, // Category:Personal life
                1655098, // Category:Works by medium
                5667343, // Category:Organizations
                6940720, // Category:Concepts
                6575338, // Category:Concepts in epistemology
                368106, // Category:Critical thinking
                4571289, // Category:Mind
                5297085, // Category:Concepts in metaphysics
                1279771, // Category:Academic disciplines
                550759,  // Category:Categories by topic
                6400291, // Category:Society
                6760142  // Category:Interdisciplinary fields
            )
            
            // parent category => (child category, weight)
            val edgeList = ArrayBuffer[(Int, Int, Double)]()
            while ( !q.isEmpty )
            {
                val first = q.pop()

                var it = Utils.lowerBound( new EfficientIntIntDouble( first, 0, 0.0 ), hierarchy, (x:EfficientIntIntDouble, y:EfficientIntIntDouble) => x.less(y) )               
                while ( it < hierarchy.size && hierarchy(it).first == first )
                {
                    val row = hierarchy(it)
                    val parentId = row.second
                    val weight = row.third
                    
                    if ( !bannedCategories.contains(parentId) )//&& !tooFrequent.contains(parentId) )
                    {
                        if ( first != parentId )
                        {
                            edgeList.append( (first, parentId, weight) )
                            
                            if ( !seen.contains( parentId ) )
                            {
                                q.push( parentId )
                                seen = seen + parentId
                            }
                        }
                    }
                    it += 1
                }
            }
            
            edgeList
        }
    }
    
    class Edge( val source : Node, val sink : Node, var weight : Weight )
    {
        assert( weight >= 0.0 )
        assert( source != sink )
        
        var flow = 0
        source.addEdge( this )
        sink.addEdge( this )
        
        def remove()
        {
            source.edges -= this
            sink.edges -= this
        }
        
        def other( from : Node ) =
        {
            assert( from == source || from == sink )
            if ( from == source ) sink else source
        }
    }
    
    class Node( val id : Int )
    {
        var edges = HashSet[Edge]()
        var distance = Double.MaxValue
        var prev : Edge = null
        var enqueued = false
        var flow = 0
        
        def addEdge( edge : Edge )
        {
            edges += edge
        }
    }
    
    class Graph[Data]()
    {
        var allNodes = HashSet[Node]()
                
        def addNode( node : Node ) =
        {
            allNodes += node
        }
        
        
        def dijkstraVisit( starts : Seq[Node], weightFn : Edge => Weight, visitFn : (Node, Weight) => Unit )
        {
            for ( n <- allNodes )
            {
                n.distance = Double.MaxValue
                n.prev = null
                n.enqueued = false
            }
            
            val q = new NPriorityQ[Node]
            for ( start <- starts )
            {
                start.distance = 0.0
                start.enqueued = true
                q.add( start.distance, start )
            }
            
            while ( !q.isEmpty )
            {
                val (distance, node) = q.popFirst()
                node.enqueued = false
                
             
                visitFn( node, distance )
                for ( edge <- node.edges )
                {
                    val s = edge.other(node)
                    val edgeWeight = weightFn(edge)
                    if ( s.enqueued || s.distance == Double.MaxValue )
                    {
                        if ( s.distance != Double.MaxValue )
                        {
                            q.remove( s.distance, s )
                        }
                       
                        val thisDistance = distance + edgeWeight
                        if ( thisDistance < s.distance )
                        {
                            s.distance = thisDistance
                            s.prev = edge
                        }
                        
                        q.add( s.distance, s )
                        s.enqueued = true
                    }
                }    
            }
        }
        
        def connectedWithout( rootNodes : Iterable[Node], excludedEdge : Edge ) =
        {
            assert( !rootNodes.isEmpty )
            val q = Stack[Node]()
            var seenNodes = HashSet[Node]()
            
            q.push( rootNodes.head )
            
            while ( !q.isEmpty )
            {
                val top = q.pop()
                
                for ( edge <- top.edges if edge != excludedEdge )
                {
                    val other = edge.other(top)
                    if ( !seenNodes.contains(other) )
                    {
                        seenNodes += other
                        q.push( other )
                    }
                }
            }
            
            val connected = rootNodes.foldLeft(true)( (x, y) => x && seenNodes.contains(y) )

            connected
        }
    }
    
    class Builder( topicIds : Seq[Int], edges : Seq[(Int, Int, Double)] )
    {
        val g = new Graph()
        var topics : HashSet[Node] = null
        initialise( edges )
        
        def initialise( edges : Seq[(Int, Int, Double)] ) =
        {
            var idToNode = HashMap[Int, Node]()
            def getNode( id : Int ) =
            {
                if ( idToNode.contains(id) )
                {
                    idToNode(id)
                }
                else
                {
                    val n = new Node(id)
                    g.addNode( n )
                    idToNode = idToNode.updated(id, n)
                    n
                }
            }
            for ( (fromId, toId, weight) <- edges )
            {
                val from = getNode(fromId)
                val to = getNode(toId)
                val edge = new Edge( from, to, weight )
                //val edge = new Edge( from, to, 1.0 )
            }
            
            topics = topicIds.foldLeft(HashSet[Node]())( _ + getNode(_) )
        }
        
        def flowLabel( weightFn : Edge => Double )
        {
            // Reset all flows to zero
            for ( node <- g.allNodes )
            {
                node.flow = 0
                for ( e <- node.edges )
                {
                    e.flow = 0
                }
            }
            
            // Push a unit of flow down the shortest path from each topic node to each other topic node
            for ( topic <- topics )
            {
                g.dijkstraVisit( topic :: Nil, weightFn, (node, height) => {} )
                
                for ( innerTopic <- topics )
                {
                    var it = innerTopic
                    while ( it != null )
                    {
                        it.flow += 1
                        if ( it.prev != null )
                        {
                            it.prev.flow += 1
                            it = it.prev.other(it)
                        }
                        else it = null
                    }
                }
            }
        }
        
        def runPathCompression()
        {
            println( "Running path compression" )
            var modified = true
            var removedCount = 0
            while (modified)
            {
                modified = false
                for ( node <- g.allNodes )
                {
                    if ( !topics.contains( node ) )
                    {
                        if ( node.edges.size == 2 )
                        {
                            val asList = node.edges.toList
                            val firstEdge = asList(0)
                            val secondEdge = asList(1)
                            
                            //assert( firstEdge != secondEdge )
                            
                            val firstNode = firstEdge.other(node)
                            val secondNode = secondEdge.other(node)
                            
                            if ( !topics.contains( firstNode ) && !topics.contains( secondNode) )
                            { 
                                val conn = new Edge( firstNode, secondNode, firstEdge.weight + secondEdge.weight )
                                firstEdge.remove()
                                secondEdge.remove()
                                
                                firstNode.edges += conn
                                secondNode.edges += conn
                                
                                removedCount += 1
                                modified = true
                            }
                        }
                    }
                }
            }
            println( "  > removed " + removedCount + " edges" )
        }
        
        def dumpGraph()
        {
            for ( node <- g.allNodes )
            {
                println( ">> " + node.id + ": " + node.flow )
                for ( edge <- node.edges ) println( ":: " + edge.source.id + " <-> " + edge.sink.id + ": " + edge.flow + " (" + edge.weight + ")" )
            }
        }
        
        def run( topicDistance : (Int, Int) => Double ) =
        {
            // Run flow labelling to mark a subset of edges that are required to keep
            // the topic graph connected
            println( "Running flow labelling on graph" )
            flowLabel( e => e.weight )
            
            //dumpGraph()
            
            println( "Pruning zero flow edges" )
            
            // 2: Remove all edges with zero flow. And then all disconnected nodes
            {
                var liveNodes = HashSet[Node]()
                for ( node <- g.allNodes )
                {
                    node.edges = node.edges.filter( _.flow > 0 )
                    
                    for ( e <- node.edges )
                    {
                        liveNodes += e.source
                        liveNodes += e.sink
                    }
                }
                   
                g.allNodes = g.allNodes.filter( x => liveNodes.contains(x) )
            }
            
            //dumpGraph()
            
            // 2.5: Path compression
            runPathCompression()

            
            // 3: Run reverse-delete MST builder on reduced graph. Lowest weight first...
            println( "Running reverse-delete to get an MST" )
            val edges = ( for ( node <- g.allNodes; edge <- node.edges ) yield edge ).toList.sortWith( (x, y) => x.weight > y.weight )
            for ( edge <- edges )
            {
                if ( g.connectedWithout( topics, edge ) )
                {
                    // Remove edge
                    edge.remove()
                }
            }
            
            // 3.5: Chop overdistant edges
            runPathCompression()
            
            // We've chosen which category each topic should belong to. Reweight to zero
            for ( node <- topics )
            {
                //assert( node.edges.size == 1 )
                if ( node.edges.size == 1 )
                {
                    node.edges.head.weight = 0.0
                }
            }
            /*g.dijkstraVisit( topics.toList, x => x.weight, (x, y) => {} )
            for ( node <- g.allNodes )
            {
                if ( node.distance > 15.0 ) node.edges.toList.map( _.remove() ) 
            }*/

            // 4: Prune disconnected nodes
            println( "Pruning disconnected nodes" )
            g.allNodes = g.allNodes.filter( x => !x.edges.isEmpty )
            
           
            // Run distance labelling on the MST. The node with the greatest distance from all others
            // is a good choice for root of the tree.
            println( "Finding suitable tree roots and building MST" )
            g.dijkstraVisit( topics.toList, x => 1.0, (x, y) => {} )
            
            var maxFlowTrees = List[CategoryTreeElement]()
            
            var visited = HashSet[Node]()
            for ( maxFlowNode <- g.allNodes.filter( x => !topics.contains(x) ).toList.sortWith( (x, y) => x.distance > y.distance ) )
            {
                def buildTree( node : Node ) : CategoryTreeElement = 
                {
                    visited += node
                    def rec( node : Node ) : CategoryTreeElement =
                    {
                        val adjNodes = node.edges.map( e => (e.weight, e.other(node)) ).filter( en => !visited.contains(en._2) )
                        for ( (weight, n) <- adjNodes ) visited += n
                        new CategoryTreeElement( node, adjNodes.toList.map( en => (en._1, rec(en._2)) ) )
                    }
                    
                    rec(node)
                }
             
                if ( !visited.contains( maxFlowNode ) )
                {   
                    maxFlowTrees = buildTree(maxFlowNode) :: maxFlowTrees
                }   
            }
            
            for ( maxFlowTree <- maxFlowTrees )
            {
                def labelRec( cte : CategoryTreeElement ) : List[Node] =
                {
                    var allTopics = List[Node]()
                    if ( topics.contains( cte.node ) ) allTopics = cte.node :: allTopics
                    for ( (dist, c) <- cte.children )
                    {
                        allTopics = allTopics ++ labelRec(c)
                    }
                    
                    var aveDist = 0.0
                    var count = 0
                    for ( node1 <- allTopics; node2 <- allTopics if (node1.id < node2.id) )
                    {
                        val dist = topicDistance( node1.id, node2.id )
                        aveDist += dist
                        count += 1
                    }
                    
                    cte.coherence = if ( count == 0 ) 0.0 else aveDist / count.toDouble
                    
                    allTopics
                }
                
                labelRec( maxFlowTree )
            }
            
            maxFlowTrees
        }
    }
    
    /*
    type TopicId = Int
    
    class HierarchyBuilder( val topicIds : Seq[TopicId], graphEdges : Seq[(TopicId, TopicId, Double)] )
    {
        type NodeType = Node[TopicId]
        val g = new Graph[TopicId]()
        var idToNodeMap = HashMap[TopicId, NodeType]()
        
        init( graphEdges )
        
        private def init( graphEdges : Seq[(TopicId, TopicId, Double)] )
        {
            for ( (fromId, toId, weight) <- graphEdges )
            {
                val fromNode = getNode( fromId )
                val toNode = getNode( toId )
                
                fromNode.addSink( toNode, weight )
            }
        }
        
        private def getNode( topicId : TopicId ) =
        {
            if ( idToNodeMap.contains(topicId) )
            {
                idToNodeMap(topicId)
            }
            else
            {
                val newNode = g.newNode( topicId )
                idToNodeMap = idToNodeMap.updated( topicId, newNode )
                newNode
            }
        }
        
        def mergePass( liveTopics : HashSet[NodeType], getName: Int => String ) =
        {
            var merges = ArrayBuffer[(NodeType, List[NodeType])]()
            
            class MergeChoice( val theNode : NodeType, val height : Double, val mergeSize : Int )
            {
                def weight =
                {
                    val optimumSize = 6.0
                    val sizeDesirability = 1.0 / (pow(abs(optimumSize - mergeSize), 2.0) max 1.0)
                    
                    sizeDesirability / pow(height, 1.0)
                    //mergeSize / pow(height, 0.8)
                }
                
                def comp( other : MergeChoice ) =
                {
                    weight > other.weight
                }
            }
            
            var options = ArrayBuffer[MergeChoice]()
            g.dijkstraVisit( liveTopics.toSeq, (node, height) =>
            {
                // All incomings must be of lower height than the merge point
                val members = node.topicMembership
                val numMembers = members.size
                //val aveHeight = members.foldLeft(0.0)( _ + _._2 ) / numMembers.toDouble
                val maxHeight = members.foldLeft(0.0)( _ max _._2 )
                
                
                if ( numMembers > 1 )
                {
                    options.append( new MergeChoice( node, maxHeight, numMembers ) )
                }
            } )
            
            g.dumpLabellings( x => (x.data + "/" + getName( x.data )) )
            
            if ( options.size > 0 )
            {
                val mergeOptionsByWeight = options.toList.sortWith( _.weight > _.weight )
                
                var mergedAlready = HashSet[TopicId]()
                val beforeSize = merges.size
                
                for ( bestChoice <- mergeOptionsByWeight )
                {
                    val mergeNode = bestChoice.theNode
                    val mergingNodes = mergeNode.topicMembership.toList.map( _._1 )
                    
                    if ( mergingNodes.foldLeft( true )( (x, y) => x && !mergedAlready.contains(y.data) ) )
                    {
                        println( mergeNode.data + ", " + mergingNodes.map(_.data) + ": " + bestChoice.height + ", " + bestChoice.mergeSize + ", " + bestChoice.weight )                            
                        merges.append( (mergeNode, mergingNodes.toList) )
                        
                        for ( mn <- mergingNodes ) mergedAlready += mn.data
                    }
                }
                
                for ( (mergeNode, mergingNodes) <- merges.slice( beforeSize, merges.size ) )
                {
                    println( "Delabelling: " + mergingNodes.map(_.data) )
                    // Remove all labelling for the merged nodes
                    g.dijkstraVisit( liveTopics.toSeq, (node, height) =>
                    {
                        for ( n <- mergingNodes ) if ( node.topicMembership.contains(n) ) node.topicMembership -= n
                    } )
                    
                    for ( removed <- mergingNodes; n <-g.allNodes ) assert( !n.topicMembership.contains(removed) )
                }
            }
            
            merges
        }
        
        def updateTopicMembership( node : NodeType, topicNode : NodeType, height : Double )
        {
            // You can't really be a member of a topic of your route in is hugely longer than
            // the min route to the topic
            if ( height < node.minHeight * 1.2 )
            {
                node.topicMembership = node.topicMembership.updated( topicNode, height )
            }
        }
        
        def run( getName: Int => String ) =
        {
            var merges = ArrayBuffer[(NodeType, List[NodeType])]()
            var liveTopics = topicIds.foldLeft(HashSet[NodeType]())( _ + getNode(_) )
            
            println( "Set up the min distance field by doing an all-nodes visit." )
            g.dijkstraVisit( liveTopics.toSeq, (node, height) =>
            {
                println( "---> " + node.data + ", " + getName(node.data) + ": " + node.minHeight + ", " + height )
            } )
            
         
            // Label all nodes with their topic incidence and height
            println( "Label all nodes with their topic incidence and height" )
            for ( topicNode <- liveTopics )
            {
                println( "  labelling: " + topicNode.data )
                g.dijkstraVisit( topicNode :: Nil, (node, height) => updateTopicMembership( node, topicNode, height ) )
            }
            
            // Find the best merge point according to min topic height, num topics, topic importance (incident edge count?)
            var finished = false
            while ( liveTopics.size > 1 && !finished )
            {
                println( "Find the best merge points: " + liveTopics.size )
                
                // Run the merges
                var passComplete = false
                
                val thisPassMerges = ArrayBuffer[(NodeType, List[NodeType])]()
                
                // It would seem better not to force merges after doing a strip,
                // in case things end up over-generalising too early
                //while ( !passComplete )
                {
                    val thisRunMerges = mergePass( liveTopics, getName )
                    
                    if ( !thisRunMerges.isEmpty )
                    {
                        thisPassMerges.appendAll( thisRunMerges )
                        
                        for ( (into, froms) <- thisRunMerges; from <- froms ) liveTopics -= from
                    }
                    else
                    {
                        passComplete = true
                    }
                }
                
                if ( thisPassMerges.isEmpty )
                {
                    finished = true
                }
                else
                {
                    for ( (into, froms) <- thisPassMerges )
                    {
                        liveTopics += into
                        
                        // Label the graph up with the new merged node
                        g.dijkstraVisit( into :: Nil, (node, height) => updateTopicMembership( node, into, height ) )
                    }
                    merges.appendAll( thisPassMerges )
                }
            }
            
            var elementMap = HashMap[NodeType, scala.xml.Elem]()
            var hasParent = HashSet[NodeType]()
            for ( (mergeInto, froms) <- merges )
            {
                val el =
                    <element name={getName(mergeInto.data)} id={mergeInto.data.toString}>
                    {
                        for ( from <- froms ) yield
                        {
                            val element =
                                if ( elementMap.contains( from ) ) elementMap(from)
                                else <element name={getName(from.data)} id={from.data.toString}/>
                                
                            hasParent += from
                            
                            element
                        }
                    }
                    </element>
                
                elementMap = elementMap.updated(mergeInto, el)
            }

            val output =
                <structure>
                {
                    for ( (node, elem) <- elementMap.filter( x => !hasParent.contains(x._1) ) ) yield elem
                }
                </structure>
                
            XML.save( "structure.xml", output, "utf8" )
            merges.map( x => ( x._1.data, x._2.map( _.data ) ) )
        }
    }*/
}
