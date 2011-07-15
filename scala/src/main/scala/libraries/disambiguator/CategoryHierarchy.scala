package org.seacourt.disambiguator

import scala.collection.immutable.{TreeMap, HashSet, HashMap}
import scala.collection.mutable.{ArrayBuffer, Stack}
import math.{log, pow, abs}
import java.io.{File, DataInputStream, FileInputStream}

import org.seacourt.utility.{NPriorityQ}
import org.seacourt.sql.SqliteWrapper._
import org.seacourt.utility._


object CategoryHierarchy
{
    type Height = Double
    val overbroadCategoryCount = 10
    
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
                368106, // Category:Critical thinking
                4571289, // Category:Mind
                5297085, // Category:Concepts in metaphysics
                1279771, // Category:Academic disciplines
                550759,  // Category:Categories by topic
                6400291, // Category:Society
                6760142  // Category:Interdisciplinary fields
            )
            
            //var edgeList = ArrayBuffer[(Int, Int, Double)]()
            
            // parent category => (child category, weight)
            var edgeMap = TreeMap[Int, List[(Int, Double)]]()
            while ( !q.isEmpty )
            {
                val first = q.pop()

                var it = Utils.lowerBound( new EfficientIntIntDouble( first, 0, 0.0 ), hierarchy, (x:EfficientIntIntDouble, y:EfficientIntIntDouble) => x.less(y) )               
                while ( it < hierarchy.size && hierarchy(it).first == first )
                {
                    val row = hierarchy(it)
                    val parentId = row.second
                    val weight = row.third
                    
                    if ( !bannedCategories.contains(parentId) )
                    {
                        //edgeList.append( (first, parentId, weight) )
                        val existingChildren = edgeMap.getOrElse( parentId, List[(Int, Double)]() )
                        edgeMap = edgeMap.updated( parentId, (first, weight) :: existingChildren )
                        
                        if ( !seen.contains( parentId ) )
                        {
                            q.push( parentId )
                            seen = seen + parentId
                        }
                    }
                    it += 1
                }
            }
            
            var edgeList = ArrayBuffer[(Int, Int, Double)]()
            val topicIdSet = topicIds.foldLeft(HashSet[Int]())( _ + _ )
            for ( (parentId, froms) <- edgeMap )
            {
                val topicFroms = froms.filter( x => topicIdSet.contains(x._1) )
                val categoryFroms = froms.filter( x => !topicIdSet.contains(x._1) ).sortWith( (x,y) => x._2 > y._2 ).slice(0, 50)
                
                for ( (fromId, weight) <- (topicFroms++categoryFroms) ) edgeList.append( (fromId, parentId, weight) )
            }
            
            /*println( "Looking up category weights" )
            val weightQuery = topicDb.prepare( "SELECT count FROM topicInboundLinks WHERE topicId=?", Col[Int]::HNil )
            val categoryWeights = seen.toList.foldLeft( TreeMap[Int, Double]() )( (map, id) =>
            {
                weightQuery.bind( id )
                val count = _1(weightQuery.toList(0)).get
                map.updated(id, log( count.toDouble ))
            } )*/
            
            edgeList
        }
    }
    
    class Node[Data]( val data : Data )
    {
        var sinks = HashMap[Node[Data], Double]()
        var height = Double.MaxValue
        var minHeight = Double.MaxValue
        var enqueued = false
        
        var topicMembership = HashMap[Node[Data], Height]()
        
        def addSink( sink : Node[Data], weight : Double )
        {
            assert( weight >= 0.0 )
            sinks = sinks.updated(sink, weight)
        }
    }
    
    class Graph[Data]()
    {
        var allNodes = HashSet[Node[Data]]()
        
        def newNode( data : Data ) =
        {
            val nn = new Node( data )
            allNodes += nn
            
            nn
        }
        
        def dumpLabellings( fn : Node[Data] => String )
        {
            for ( node <- allNodes.toList.sortWith( _.topicMembership.size < _.topicMembership.size ) )
            {
                if ( node.height != Double.MaxValue )
                {
                    println( fn( node ) + ": " + node.height + " " + node.enqueued + "<" + node.topicMembership.size + ">")
                    for ( membership <- node.topicMembership )
                    {
                        println( "  " + fn(membership._1) + " - " + membership._2 )
                    }
                }
            }
        }

        
        def dijkstraVisit( starts : Seq[Node[Data]], visitFn : (Node[Data], Height) => Unit )
        {
            for ( n <- allNodes )
            {
                n.height = Double.MaxValue
                n.enqueued = false
            }
            
            val q = new NPriorityQ[Node[Data]]
            for ( s <- starts )
            {
                s.height = 0.0
                s.enqueued = true
                q.add( s.height, s )
            }
            
            while ( !q.isEmpty )
            {
                //println( "Q size: " + q.size )
                val (height, node) = q.popFirst()
                node.enqueued = false
                
                if ( node.minHeight > node.height )
                {
                    node.minHeight = node.height
                }
                visitFn( node, height )
                
             
                for ( (s, weight) <- node.sinks )
                {
                    if ( s.enqueued || s.height == Double.MaxValue )
                    {
                        if ( s.height != Double.MaxValue )
                        {
                            q.remove( s.height, s )
                        }
                       
                        s.height = s.height min (height + weight)
                        
                        q.add( s.height, s )
                        s.enqueued = true
                    }
                }    
            }
        }
        
        def filter( starts : Seq[Node[Data]], filterPredicate : Node[Data] => Boolean )
        {
            dijkstraVisit( starts,
            { (node, height) =>
                if ( !filterPredicate( node ) )
                {
                    allNodes = allNodes - node
                }
            } )
        }
    }
    
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
                
                fromNode.addSink( toNode, -log(weight) )
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
                    val optimumSize = 4.0
                    val sizeDesirability = 1.0 / (pow(abs(optimumSize - mergeSize), 2.0) max 1.0)
                    
                    sizeDesirability / pow(height, 1.0)
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
                val aveHeight = members.foldLeft(0.0)( _ + _._2 ) / numMembers.toDouble
                
                
                if ( numMembers > 1 )
                {
                    options.append( new MergeChoice( node, aveHeight, numMembers ) )
                }
            } )
            
            //g.dumpLabellings( x => (x.data + "/" + getName( x.data )) )
            
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
            if ( height < node.minHeight * 1.3 )
            {
                node.topicMembership = node.topicMembership.updated( topicNode, height )
            }
        }
        
        def run( getName: Int => String ) =
        {
            var merges = ArrayBuffer[(NodeType, List[NodeType])]()
            var liveTopics = topicIds.foldLeft(HashSet[NodeType]())( _ + getNode(_) )
            
            println( "Set up the min distance field by doing an all-nodes visit." )
            g.dijkstraVisit( liveTopics.toSeq, (x, y) => Unit )
         
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
                while ( !passComplete )
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
            
            merges.map( x => ( x._1.data, x._2.map( _.data ) ) )
        }
    }
}
