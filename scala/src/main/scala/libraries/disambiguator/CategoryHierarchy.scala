package org.seacourt.disambiguator

import org.seacourt.utility.{NPriorityQ}
import scala.collection.immutable.{TreeMap, HashSet, HashMap}
import scala.collection.mutable.{ArrayBuffer}

import math.{log, pow, abs}

object CategoryHierarchy
{
    type Height = Double
    
    class Node[Data]( val data : Data )
    {
        var sinks = HashMap[Node[Data], Double]()
        var height = Double.MaxValue
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
        
        def dijkstraVisit( starts : List[Node[Data]], visitFn : (Node[Data], Height) => Unit )
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
        
        def filter( starts : List[Node[Data]], filterPredicate : Node[Data] => Boolean )
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
    
    class HierarchyBuilder( val topicIds : List[TopicId], graphEdges : List[(TopicId, TopicId, Double)] )
    {
        type NodeType = Node[TopicId]
        val g = new Graph[TopicId]()
        var idToNodeMap = HashMap[TopicId, NodeType]()
        
        init( graphEdges )
        
        private def init( graphEdges : List[(TopicId, TopicId, Double)] )
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
        
        def run() =
        {
            var merges = ArrayBuffer[(NodeType, List[NodeType])]()
            var liveTopics = topicIds.foldLeft(HashSet[NodeType]())( _ + getNode(_) )
         
            // Label all nodes with their topic incidence and height
            println( "Label all nodes with their topic incidence and height" )
            for ( topicNode <- liveTopics )
            {
                println( "  labelling: " + topicNode.data )
                g.dijkstraVisit( topicNode :: Nil, (node, height) => node.topicMembership = node.topicMembership.updated(topicNode, height) )
            }
            
            // Find the best merge point according to min topic height, num topics, topic importance (incident edge count?)
            while ( liveTopics.size > 1 )
            {
                println( "Find the best merge point: " + liveTopics.size )
                
                class MergeChoice( val theNode : NodeType, val height : Double, val mergeSize : Int )
                {
                    def weight =
                    {
                        val optimumSize = 10.0
                        val sizeDesirability = if ( mergeSize < optimumSize ) mergeSize else optimumSize*(optimumSize / mergeSize)
                        sizeDesirability / pow(height, 1.0)
                    }
                    
                    def comp( other : MergeChoice ) =
                    {
                        weight > other.weight
                    }
                }
                
                var options = ArrayBuffer[MergeChoice]()
                g.dijkstraVisit( liveTopics.toList, (node, height) =>
                {
                    val members = node.topicMembership
                    val numMembers = members.size
                    val maxHeight = members.foldLeft(0.0)( _ + _._2 ) / numMembers.toDouble
                    
                    if ( numMembers > 1 )
                    {
                        options.append( new MergeChoice( node, maxHeight, numMembers ) )
                    }
                } )
                
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
                        g.dijkstraVisit( liveTopics.toList, (node, height) =>
                        {
                            for ( n <- mergingNodes ) if ( node.topicMembership.contains(n) ) node.topicMembership -= n
                        } )
                        
                        for ( removed <- mergingNodes; n <-g.allNodes ) assert( !n.topicMembership.contains(removed) )
                        
                        // Label the graph up with the new merged node
                        g.dijkstraVisit( mergeNode :: Nil, (node, height) =>
                        {
                            node.topicMembership = node.topicMembership.updated(mergeNode, height)
                        } )
                           
                        for ( n <- mergingNodes )
                        {
                            liveTopics -= n
                        }
                        liveTopics += mergeNode
                    }
                }
                else
                {
                    // Clear and exit
                    liveTopics = HashSet[NodeType]()
                }
            }
            
            merges.map( x => ( x._1.data, x._2.map( _.data ) ) )
        }
    }
}
