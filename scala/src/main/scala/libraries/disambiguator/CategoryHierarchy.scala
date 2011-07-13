package org.seacourt.disambiguator

import org.seacourt.utility.{PriorityQ}
import scala.collection.immutable.{TreeMap, HashSet, HashMap}
import scala.collection.mutable.{ArrayBuffer}

object CategoryHierarchy
{
    type Height = Double
    
    class Node[Data]( val data : Data )
    {
        var sinks = HashSet[Node[Data]]()
        var height = Double.MaxValue
        var enqueued = false
        
        val topicMembership = ArrayBuffer[(Node[Data], Height)]()
        
        def addSink( sink : Node[Data] ) = (sinks += sink)
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
            
            val q = new PriorityQ[Node[Data]]
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
             
                for ( s <- node.sinks )
                {
                    if ( s.enqueued || s.height == Double.MaxValue )
                    {
                        if ( s.height != Double.MaxValue )
                        {
                            q.remove( s.height, s )
                        }
                       
                        s.height = s.height min (height+1)
                        
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
    
    class HierarchyBuilder( val topicIds : List[TopicId], graphEdges : List[(TopicId, TopicId)] )
    {
        type NodeType = Node[TopicId]
        val g = new Graph[TopicId]()
        var idToNodeMap = HashMap[TopicId, NodeType]()
        
        init( graphEdges )
        
        private def init( graphEdges : List[(TopicId, TopicId)] )
        {
            for ( (fromId, toId) <- graphEdges )
            {
                val fromNode = getNode( fromId )
                val toNode = getNode( toId )
                
                fromNode.addSink( toNode )
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
            var merges = ArrayBuffer[(TopicId, List[TopicId])]()
            var liveTopics = topicIds.foldLeft(HashSet[NodeType]())( _ + getNode(_) )
         
            // Label all nodes with their topic incidence and height
            println( "Label all nodes with their topic incidence and height" )
            for ( topicNode <- liveTopics )
            {
                println( "  labelling: " + topicNode.data )
                g.dijkstraVisit( topicNode :: Nil, (node, height) => node.topicMembership.append( (topicNode, height) ) )
            }
            
            // Find the best merge point according to min topic height, num topics, topic importance (incident edge count?)
            while ( liveTopics.size > 1 )
            {
                println( "Find the best merge point: " + liveTopics.size )
                var bestChoice : (NodeType, Double) = null
                g.dijkstraVisit( liveTopics.toList, (node, height) =>
                {
                    val members = node.topicMembership
                    val minHeight = height//members.foldLeft(Double.MaxValue)( _ min _._2 )
                    val numMembers = members.size
                    val mergeWeight = if (minHeight==0.0) 0.0 else numMembers / (minHeight * minHeight)
                    // Information here: node.topicMembership [(topicId, Height)]
                    
                    //println( node.data, members )
                    
                    if ( bestChoice == null || (mergeWeight > bestChoice._2 && numMembers > 1 ) )
                    {
                        bestChoice = (node, mergeWeight)
                    }
                } )
                
                println( bestChoice._1 + ", " + bestChoice._2 )
                val mergeNode = bestChoice._1
                val mergingNodes = mergeNode.topicMembership.map( _._1 )
                merges.append( (mergeNode.data, mergingNodes.toList.map(_.data)) )
                for ( n <- mergingNodes ) liveTopics -= n
                
                // Label the graph up with the new merged node
                g.dijkstraVisit( mergeNode :: Nil, (node, height) =>
                {
                    node.topicMembership.append( (mergeNode, height) )
                } )
                liveTopics += mergeNode
            }
            
            merges
        }
    }
}
