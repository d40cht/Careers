package org.seacourt.disambiguator

import org.seacourt.utility.{PriorityQ}
import scala.collection.immutable.{TreeMap, HashSet, HashMap}
import scala.collection.mutable.{ArrayBuffer}

import math.{log, pow}

object CategoryHierarchy
{
    type Height = Double
    
    class Node[Data]( val data : Data )
    {
        var sinks = HashSet[Node[Data]]()
        var height = Double.MaxValue
        var enqueued = false
        
        var topicMembership = HashMap[Node[Data], Height]()
        
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
                g.dijkstraVisit( topicNode :: Nil, (node, height) => node.topicMembership = node.topicMembership.updated(topicNode, height) )
            }
            
            // Find the best merge point according to min topic height, num topics, topic importance (incident edge count?)
            while ( liveTopics.size > 1 )
            {
                println( "Find the best merge point: " + liveTopics.size )
                
                class MergeChoice( val theNode : NodeType, val height : Double, val mergeSize : Int )
                {
                    def comp( other : MergeChoice ) =
                    {
                        //(log(mergeSize)/(height) > (log(other.mergeSize)/other.height)
                        (mergeSize/pow(height, 2.0)) > (other.mergeSize/pow(other.height, 2.0))
                    }
                }
                
                var bestChoice : MergeChoice = null
                g.dijkstraVisit( liveTopics.toList, (node, height) =>
                {
                    val members = node.topicMembership
                    val maxHeight = members.foldLeft(0.0)( _ max _._2 )
                    val numMembers = members.size
                    
                    //println( node.data, members )
                    
                    
                    if ( (numMembers > 1) )
                    {
                        val thisChoice = new MergeChoice( node, maxHeight, numMembers )
                        if ( (bestChoice == null || thisChoice.comp( bestChoice )) )
                        {
                            bestChoice = thisChoice
                        }
                    }
                } )
                
                //assert( bestChoice != null )
                if ( bestChoice == null )
                {
                    liveTopics = HashSet[NodeType]()
                }
                else
                {
                    val mergeNode = bestChoice.theNode
                    val mergingNodes = mergeNode.topicMembership.toList.map( _._1 )
                    println( mergeNode + ", " + mergingNodes.map(_.data) + ": " + bestChoice.height + ", " + bestChoice.mergeSize )
                    merges.append( (mergeNode.data, mergingNodes.toList.map(_.data)) )
                    
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
            
            merges
        }
    }
}
