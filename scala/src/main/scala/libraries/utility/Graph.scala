package org.seacourt.utility

import scala.collection.mutable.Stack

class NodeClass[NodeInfo]( val id : Int, val info : NodeInfo )
{
    var sinks = List[NodeClass[NodeInfo]]()
    var index : Int = -1
    var lowlink : Int = -1
    var onStack = false
    var edgeCounts = 0
    
    def addSink( sink : NodeClass[NodeInfo] )
    {
        sinks = sink :: sinks
        edgeCounts += 1
        sink.edgeCounts += 1
    }
}

class Graph[NodeInfo]()
{
    type Node = NodeClass[NodeInfo]    

    var nodes = List[Node]()
    var lastId = 0
    
    def addNode( info : NodeInfo ) =
    {
        val theNode = new Node( lastId, info )
        lastId += 1
        nodes = theNode :: nodes
        
        theNode
    }
    
    def addEdge( from : Node, to : Node )
    {
        from.addSink( to )
    }
    
    class Connected()
    {
        var sccs = List[List[Node]]()
        var stack = Stack[Node]()
        var index = 0
        
        private def tarjan( node : Node, minEdges : Int )
        {
            //println( ":::::::: visiting: " + node.name )
            
            if ( node.edgeCounts >= minEdges )
            {
                stack.push( node )
                node.onStack = true
                node.index = index
                node.lowlink = index
                index += 1
                
                for ( sink <- node.sinks )
                {
                    if ( sink.edgeCounts >= minEdges )
                    {
                        if ( sink.index == -1 )
                        {
                            tarjan( sink, minEdges )
                            node.lowlink = node.lowlink min sink.lowlink
                        }
                        else if ( sink.onStack )
                        {
                            node.lowlink = node.lowlink min sink.index
                        }
                    }
                }
                
                if ( node.lowlink == node.index )
                {
                    var scc = List[Node]()
                    var done = false
                    while ( !done )
                    {
                        val front = stack.pop()
                        front.onStack = false
                        scc = front :: scc
                        if ( node == front )
                        {
                            done = true
                        }
                    }
                    
                    sccs = scc :: sccs
                }
            }
        }
        
        def run( minEdges : Int ) : List[List[Node]] =
        {
            for ( node <- nodes )
            {
                node.index = -1
                node.lowlink = -1
                node.onStack = false
            }
            
            for ( node <- nodes )
            {   
                if ( node.index == -1 )
                {
                    // Run Tarjan
                    tarjan( node, minEdges )
                }
            }
            
            sccs
        }
    }
    
    def connected( minEdges : Int ) =
    {
        val c = new Connected()
        c.run( minEdges )
    }
}

