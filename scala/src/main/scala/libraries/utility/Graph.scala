package org.seacourt.utility

import scala.collection.mutable.Stack

class Node( val id : Int, val name : String )
{
    var sinks = List[Node]()
    var index : Int = -1
    var lowlink : Int = -1
    var onStack = false
    
    def addSink( sink : Node )
    {
        sinks = sink :: sinks
    }
}

class Graph()
{
    var nodes = List[Node]()
    var lastId = 0
    
    def addNode( name : String ) =
    {
        val theNode = new Node( lastId, name )
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
        
        private def tarjan( node : Node )
        {
            //println( ":::::::: visiting: " + node.name )
            stack.push( node )
            node.onStack = true
            node.index = index
            node.lowlink = index
            index += 1
            
            for ( sink <- node.sinks )
            {
                if ( sink.index == -1 )
                {
                    tarjan( sink )
                    node.lowlink = node.lowlink min sink.lowlink
                }
                else if ( sink.onStack )
                {
                    node.lowlink = node.lowlink min sink.index
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
        
        def run() : List[List[Node]] =
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
                    tarjan( node )
                }
            }
            
            sccs
        }
    }
    
    def connected() =
    {
        val c = new Connected()
        c.run()
    }
}

