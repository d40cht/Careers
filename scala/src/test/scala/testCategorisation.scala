package org.seacourt.Graph

import org.scalatest.FunSuite
import scala.collection.mutable.Stack
import scala.collection.immutable.TreeSet


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

class GraphTests extends FunSuite
{
    test( "Strongly connected components test" )
    {
        val g = new Graph()
        
        val a = g.addNode( "a" )
		val b = g.addNode( "b" )
		val c = g.addNode( "c" )
		val d = g.addNode( "d" )
		val e = g.addNode( "e" )
		val f = g.addNode( "f" )
		
		g.addEdge( d, e )
		g.addEdge( e, f )
		g.addEdge( f, e )
		g.addEdge( f, a )
		g.addEdge( d, a )
		g.addEdge( a, b )
		g.addEdge( b, a )
		g.addEdge( a, c )
		g.addEdge( c, a )
		g.addEdge( b, c )
		g.addEdge( c, b )
		
		val sccs = g.connected()
		
		assert( sccs.length === 3 )
		val first = sccs.head
		val second = sccs.tail.head
		val third = sccs.tail.tail.head
		assert( first.length === 1 )
		assert( second.length === 2 )
		assert( third.length === 3 )
		
		//val v = TreeSet[String]( "hello", "world" )
		//assert( first.head.name === "d" )
		def getNames( l : List[Node] ) = l.foldLeft(TreeSet[String]())( _ + _.name )
		
		assert( getNames( first ) === TreeSet[String]("d") )
		assert( getNames( second ) === TreeSet[String]("e", "f") )
		assert( getNames( third ) === TreeSet[String]("a", "b", "c") )
		
		/*self.assertEqual( [a.id for a in sccs[0]], ['c', 'b', 'a'] )
		self.assertEqual( [a.id for a in sccs[1]], ['f', 'e'] )
		self.assertEqual( [a.id for a in sccs[2]], ['d'] )*/
    }
}



