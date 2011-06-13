import org.scalatest.FunSuite
import scala.collection.immutable.TreeSet

import org.seacourt.utility.{Graph, Node}


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
		
		def getNames( l : List[Node] ) = l.foldLeft(TreeSet[String]())( _ + _.name )
		
		assert( getNames( first ) === TreeSet[String]("d") )
		assert( getNames( second ) === TreeSet[String]("e", "f") )
		assert( getNames( third ) === TreeSet[String]("a", "b", "c") )
    }
}



