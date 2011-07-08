package org.seacourt.disambiguator.Community

import scala.collection.mutable.ArrayBuffer
import scala.collection.immutable.HashMap
import sbt.Process._
import java.io._
import scala.io.Source._

import org.seacourt.utility._

abstract class CommunityTreeBase
{
    def size : Int
}

case class InternalNode( var children : ArrayBuffer[CommunityTreeBase] ) extends CommunityTreeBase
{
    def this() = this( ArrayBuffer[CommunityTreeBase]() )
    def size = children.size
}

case class LeafNode( val children : ArrayBuffer[Int] ) extends CommunityTreeBase
{
    def this() = this( ArrayBuffer[Int]() )
    def size = children.size
}

class Louvain
{
    private var convertPath     = "/home/alexw/AW/optimal/community/convert"
    private var communityPath   = "/home/alexw/AW/optimal/community/community"
    
    private var idMap = HashMap[Int, Int]()
    private var nextId = 0
    
    class Edge( val from : Int, val to : Int, val weight : Double )
    
    private var edgeList = List[Edge]()
    
    private def getNodeId( node : Int ) =
    {
        val id = idMap.getOrElse( node, nextId )
        if ( id == nextId )
        {
            idMap = idMap.updated( node, nextId )
            nextId += 1
        }
        
        id
    }
    
    def addEdge( from : Int, to : Int, weight : Double )
    {
        val fromId = getNodeId( from )
        val toId = getNodeId( to )
        
        edgeList = new Edge( fromId, toId, weight ) :: edgeList
    }
    
    
    
    def run() =
    {
        var strip = ArrayBuffer[CommunityTreeBase]()
        
        Utils.withTemporaryDirectory
        { dir =>
            val edgeFile = new PrintWriter( new File( "%s/edges.txt".format( dir ) ) )
            edgeList.map( x => edgeFile.println( "%d %d %f".format( x.from, x.to, x.weight ) ) )
            edgeFile.close()
            
            // Convert the data to appropriate binary format
            val convertCmd = "%s -i %s/edges.txt -o %s/edges.bin -w %s/weights.bin".format( convertPath, dir, dir, dir )
            convertCmd !
            
            // And run the community algo  
            val communityCmd = "%s %s/edges.bin -w %s/weights.bin -l -1".format( communityPath, dir, dir, dir )
            val res : String = communityCmd !!
            
            // Parse the output and build the tree
            val assignments = res.split("\n").map( l =>
            {
                val els = l.split(" ")
                assert( els.size == 2 )
                (els.head.toInt, els.tail.head.toInt)
            } )
            
            val sizes = ArrayBuffer[Int]()
            
            {
                var lastId = -1
                var maxGroupId = 0
                for ( (id, groupId) <- assignments )
                {
                    if ( id <= lastId )
                    {
                        sizes.append( maxGroupId+1 )
                        maxGroupId = 0
                    }
                    maxGroupId = maxGroupId max groupId
                    lastId = id
                }
                sizes.append( maxGroupId+1 )
            }
            
            val reverseLookup = idMap.foldLeft( HashMap[Int, Int]() )( (hm, v) => hm.updated( v._2, v._1 ) )

            var lastStrip : ArrayBuffer[CommunityTreeBase] = null
            
            for ( i <- 0 until sizes(0) ) strip.append( new LeafNode() )
            var index = 1
            var lastId = -1
            for ( (id, groupId) <- assignments )
            {
                if ( id <= lastId )
                {
                    lastStrip = strip
                    strip = ArrayBuffer[CommunityTreeBase]()
                    for ( i <- 0 until sizes(index) ) strip.append( new InternalNode() )
                    index += 1
                }
                
                (index, strip(groupId)) match
                {
                    case (1, v : LeafNode) => v.children.append( reverseLookup(id) )
                    case (_, v : InternalNode) => v.children.append( lastStrip(id) )
                    
                    case _ => throw new MatchError( "Mismatch" )
                }
                
                lastId = id
            }
        }
        
        new InternalNode( strip )
    }
}

/*
import scala.collection.mutable.ArrayBuffer

// We only handle the weighted case.
class Graph( val nb_nodes : Int, val nb_links : Int )
{
    var total_weight    = 0.0
    
    var degrees         = ArrayBuffer[Int]()
    var links           = ArrayBuffer[Int]()
    var weights         = ArrayBuffer[Double]()
    
    def nb_neighbours( node : Int ) =
    {
        assert( node >= 0 && node < nb_nodes )
        
        if ( node == 0 ) degrees(0)
        else degrees(node) - degrees(node-1)
    }

    def neighbours( node : Int ) =
    {
        assert( node >= 0 && node < nb_nodes )
        
        val from = if (node==0) 0 else degrees(node-1)
        val to = if (node==nb_nodes-1) links.size-1 else degrees(node)
        
        (links.slice(from, to), weights.slice(from, to))
    }
    
    def nb_selfloops( node : Int ) : Double =
    {
        assert( node >= 0 && node < nb_nodes )
        
        val (ls, ws) = neighbours(node)
        for ( (to, w) <- ls.zip(ws) )
        {
            if (to == node) return w
        }
        return 0.0
    }

    def weighted_degree( node : Int ) =
    {
        assert( node >= 0 && node < nb_nodes )
        
        val (ls, ws) = neighbours(node)
        
        ws.foldLeft(0.0)( _ + _ )
    }
}


class Community
{
    val neigh_weight    = new ArrayBuffer[Double]()
    val neigh_pos       = new ArrayBuffer[Double]()
    var neigh_last      = 0
    
    val g               = new Graph( 0, 0 )
    val size            = 0
    val n2c             = new ArrayBuffer[Int]()
    val in              = new ArrayBuffer[Double]()
    val tot             = new ArrayBuffer[Double]()
    
    val nb_pass         = 0
    val min_modularity  = 0.0
    
    def remove( node : Int, comm : Int, dnodecomm : Double )
    {
        assert( node >= 0 && node < size )
        
        tot(comm)   -= g.weighted_degree(node)
        in(comm)    -= 2.0 * dnodecomm + g.nb_selfloops(node)
        n2c(comm)   = -1
    }
    
    def insert( node : Int, comm : Int, dnodecomm : Double )
    {
        assert( node >= 0 && node < size )
        
        tot(comm)   += g.weighted_degree(node)
        in(comm)    += 2.0 * dnodecomm + g.nb_selfloops(node)
        n2c(node)   = comm
    }
    
    def modularity_gain( node : Int, comm : Int, dnodecomm : Double, w_degree : Double ) =
    {
        assert( node >= 0 && node < size )
        
        val totc    = tot(comm)
        val degc    = w_degree
        val m2      = g.total_weight
        val dnc     = dnodecomm
        
        dnc - totc*degc/m2
    }
    
    def modularity() =
    {
        var q = 0.0
        val m2 = g.total_weight
        
        for ( (i, t) <- in.zip(tot) )
        {
            if ( t > 0 ) q += (i/m2) - ((t/m2)*(t/m2))
        }
        
        q
    }
  
    def neigh_comm( node : Int )
    {
        for ( i <- 0 until neigh_last )
        {
            neigh_weight(neigh_pos(i)) = -1
        }
        neigh_last = 0
        
        val (links, weights) = g.neighbours(node)
        val deg = g.nb_neighbours(node)
        
        neigh_pos(0) = n2c(node)
        neigh_weight(neigh_pos(0)) = 0
        neigh_last = 1
        
        for ( (neigh, neigh_w) <- links.zip(weights) )
        {
            val neigh_comm = n2c(neigh)
            if ( neigh != node )
            {
                if ( neigh_weight(neigh_comm) == -1 )
                {
                    neigh_weight(neigh_comm) = 0.0
                    neigh_pos(neigh_last) = neigh_comm
                    neigh_last += 1
                }
                neigh_weight(neigh_comm) += neigh_w
            }
        }
    }
    
    private def get_renumber() =
    {
        val renumber = ArrayBuffer[Int]()
        for ( i <- 0 until size ) renumber.append(-1)
        for ( v <- n2c ) renumber(v) += 1
        
        val finali = 0
        for ( i <- 0 until size )
        {
            if ( renumber(i) != -1 )
            {
                renumber(i) = finali
                finali += 1
            }
        }
        
        (renumber, finali)
    }

    def partition2graph()
    {
        val (renumber, finali) = get_renumber()
        
        for ( i <- 0 until size )
        {
            val (links, weights) = g.neighbours(i)
            
            for ( neigh <- links )
            {
                println( renumber(n2c(i)) + " " + renumber(nc2(neigh)) )
            }
        }
    }
    
    
    
    def display_partition()
    {
        val (renumber, finali) = get_renumber()
        
        for ( i <- 0 until size )
        {
            println( i + " " + renumber(n2c(i)) )
        }
    }
    
    def partition2graph_binary() =
    {
        val (renumber, finali) = get_renumber()
        
        // Compute communities
        val comm_nodes = ArrayBuffer[ArrayBuffer[Int]]()
        for ( i <- 0 until finali ) comm_nodes.append( ArrayBuffer[Int]() )
        for ( i <- 0 until size ) comm_nodes(renumber(n2c(node))).append(i)
        
        // Compute weighted graph
        val g2 = new Graph( finali )

        val comm_deg = finali
        
        for ( comm <- 0 until comm_deg )
        {
            val m = TreeMap[Int, Double]()
            val comm_size = comm_nodes(comm).length
            
            for ( node <- 0 until comm_size )
            {
                (links, weights) = g.neighbours( comm_nodes(comm)(node) )
                val deg = g.nb_neighbours( comm_nodes(comm)(node) )
                
                for ( (neigh, neight_weight) <- links.zip(weights) )
                {
                    val neigh_comm = renumber(n2c(neigh))
                    
                    val oldWeight = m.getOrElse(neigh_comm, 0.0)
                    m(neigh_comm) = oldWeight + neigh_weight
                }
                
                g2.degrees(comm) = if (comm==0) m.size() else g2.degrees(comm-1) + m.size()
                g2.nb_links += m.size()
                
                for ( (node, weight) <- m )
                {
                    g2.total_weight += weight
                    g2.links.append( node )
                    g2.weights.append( weight )
                }
            }
        }
        
        g2
    }
    
    def one_level() =
    {
        var improvement = false
        var nb_moves = 0
        var nb_pass_done = 0
        
        val new_mode = modularity()
        val curr_mod = new_mod
        
        // AW: There must be a java/scala function for this. Also not keen on the non-determinism
        val random_order = ArrayBuffer[Int]()
        for ( i <- 0 until size ) random_order.append(i)
        for ( i <- 0 until size )
        {
            val rand_pos = rand() % (size-i)+i
            val tmp = random_order(i)
            random_order(i) = random_order(rand_pos)
            random_order(rand_pos) = tmp
        }
        
        var run = true
        while (run)
        {
            cur_mod = new_mod
            nb_moves = 0
            nb_pass_done += 1
            
            for ( node_tmp <- 0 until size )
            {
                val node = random_order(node_tmp)
                val node_comm = n2c(node)
                val w_degree = g.weighted_degree(node)
                
                neigh_comm(node)
                remove(node, node_comm, neigh_weight(node_comm))
                
                val best_comm = node_comm
                val best_bnlinks = 0.0
                val best_increase = 0.0
                
                for ( i <- 0 until neigh_last )
                {
                    val increase = modularity_gain(node, neigh_pos(i), neigh_weight(neigh_pos(i)), w_degree)
                    if ( increase > best_increase )
                    {
                        best_comm = neigh_pos(i)
                        best_nblinks = neigh_weight(best_comm)
                        best_increase = increase
                    }
                }
                
                insert( node, best_comm, best_nblinks )
                
                if ( best_comm != node_comm ) nb_moves++
            }
            
            val total_tol = 0.0
            val total_in = 0.0
            for ( i <- 0 until tot.length )
            {
                total_tot += tot(i)
                total_in += in(i)
            }
            
            new_mode = modularity()
            if ( nb_moves > 0 ) improvement = true
            
            run = nb_moves > 0 && new_mod-cur_mod>min_modularity
        }
        improvement
    }
}

*/


