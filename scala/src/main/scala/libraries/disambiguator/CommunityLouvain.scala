package org.seacourt.disambiguator.Community

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
}



