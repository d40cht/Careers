package org.seacourt.tests

import org.scalatest.FunSuite
import org.scalatest.Tag

import scala.collection.immutable.{HashMap}
import scala.collection.mutable.{ArrayBuffer}

import scala.math.{log, pow, ceil}
import scala.xml._
import scala.io.Source._
import org.seacourt.utility._
import org.seacourt.disambiguator.{WrappedTopicId, AgglomClustering, TopicVector, TopicElement, DocumentDigest}



import org.seacourt.serialization.SerializationProtocol._

class DistanceMetricTest extends FunSuite
{
    test( "DistanceMetricTest", TestTags.largeDataTests )
    {
        def wikiLink( topicName : String ) =
        {
            val wikiBase = "http://en.wikipedia.org/wiki/"
            wikiBase + (if (topicName.startsWith("Main:")) topicName.drop(5) else topicName)
        }
        
        val names = ArrayBuffer( "Alex", "Gav", "Steve", "Sem", "George", "George", "Alistair", "Chris", "Sarah", "Rob", "Croxford", "EJ", "Nils", "Zen", "Susanna", "Karel", "Tjark", "Jasbir", "Jasbir", "Pippo", "Olly", "Margot", "Sarah T", "Charlene Watson", "Nick Hill", "Jojo", "Matthew Schumaker", "Some quant dude off the web", "A second quant dude off the web", "Pete Williams web dev", "Jackie Lee web dev", "Katie McGregor", "David Green (env consultant)", "Mark Tarrant", "Mike Pepler" )
        
        val range = 1 until 3//(names.size+1)
        val dds = range.map( i => sbinary.Operations.fromFile[DocumentDigest]( new java.io.File( "./documentDigest%d.bin".format(i) ) ) )
        
        
        def renderGroup( rootId : String, groupMembership : List[(Int, String, Boolean, Double)], klass : String ) =
        {
            <div style="padding:6px" class={klass} id={rootId}>
            {
                var colours = List( "#a0a040", "#40a0a0" )
                var greys = List( "#606060", "#a0a0a0" )
                for ( ((rank, name, primaryTopic, weight), i) <- groupMembership.sortWith( _._4 > _._4 ).zipWithIndex ) yield
                {
                    //val fontSize = (17 + log(weight).toInt) min 10
                    val reweight = (log(weight*10000.0) max 0.0)
                    var styles = "text-decoration:none" :: "font-size: %.2f%%".format( 100.0 * (0.7 + reweight/15.0) ) :: Nil
                    if ( primaryTopic )
                    {
                        styles = "color: %s; font-weight: bold".format(colours.head) :: styles
                        colours = colours.tail ++ List(colours.head)
                    }
                    else
                    {
                        styles = "color: %s".format(greys.head) :: styles
                        greys = greys.tail ++ List(greys.head)
                    }
                    
                    <span class={klass} id={"%s_%d".format(rootId, i)}><a href={wikiLink(name)} style={styles.mkString(";")}>{ name.replace( "Main:", "" ) }</a></span><span> </span>
                }
            }
            </div>
        }
        
        val res =
            <html>
                <head>
                <link rel="stylesheet" type="text/css" href="./public/stylesheets/style.css"></link>
                <script type="text/javascript" src="./public/javascripts/jquery-1.5.2.min.js"></script>
                <script type="text/javascript" src="./public/javascripts/script.js"></script>
                </head>
                <body style="font-family:sans-serif">
                {
                    for ( (dd1, ddIndex) <- dds.zipWithIndex ) yield
                    {
                        val tv1 = dd1.topicVector
                        val rankedTopics = tv1.topics.map( _._2 ).filter( _.primaryTopic ).toList.sortWith( _.weight > _.weight ).zipWithIndex
                        var grouped = new AutoMap[Int, List[(Int, TopicElement)]]( x => Nil )
                        for ( (te, rank) <- rankedTopics )
                        {
                            grouped.set( te.groupId, (rank, te) :: grouped(te.groupId) )
                        }
                        
                        val groupsByRank = grouped.map( el =>
                        {
                            val (gid, tes) = el
                            
                            var sum = 0.0
                            var count = 0
                            for ( (rank, te) <- tes )
                            {
                                sum += rank
                                count += 1
                            }

                            (sum/count.toDouble, tes )
                        } ).toList.sortWith( _._1 < _._1 ).map( _._2 )
                        
                        
                        <div style="text-align:justify">
                            <h1>{names(dd1.id-1)}</h1>
                            
                            <table>
                            {
                                val asArr = groupsByRank.toArray
                                val numCells = asArr.size
                                val numCols = 6
                                val numRows = ceil(numCells.toDouble / numCols.toDouble).toInt
                                var i = 0
                                for ( r <- 0 until numRows ) yield
                                {
                                    <tr>
                                    {
                                        for ( c <- 0 until numCols ) yield
                                        {
                                            val cell = if ( i < asArr.size )
                                            {
                                                val tes = asArr(i)
                                                
                                                renderGroup( "g_%d_%d".format(ddIndex, i), tes.zipWithIndex.map( x => {
                                                    val rank = x._2
                                                    val te = x._1._2
                                                    
                                                    (rank, te.name, te.primaryTopic, te.weight)
                                                } ), "selectable" )
                                            }
                                            else <span/>
                                            
                                            i += 1
                                            <td>{cell}</td>
                                        }
                                    }
                                    </tr>
                                }
                            }
                            </table>
                            
                            <table>
                            {
                                val dists = for ( dd2 <- dds if dd1.id != dd2.id ) yield
                                {
                                    val tv2 = dd2.topicVector
                                    if ( true )
                                    {
                                        val minSize = (tv1.size min tv2.size)
                                        val tv1cp = tv1.prunedToTop( minSize )
                                        val tv2cp = tv2.prunedToTop( minSize )
                                        
                                        tv1cp.pruneSolitaryContexts( tv2cp, true )
                                        tv2cp.pruneSolitaryContexts( tv1cp, false )
                                        
                                        assert( tv1cp.size <= minSize )
                                        assert( tv2cp.size <= minSize )
                                        (dd2, tv1cp.distance(tv2cp))
                                    }
                                    else
                                    {
                                        (dd2, tv1.distance(tv2))
                                    }
                                }
                                
                                for ( (dd2, (dist, fullWhy)) <- dists.sortWith( _._2._1 > _._2._1 ) ) yield
                                {
                                    val tv2 = dd2.topicVector
                                    if ( dist > 0.01 )
                                    {
                                        <tr>
                                            <td><h3>{names(dd2.id-1)}:&nbsp;{"%.2f".format(dist)}</h3></td>

                                            {
                                                val why = fullWhy.slice(0, 200)
                                                
                                                var topicMap = new AutoMap[Int, WrappedTopicId]( id => new WrappedTopicId(id) )
                                                val topicClustering = new AgglomClustering[WrappedTopicId]()
                                                val chosenTopics = why.foldLeft(HashMap[Int, TopicElement]())( (m, ti) => m.updated(ti._2, ti._3) )
                                                val relevantEdges = dd1.topicLinks.filter( x => chosenTopics.contains(x._1) && chosenTopics.contains(x._2) )
                                                relevantEdges.foreach( e => topicClustering.update( topicMap(e._1), topicMap(e._2), e._3 ) )

                                                val groupMembership = topicClustering.runGrouped( 0.7, x => false, () => Unit, (x, y) => true, x => "", false )
                                                
                                                var rankBuilder = new AutoMap[Int, List[(Int, String, Boolean, Double)]]( x => Nil )
                                                for ( ((weight, topicId, te), index) <- why.zipWithIndex )
                                                {
                                                    val wid = topicMap(topicId)
                                                    val groupId = if ( groupMembership.contains(wid) ) groupMembership(wid) else -1
                                                    val rank = index + 1
                                                    rankBuilder.set( groupId, (rank, te.name, te.primaryTopic, weight) :: rankBuilder(groupId) )
                                                }
                                                
                                                val aveRankSorted = rankBuilder.toList.map( x =>
                                                {
                                                    val groupId = x._1
                                                    var totalRank = 0
                                                    var count = 0
                                                    for ( (rank, name, primaryTopic, weight) <- x._2 )
                                                    {
                                                        totalRank += rank
                                                        count += 1
                                                    }
                                                    
                                                    (totalRank.toDouble / count.toDouble, groupId, x._2)
                                                } ).sortWith( _._1 < _._1 )
                                                
                                                for ( ((aveRank, groupId, groupMembership), i) <- aveRankSorted.slice(0, 10).zipWithIndex ) yield
                                                {
                                                    <td>{renderGroup("m_%d_%d".format(ddIndex, i).format(i), groupMembership, "")}</td>
                                                }
                                            }
                                        </tr> 
                                    }
                                }
                            }
                            </table>
                        </div>
                    }
                }
                <script>enableSelectable()</script>
                </body>
            </html>
            
        XML.save( "distances.html", res, "utf8" )
    }
}



