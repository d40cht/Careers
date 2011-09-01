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
        
        val names = ArrayBuffer( "Alex", "Gav", "Steve", "Sem", "George", "George", "Alistair", "Chris", "Sarah", "Rob", "Croxford", "EJ", "Nils", "Zen", "Susanna", "Karel", "Tjark", "Jasbir", "Jasbir", "Pippo", "Olly", "Margot", "Sarah T", "Charlene Watson", "Nick Hill", "Jojo", "Matthew Schumaker", "Some quant dude off the web", "A second quant dude off the web", "Pete Williams web dev", "Jackie Lee web dev", "Katie McGregor", "David Green (env consultant)" )
        
        val range = 1 until 6
        val dds = range.map( i => sbinary.Operations.fromFile[DocumentDigest]( new java.io.File( "./documentDigest%d.bin".format(i) ) ) )
        
        val styleText = """
            .selectable
            {
                width:200px;
                position: relative;
                padding: 2px;
            }
            
            .selected
            {
                background-color: #eef;
            }
          
            div#controls
            {
                border: 1px solid; 
                padding: 3px;
                background-color: #fff;
                min-width: 0px;
                vertical-align: middle;
                width: 60px;
                text-align: center;
                position: absolute;
                top: -15;
                right: 0;
                opacity: 0.9;
            }
            div#controls img { padding: 2px; }
        """
        
        val jsText = """
            $(".selectable").hover( function()
            {
                $(".selected").removeClass("selected")
                $("#controls").remove()
                $(this).addClass("selected")
                $(this).append( '<div id="controls"><img id="delete" src="./images/delete.png"/><img id="increment" src="./images/plus.png"/><img id="decrement" src="./images/minus.png"/></div>' )
                var res = $(this)
                $("#delete").click( function() { /*alert( "delete: " + res.text() );*/ res.css({'text-decoration': 'line-through'}) } )
            } )
        """
        
        def renderGroup( groupMembership : List[(Int, String, Boolean, Double)], klass : String ) =
        {
            <div style="padding:6px" class={klass}>
            {
                var colours = List( "#a0a040", "#40a0a0" )
                var greys = List( "#606060", "#a0a0a0" )
                for ( (rank, name, primaryTopic, weight) <- groupMembership.sortWith( _._4 > _._4 ) ) yield
                {
                    var styles = "font-size: %d".format( 17 + log(weight).toInt ) :: "text-decoration: none" :: Nil
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
                    
                    <span class={klass}><a href={wikiLink(name)} style={styles.mkString(";")}>{ name.replace( "Main:", "" ) }</a></span><span> </span>
                    //<span class="selectable">{ name.replace( "Main:", "" ) }</span> <span></span>
                }
            }
            </div>
        }
        
        val res =
            <html>
                <head>
                <style type="text/css">{styleText}</style>
                <script type="text/javascript" src="https://ajax.googleapis.com/ajax/libs/jquery/1.6.2/jquery.min.js"></script>
                </head>
                <body style="font-family:sans-serif">
                {
                    for ( dd1 <- dds ) yield
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
                                                
                                                renderGroup( tes.zipWithIndex.map( x => {
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
                                /*for ( tes <- groupsByRank ) yield
                                {
                                    <li>
                                    {
                                        tes.map( te => "%s (%2.2e)".format(te.name, te.weight) ).mkString(", ")
                                    }
                                    </li>
                                }*/
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
                                                
                                                for ( (aveRank, groupId, groupMembership) <- aveRankSorted.slice(0, 10) ) yield
                                                {
                                                    <td>{renderGroup(groupMembership, "")}</td>
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
                <script>{xml.Unparsed(jsText)}</script>
                </body>
            </html>
            
        XML.save( "distances.html", res, "utf8" )
    }
}



