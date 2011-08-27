package org.seacourt.tests

import org.scalatest.FunSuite
import org.scalatest.Tag

import scala.collection.immutable.{HashMap}
import scala.collection.mutable.{ArrayBuffer}

import scala.math.{log, pow}
import scala.xml._
import scala.io.Source._
import org.seacourt.utility._
import org.seacourt.disambiguator.{WrappedTopicId, AgglomClustering, TopicVector, TopicElement, DocumentDigest}



import org.seacourt.serialization.SerializationProtocol._

class DistanceMetricTest extends FunSuite
{
    def makeDocumentDigest( fileName : String, documentId : Int ) =
    {
        val data = XML.loadFile( fileName )
        
        // Pull out the name map at the end of the resolution file.
        val nameMap = (data \\ "topic").foldLeft( HashMap[Int, (String, Boolean)]() )( (c, el) => c.updated( (el \\ "id").text.toInt, ((el \\ "name").text, (el \\ "primaryTopic").text.toBoolean ) ) )
        
        /*// Pull out the topic groupings: map from topicId to group id
        var groupMembership = HashMap[Int, Int]()
        for ( group <- data \\ "group" )
        {
            val groupId = (group \ "@id").text.toInt
            for ( topic <- group \\ "topicId" )
            {
                val topicId = topic.text.toInt
                groupMembership = groupMembership.updated( topicId, groupId )
            }
        }*/
        
        val idMap = (data \\ "site").foldLeft(HashMap[Int, Int]())( (m, s) => m.updated((s \ "id").text.toInt, (s \ "topicId").text.toInt) )
        
        println( "Id map size: " + idMap.size )
        
        var topicMap = new AutoMap[Int, WrappedTopicId]( id => new WrappedTopicId(id) )
        val topicClustering = new AgglomClustering[WrappedTopicId]()
        
        var linkWeights = new AutoMap[(Int, Int), Double]( x => 0.0 )
        val topicWeightings = new AutoMap[Int, Double]( x => 0.0 )
        for ( site <- data \ "sites" \ "site" )
        {
            val id = (site \ "id").text.toInt
            val topicId = (site \ "topicId").text.toInt
            //val weight = (site \ "weight").text.toDouble
            
            for ( peer <- site \\ "peer" )
            {
                val peerId = (peer \\ "id").text.toInt
                val weightings = (peer \\ "component").foldLeft( List[(Int, Double)]() )( (l, el) => ( (el \\ "contextTopicId").text.toInt, (el \\ "weight").text.toDouble) :: l )
                
                var totalWeight = 0.0
                for ( (contextId, weight) <- weightings )
                {
                    val key = if (topicId < contextId) (topicId, contextId) else (contextId, topicId)
                    linkWeights.set( key, linkWeights(key) + weight )
                    
                    //topicClustering.update( topicMap(topicId), topicMap(contextId), weight )
                    topicWeightings.set( contextId, topicWeightings(contextId) + weight )
                    topicWeightings.set( topicId, topicWeightings(topicId) + weight )
                    totalWeight += 0.0
                }
                topicClustering.update( topicMap(idMap(id)), topicMap(idMap(peerId)), totalWeight )
            }
        }
        
        val groupings = topicClustering.run( 0.2, x => false, () => Unit, (x, y) => true, x => nameMap(x.id)._1, false )
        var groupMembership = HashMap[Int, Int]()
        groupings.zipWithIndex.foreach( x => {
            val members = x._1
            val gid = x._2
            
            members.foreach( wid =>
            {
                groupMembership = groupMembership.updated( wid.id, gid )
            } )
        } )

        
        val topicVector = new TopicVector()
        for ( (id, weight) <- topicWeightings )
        {
            val (name, primaryTopic) = nameMap(id)
            
            if ( !name.startsWith("Category:") )
            {
                if ( groupMembership.contains(id) )
                {
                    val groupId = groupMembership(id)
                    topicVector.addTopic( id, weight, name, groupId, primaryTopic )
                }
                else
                {
                    println( "Missing group for id: " + id )
                }
            }                
        }
        
        new DocumentDigest( documentId, topicVector, linkWeights.map( x => (x._1._1, x._1._2, x._2) ).toList )
    }

    test( "DistanceMetricTest", TestTags.largeDataTests )
    {
        if ( true )
        {
            def wikiLink( topicName : String ) =
            {
                val wikiBase = "http://en.wikipedia.org/wiki/"
                wikiBase + (if (topicName.startsWith("Main:")) topicName.drop(5) else topicName)
            }
            
            val names = ArrayBuffer( "Alex", "Gav", "Steve", "Sem", "George", "George", "Alistair", "Chris", "Sarah", "Rob", "Croxford", "EJ", "Nils", "Zen", "Susanna", "Karel", "Tjark", "Jasbir", "Jasbir", "Pippo", "Olly", "Margot", "Sarah T", "Charlene Watson", "Nick Hill", "Jojo", "Matthew Schumaker", "Some quant dude off the web", "A second quant dude off the web", "Pete Williams web dev", "Jackie Lee web dev", "Katie McGregor", "David Green (env consultant)" )
            
            //val range = 1 until 34
            val range = 1 until 7
            val dds = range.map( i => makeDocumentDigest( "./ambiguityresolution%d.xml".format(i), i ) )
            dds.zipWithIndex.foreach( el => sbinary.Operations.toFile( el._1 )( new java.io.File( "./dd%d.bin".format(el._2) ) ) )
            
            //val dds = range.map( i => sbinary.Operations.fromFile[DocumentDigest]( new java.io.File( "./dd%d.bin".format(i) ) ) )

            val res =
                <html>
                    <head></head>
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

                                (sum/count.toDouble, tes.map( _._2 ) )
                            } ).toList.sortWith( _._1 < _._1 ).map( _._2 )
                            
                            
                            <div style="text-align:justify">
                                <h1>{names(dd1.id-1)}</h1>
                                
                                <ul>
                                {
                                    for ( tes <- groupsByRank ) yield
                                    {
                                        <li>
                                        {
                                            tes.map( te => "%s (%2.2e)".format(te.name, te.weight) ).mkString(", ")
                                        }
                                        </li>
                                    }
                                }
                                </ul>
                                
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
                                    
                                    for ( (dd2, (dist, why)) <- dists.sortWith( _._2._1 > _._2._1 ) ) yield
                                    {
                                        val tv2 = dd2.topicVector
                                        if ( dist > 0.01 )
                                        {
                                            <tr>
                                                <td><h3>{names(dd2.id-1)}:&nbsp;{"%.2f".format(dist)}</h3></td>

                                                {
                                                    var rankBuilder = new AutoMap[Int, List[(Int, String, Boolean, Double)]]( x => Nil )
                                                    for ( ((weight, name, primaryTopic, groupId), index) <- why.slice(0, 200).zipWithIndex )
                                                    {
                                                        val rank = index + 1
                                                        rankBuilder.set( groupId, (rank, name, primaryTopic, weight) :: rankBuilder(groupId) )
                                                        //println( "  %s %2.6f".format( name, weight ) )
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
                                                        <td style="padding:6px">
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
                                                                
                                                                <a href={wikiLink(name)} style={styles.mkString(";")}>{ name.replace( "Main:", "" ) }</a><span> </span>
                                                            }
                                                        }
                                                        </td>
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
                    </body>
                </html>
                
            XML.save( "distances.html", res, "utf8" )
        }
    }
}



