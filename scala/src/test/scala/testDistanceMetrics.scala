import org.scalatest.FunSuite
import scala.collection.immutable.{HashMap}

import scala.xml._
import scala.io.Source._
import org.seacourt.utility._
import org.seacourt.disambiguator.{WrappedTopicId, AgglomClustering}

import wordcram._
import processing.core.{PApplet, PGraphics, PConstants, PGraphicsJava2D}



class MyGraphics2D() extends PGraphicsJava2D
{
    override def displayable() = false
}

class Blah() extends PApplet
{
    var wc : WordCram = null
    //val pg = createGraphics( 400, 300, "MyGraphics2D" )
    
    override def setup()
    {
        // http://wordcram.org/2010/09/09/get-acquainted-with-wordcram/
        background(255)
        //colorMode(HSB)
        wc = new wordcram.WordCram( this )
        
        //wc.withColors( color(0, 250, 200), color(30), color(170, 230, 200) )
        //wc.withColors( 0x444493, 0xD0D0F0, 0x90F090 )
        //wc.sizedByWeight( 10, 90 )
        //wc.withAngler( Anglers.mostlyHoriz() )
        //wc.withPlacer( Placers.horizLine() )
        //wc.withColorer( Colorers.pickFrom() )
            //Fonters.alwaysUse(createFont("LiberationSerif-Regular.ttf", 1)),

        wc.fromTextFile( "./src/test/scala/data/georgecv.txt" )
        
        size( 400, 300, "MyGraphics2D" )
        println( "Here1" )
        noLoop()
    }
    
    override def draw()
    {
        println( "Here2" )
        wc.drawAll()
        println( "Here3" )
        save( "wordcram.png" )
        Thread.sleep( 15000 )
        //exit()
    }
}


class TopicVector( val id : Int )
{
    type TopicId = Int
    type TopicWeight = Double
    
    class TopicElement( val weight : TopicWeight, val name : String, val groupId : Int, val primaryTopic : Boolean )
    {
    }
    
    private var topics = HashMap[TopicId, TopicElement]()
    
    def addTopic( id : TopicId, weight : TopicWeight, name : String, groupId : Int, primaryTopic : Boolean )
    {
        topics = topics.updated( id, new TopicElement( weight, name, groupId, primaryTopic ) )
    }
    
    def distance( other : TopicVector ) =
    {
        var AB = 0.0
        var AA = 0.0
        var BB = 0.0
        
        // Weight, name, groupId
        var weightedMatches = List[(Double, String, Int)]()
        
        for ( (id, te) <- topics )
        {
            if ( other.topics.contains(id) )
            {
                val otherte = other.topics(id)
                
                val combinedWeight = te.weight * otherte.weight
                val priorityWeight = combinedWeight / math.sqrt( (te.weight*te.weight) + (otherte.weight*otherte.weight) )
                
                
                weightedMatches = (priorityWeight, (if (te.primaryTopic) "* " else "  ") + te.name, te.groupId) :: weightedMatches
                
                AB += combinedWeight
            }
            AA += (te.weight*te.weight)
        }
        
        for ( (id, te) <- other.topics )
        {
            BB += (te.weight*te.weight)
        }
        
        val cosineDist = AB / (math.sqrt(AA) * math.sqrt(BB))
        
        ( cosineDist, weightedMatches.sortWith( _._1 > _._1 ) )
    }
}

class DistanceMetricTest extends FunSuite
{
    def makeTopicVector( fileName : String, documentId : Int ) =
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
        
        var topicMap = new AutoMap[Int, WrappedTopicId]( id => new WrappedTopicId(id) )
        val topicClustering = new AgglomClustering[WrappedTopicId]()
        
        val topicWeightings = new AutoMap[Int, Double]( x => 0.0 )
        for ( site <- data \ "sites" \ "site" )
        {
            val id = (site \ "id").text.toInt
            val topicId = (site \ "topicId").text.toInt
            val weight = (site \ "weight").text.toDouble
            
            for ( peer <- site \\ "peer" )
            {
                val peerId = (peer \\ "id").text.toInt
                val weightings = (peer \\ "component").foldLeft( List[(Int, Double)]() )( (l, el) => ( (el \\ "contextTopicId").text.toInt, (el \\ "weight").text.toDouble) :: l )
                
                for ( (id, weight) <- weightings )
                {
                    topicClustering.update( topicMap(topicId), topicMap(id), weight )
                    topicWeightings.set( id, topicWeightings(id) + weight )
                }
            }
        }
        
        val groupings = topicClustering.run( 0.3, x => false, () => Unit, (x, y) => true, x => nameMap(x.id)._1, false )
        var groupMembership = HashMap[Int, Int]()
        groupings.zipWithIndex.foreach( x => {
            val members = x._1
            val gid = x._2
            
            members.foreach( wid =>
            {
                groupMembership = groupMembership.updated( wid.id, gid )
            } )
        } )

        
        val topicVector = new TopicVector(documentId)
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
        
        topicVector
    }
    
    
    
    test( "WordCram" )
    {   
        val b = new Blah()
        b.init()
        /*println( "Here4" )
        b.setup()
        b.redraw()
        println( "Here5" )*/
        PApplet.main( List("--present", "Blah").toArray )
    }

    test( "DistanceMetricTest" )
    {
        if ( false )
        {
            var tvs = List[TopicVector]()
            for ( i <- 1 until 23 )
            {
                val tv = makeTopicVector( "/home/alexw/AW/optimal/scala/ambiguityresolution%d.xml".format(i), i )
                tvs = tv :: tvs
            }
            
            for ( tv1 <- tvs; tv2 <- tvs if ( tv1.id < tv2.id && tv1.id != 6 && tv2.id != 6 ) )
            {
                val (dist, why) = tv1.distance(tv2)
                
                if ( dist > 0.01 )
                {
                    println( "************* %d to %d: %2.6f ***************".format( tv1.id, tv2.id, dist ) )
                    
                    var rankBuilder = new AutoMap[Int, List[(Int, String, Double)]]( x => Nil )
                    for ( ((weight, name, groupId), index) <- why.slice(0, 100).zipWithIndex )
                    {
                        val rank = index + 1
                        rankBuilder.set( groupId, (rank, name, weight) :: rankBuilder(groupId) )
                        //println( "  %s %2.6f".format( name, weight ) )
                    }
                    
                    val aveRankSorted = rankBuilder.toList.map( x =>
                    {
                        val groupId = x._1
                        var totalRank = 0
                        var count = 0
                        for ( (rank, name, weight) <- x._2 )
                        {
                            totalRank += rank
                            count += 1
                        }
                        
                        (totalRank.toDouble / count.toDouble, groupId, x._2)
                    } ).sortWith( _._1 < _._1 )
                    
                    for ( (aveRank, groupId, groupMembership) <- aveRankSorted.slice(0, 10) )
                    {
                        println( "Group id: %d, ave rank: %.2f".format( groupId, aveRank ) )
                        
                        for ( (rank, name, weight) <- groupMembership.sortWith( _._3 > _._3 ) )
                        {
                            println( "  %s: %d, %2.2e".format( name, rank, weight ) )
                        }
                    }
                }
            }
        }
    }
}



