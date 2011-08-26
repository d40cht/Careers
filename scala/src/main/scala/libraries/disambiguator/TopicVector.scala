package org.seacourt.disambiguator

import scala.collection.immutable.{HashMap}

class TopicElement( val weight : Double, val name : String, val groupId : Int, val primaryTopic : Boolean )
{
}

class TopicVector( val id : Int )
{
    type TopicId = Int
    type TopicWeight = Double
    
    var topics = HashMap[TopicId, TopicElement]()
    var topicLinks = List[(TopicId, TopicId, Double)]()
    
    def addTopic( id : TopicId, weight : TopicWeight, name : String, groupId : Int, primaryTopic : Boolean )
    {
        topics = topics.updated( id, new TopicElement( weight, name, groupId, primaryTopic ) )
    }
    
    def addTopic( id : TopicId, te : TopicElement )
    {
        topics = topics.updated( id, te )
    }
    
    def distance( other : TopicVector ) =
    {
        var AB = 0.0
        var AA = 0.0
        var BB = 0.0
        
        // Weight, name, groupId
        var weightedMatches = List[(Double, String, Boolean, Int)]()
        
        // Choose top N from each
        val topTopics = topics
        val topOtherTopics = other.topics
        for ( (id, te) <- topTopics )
        {
            if ( topOtherTopics.contains(id) )
            {
                val otherte = topOtherTopics(id)
                
                if ( true )//te.primaryTopic || otherte.primaryTopic )
                {
                    val combinedWeight = te.weight * otherte.weight
                    val priorityWeight = combinedWeight / math.sqrt( (te.weight*te.weight) + (otherte.weight*otherte.weight) )
                    
                    
                    weightedMatches = (priorityWeight, te.name, te.primaryTopic, te.groupId) :: weightedMatches
                    
                    AB += combinedWeight
                }
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
