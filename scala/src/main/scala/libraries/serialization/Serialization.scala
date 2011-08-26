package org.seacourt.serialization

import sbinary._
import sbinary.Operations._

import org.seacourt.disambiguator.{TopicElement, TopicVector}

object SerializationProtocol extends sbinary.DefaultProtocol
{
    implicit object TopicVectorElementFormat extends Format[TopicElement]
    {
        def reads(in : Input) = new TopicElement( read[Double](in), read[String](in), read[Int](in), read[Boolean](in) )
        def writes(out : Output, tw : TopicElement)
        {
            write(out, tw.weight)
            write(out, tw.name)
            write(out, tw.groupId)
            write(out, tw.primaryTopic)
        }
    }
    
    implicit object TopicVectorFormat extends Format[TopicVector]
    {
        def reads(in : Input) =
        {
            val id = read[Int](in)
            val tv = new TopicVector( id )
            
            val size = read[Int](in)
            for ( i <- 0 until size )
            {
                val teid = read[Int](in)
                val te = read[TopicElement](in)
                tv.addTopic( teid, te )
            }
            tv.topicLinks = read[List[(Int, Int, Double)]](in)
            tv
        }
        def writes(out : Output, tv : TopicVector)
        {
            write(out, tv.id)
            write(out, tv.topics.size)
            for ( (teid, te) <- tv.topics )
            {
                write(out, teid)
                write(out, te)
            }
            write(out, tv.topicLinks)
        }
    }
}
