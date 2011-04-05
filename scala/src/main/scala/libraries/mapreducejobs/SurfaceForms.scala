package org.seacourt.mapreducejobs


import org.apache.hadoop.io.Text

import scala.collection.JavaConversions._

import org.dbpedia.extraction.wikiparser._

import scala.collection.mutable.HashSet

import org.seacourt.mapreduce._
import org.seacourt.utility._

// TODO:
// Sanity check text to remove junk

object SurfaceFormsGleaner extends MapReduceJob[Text, Text, Text, Text, Text, TextArrayWritable]
{
    class JobMapper extends MapperType
    {
        override def map( topicTitle : Text, topicText : Text, output : MapperType#Context )
        {
            val parsed = Utils.wikiParse( topicTitle.toString, topicText.toString )
            
            Utils.traverseWikiTree( parsed, element =>
            {
                element match
                {
                    case InternalLinkNode( destination, children, line ) =>
                    {
                        if ( children.length != 0 &&
                            (destination.namespace.toString() == "Main" ||
                             destination.namespace.toString() == "Category") )
                        {
                            children(0) match
                            {
                                case TextNode( surfaceForm, line ) =>
                                {
                                    output.write( new Text(Utils.normalize( surfaceForm )), new Text(destination.namespace.toString +  ":" + destination.decoded.toString) )
                                }
                                case _ =>
                            }
                        }
                    }
                    case _ =>
                }
            } )
        }
    }
     
    class JobReducer extends ReducerType
    {
        override def reduce( word : Text, values: java.lang.Iterable[Text], output : ReducerType#Context )
        {
            val seen = new HashSet[Text]
            val outValues = new TextArrayWritable()
            var count = 0
            for ( value <- values )
            {
                if ( !seen.contains(value) )
                {
                    outValues.append( value.toString() )
                    count += 1
                }
                if ( count > 1000 ) return Unit
            }
            output.write( word, outValues )
        }
    }
    
    override def register( job : Job )
    {
        job.setMapperClass(classOf[JobMapper])
        job.setReducerClass(classOf[JobReducer])
    }
}


