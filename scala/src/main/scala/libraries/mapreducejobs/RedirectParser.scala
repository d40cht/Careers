package org.seacourt.mapreducejobs


import org.apache.hadoop.io.Text
import scala.collection.JavaConversions._

import org.dbpedia.extraction.wikiparser._

import org.seacourt.mapreduce._
import org.seacourt.utility._

object RedirectParser extends MapReduceJob[Text, Text, Text, Text, Text, Text]
{
    class JobMapper extends MapperType
    {
        override def map( topicTitle : Text, topicText : Text, output : MapperType#Context )
        {
            try
            {
                val parsed = Utils.wikiParse( topicTitle.toString, topicText.toString )
                
                if ( parsed.isRedirect )
                {
                    Utils.traverseWikiTree( parsed, element =>
                    {
                        element match
                        {
                            case InternalLinkNode(destination, children, line) =>
                            {
                                output.write( topicTitle, new Text( destination.namespace + ":" + destination.decoded ) )
                            }
                            case _ =>
                        }
                    } )
                }
            }
            catch
            {
                case e : WikiParserException =>
                case _ => 
            }
        }
    }
    
    class JobReducer extends ReducerType
    {
        override def reduce( word : Text, values: java.lang.Iterable[Text], output : ReducerType#Context )
        {
            for ( value <- values ) output.write( word, value )
        }
    }
    
    override def register( job : Job )
    {
        job.setMapperClass(classOf[JobMapper])
        job.setReducerClass(classOf[JobReducer])
    }
}

