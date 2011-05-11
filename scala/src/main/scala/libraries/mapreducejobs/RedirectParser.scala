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
        def mapWork( topicTitle : String, topicText : String, output : (String, String) => Unit )
        {
            try
            {
                val parsed = Utils.wikiParse( topicTitle, topicText )
                
                if ( parsed.isRedirect )
                {
                    Utils.traverseWikiTree( parsed, element =>
                    {
                        element match
                        {
                            case InternalLinkNode(destination, children, line) =>
                            {
                                output( Utils.normalizeTopicTitle( topicTitle ), Utils.normalizeLink( destination ) )
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
        
        override def map( topicTitle : Text, topicText : Text, output : MapperType#Context )
        {
            mapWork( topicTitle.toString, topicText.toString, (key, value) => output.write( new Text(key), new Text(value) ) )
        }
    }
    
    class JobReducer extends ReducerType
    {
        override def reduce( word : Text, values: java.lang.Iterable[Text], output : ReducerType#Context )
        {
            for ( value <- values ) output.write( word, value )
        }
    }
    
    override def register( job : Job, config : Configuration )
    {
        job.setMapperClass(classOf[JobMapper])
        job.setReducerClass(classOf[JobReducer])
    }
}

