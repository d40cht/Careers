package org.seacourt.mapreducejobs

import org.apache.hadoop.io.Text
import scala.collection.JavaConversions._

import org.dbpedia.extraction.wikiparser._

import scala.util.matching.Regex


import org.seacourt.mapreduce._
import org.seacourt.utility._

// TODO:
// Sanity check text to remove junk

object CategoriesAndContexts extends MapReduceJob[Text, Text, Text, Text, Text, TextArrayWritable]
{
    class JobMapper extends MapperType
    {
        override def map( topicTitle : Text, topicText : Text, output : MapperType#Context )
        {
            try
            {
                val parsed = Utils.wikiParse( topicTitle.toString, topicText.toString )
             
                // Don't bother with list-of and table-of links atm because it's hard to work
                // out which links in the page are part of the list and which are context
                val linkRegex = new Regex( "[=]+[^=]+[=]+" )
                
                Utils.foldlWikiTree( parsed, true, (element : Node, inFirstSection : Boolean) =>
                {
                    var newInFirstSection = inFirstSection
                    
                    element match
                    {
                        case InternalLinkNode( destination, children, line ) =>
                        {
                            // Contexts are: any link to a category or any link in the first section
                            // (also could be any links to topics that are reciprocated)
                            val namespace = destination.namespace.toString
                            if ( namespace == "Category" || (namespace == "Main" && inFirstSection) )
                            {
                                output.write( topicTitle, new Text(namespace + ":" + destination.decoded.toString) )
                            }
                        }
                        case TextNode( text, line ) =>
                        {
                            linkRegex.findFirstIn(text) match
                            {
                                case None =>
                                case _ => newInFirstSection = false
                            }
                        }
                        case _ =>
                    }
                    
                    newInFirstSection
                } )
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
            val outValues = new TextArrayWritable()
            for ( value <- values ) outValues.append( value.toString() )
            output.write( word, outValues )
        }
    }
    
    override def register( job : Job )
    {
        job.setMapperClass(classOf[JobMapper])
        job.setReducerClass(classOf[JobReducer])
    }
}



