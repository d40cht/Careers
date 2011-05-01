package org.seacourt.mapreducejobs

import org.apache.hadoop.io.Text
import scala.collection.JavaConversions._
import scala.collection.immutable.TreeSet

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
        def mapWork( topicTitle : String, topicText : String, output : (String, String) => Unit )
        {
            if ( topicTitle.toString == "28 Days Later: The Soundtrack Album" )
            {
                //println( topicText )
                output( topicTitle.toString, topicText.toString )
            }
            
            if ( false )
            {
                try
                {
                    val parsed = Utils.wikiParse( topicTitle, topicText )
                 
                    // Don't bother with list-of and table-of links atm because it's hard to work
                    // out which links in the page are part of the list and which are context
                    val linkRegex = new Regex( "[=]+[^=]+[=]+" )
                    
                    var linkSet = new TreeSet[String]
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
                                    linkSet = linkSet + Utils.normalizeLink( destination )
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
                    
                    for ( linkTo <- linkSet ) output( Utils.normalizeTopicTitle( topicTitle ), linkTo )
                }
                catch
                {
                    case e : WikiParserException =>
                    case _ => 
                }
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







