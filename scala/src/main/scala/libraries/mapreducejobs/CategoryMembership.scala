package org.seacourt.mapreducejobs

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.hadoop.io.IntWritable
import org.apache.hadoop.io.Text
import org.apache.hadoop.mapreduce.Job
import org.apache.hadoop.mapreduce.Mapper
import org.apache.hadoop.mapreduce.Reducer
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat
import org.apache.hadoop.util.GenericOptionsParser
import scala.collection.JavaConversions._

import org.dbpedia.extraction.wikiparser._
import org.dbpedia.extraction.sources.WikiPage
import org.dbpedia.extraction.wikiparser.{Node}

import scala.collection.mutable.HashSet

import org.json.JSONArray
import edu.umd.cloud9.io.JSONObjectWritable

import scala.util.matching.Regex


import org.seacourt.mapreduce._
import org.seacourt.utility._

// TODO:
// Sanity check text to remove junk

object CategoriesAndContexts extends MapReduceJob[Text, Text, Text, Text, Text, TextArrayWritable]
{
    override def mapfn( topicTitle : Text, topicText : Text, outputFn : (Text, Text) => Unit )
    {
        val markupParser = WikiParser()
        val page = new WikiPage( WikiTitle.parse( topicTitle.toString ), 0, 0, topicText.toString )
        val parsed = markupParser( page )
     
        // Don't bother with list-of and table-of links atm because it's hard to work
        // out which links in the page are part of the list and which are context

        class LinkParser( registerContext : Text => Unit )
        {
            var inFirstSection = true
            val linkRegex = new Regex( "[=]+[^=]+[=]+" )
            
            def apply( element : Node )
            {
                element match
                {
                    case InternalLinkNode( destination, children, line ) =>
                    {
                        // Contexts are: any link to a category or any link in the first section
                        // (also could be any links to topics that are reciprocated)
                        val namespace = destination.namespace.toString
                        if ( namespace == "Category" || (namespace == "Main" && inFirstSection) )
                        {
                            registerContext( new Text(namespace + ":" + destination.decoded.toString) )
                        }
                    }
                    case TextNode( text, line ) =>
                    {
                        linkRegex.findFirstIn(text) match
                        {
                            case None =>
                            case _ => inFirstSection = false
                        }
                    }
                }
            }
        }
        
        val lp = new LinkParser( linkTarget => outputFn( topicTitle, linkTarget ) )
        Utils.traverseWikiTree( parsed, lp.apply )
    }
    
    override def reducefn( word : Text, values: java.lang.Iterable[Text], outputFn : (Text, TextArrayWritable) => Unit )
    {
        val outValues = new TextArrayWritable()
        for ( value <- values )
        {
            outValues.append( value.toString() )
        }
        outputFn( word, outValues )
    }
}



