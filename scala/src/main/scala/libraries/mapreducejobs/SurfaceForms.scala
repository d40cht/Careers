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
import org.apache.hadoop.io.{Writable}

import scala.collection.mutable.HashSet

import org.json.JSONArray

import org.seacourt.mapreduce._
import org.seacourt.utility._

// TODO:
// Sanity check text to remove junk

object SurfaceFormsGleaner extends MapReduceJob[Text, Text, Text, Text, Text, TextArrayWritable]
{    
    override def mapfn( topicTitle : Text, topicText : Text, outputFn : (Text, Text) => Unit )
    {
        val markupParser = WikiParser()
        val page = new WikiPage( WikiTitle.parse( topicTitle.toString ), 0, 0, topicText.toString )
        val parsed = markupParser( page )
        
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
                                outputFn( new Text(Utils.normalize( surfaceForm )), new Text(destination.namespace.toString +  ":" + destination.decoded.toString) )
                            }
                            case _ =>
                        }
                    }
                }
            }
        } )
    }
    
    override def reducefn( word : Text, values: java.lang.Iterable[Text], outputFn : (Text, TextArrayWritable) => Unit )
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
        outputFn( word, outValues )
    }
}


