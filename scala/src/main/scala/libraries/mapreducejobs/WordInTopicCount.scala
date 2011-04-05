package org.seacourt.mapreducejobs


import org.apache.hadoop.io.{Text, IntWritable}

import scala.collection.JavaConversions._

import org.dbpedia.extraction.wikiparser._

import scala.collection.immutable.TreeSet

import org.seacourt.utility._
import org.seacourt.mapreduce._

object WordInTopicCounter extends MapReduceJob[Text, Text, Text, IntWritable, Text, IntWritable]
{
    class JobMapper extends MapperType
    {
        override def map( topicTitle : Text, topicText : Text, output : MapperType#Context )
        {
            val parsed = Utils.wikiParse( topicTitle.toString, topicText.toString )
            
            Utils.foldlWikiTree( parsed, TreeSet[String](), (element : Node, seenSet : TreeSet[String]) =>
            {
                var newSet = seenSet
                element match
                {
                    case TextNode( text, line ) => Utils.luceneTextTokenizer( text ).foreach( x =>
                    {
                        val lc = x.toLowerCase
                        if ( !seenSet.contains(lc) )
                        {
                            output.write( new Text(lc), new IntWritable(1) )
                            newSet = seenSet + lc
                        }
                    } )
                    case _  =>
                }
                
                newSet
            } )
        }
    }
    
    class JobReducer extends ReducerType
    {
        override def reduce( word : Text, values: java.lang.Iterable[IntWritable], output : ReducerType#Context )
        {
            var count = 0
            for ( value <- values )
            {
                count += 1
            }
            output.write( word, new IntWritable(count) )
        }
    }
    
    override def register( job : Job )
    {
        job.setMapperClass(classOf[JobMapper])
        job.setReducerClass(classOf[JobReducer])
    }
}


