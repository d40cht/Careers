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

import org.apache.lucene.util.Version.LUCENE_30
import org.apache.lucene.analysis.Token
import org.apache.lucene.analysis.tokenattributes.TermAttribute
import org.apache.lucene.analysis.standard.StandardTokenizer

import scala.collection.immutable.TreeSet
import java.io.{File, BufferedReader, FileReader, StringReader, Reader}

import org.seacourt.utility.Utils._
import org.seacourt.mapreduce._

object WordInTopicCounter extends MapReduceJob[Text, Text, Text, IntWritable, Text, IntWritable]
{    
    override def mapfn( topicTitle : Text, topicText : Text, outputFn : (Text, IntWritable) => Unit )
    {
        class WordCounter
        {
            var uniqueWords = TreeSet[String]()
            
            def apply( element : Node )
            {
                element match
                {
                    case TextNode( text, line ) => luceneTextTokenizer( text ).foreach( x => uniqueWords = uniqueWords + x.toLowerCase )
                    case _  =>
                }
            }
        }
        
        val markupParser = WikiParser()
        val page = new WikiPage( WikiTitle.parse( topicTitle.toString ), 0, 0, topicText.toString )
        val parsed = markupParser( page )
        val wc = new WordCounter()
        
        traverseWikiTree( parsed, wc.apply )
        for ( word <- wc.uniqueWords ) outputFn( new Text(word), new IntWritable(1) )
    }
    
    override def reducefn( word : Text, values: java.lang.Iterable[IntWritable], outputFn : (Text, IntWritable) => Unit )
    {
        var count = 0
        for ( value <- values )
        {
            count += 1
        }
        outputFn( word, new IntWritable(count) )
    }
}


