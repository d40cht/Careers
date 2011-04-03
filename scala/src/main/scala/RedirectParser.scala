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

import scala.collection.immutable.TreeSet
import java.io.{File, BufferedReader, FileReader, StringReader, Reader}
import Utils._

object RedirectParser extends MapReduceJob[Text, Text, Text, Text, Text, Text]
{
    override def mapfn( topicTitle : Text, topicText : Text, outputFn : (Text, Text) => Unit )
    {
        val markupParser = WikiParser()
        val page = new WikiPage( WikiTitle.parse( topicTitle.toString ), 0, 0, topicText.toString )
        val parsed = markupParser( page )
        
        if ( parsed.isRedirect )
        {
            traverseWikiTree( parsed, element =>
            {
                element match
                {
                    case InternalLinkNode(destination, children, line) =>
                    {
                        outputFn( topicTitle, new Text( destination.namespace + ":" + destination.decoded ) )
                    }
                    case _ =>
                }
            } )
        }
    }
    
    override def reducefn( word : Text, values: java.lang.Iterable[Text], outputFn : (Text, Text) => Unit )
    {
        for ( value <- values ) outputFn( word, value )
    }
}

