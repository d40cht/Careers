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

import Utils._
//import MapReduceJob._

class WordInTopicCounter extends MapReduceJob[Text, Text, Text, IntWritable, Text, IntWritable]
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
                    case TextNode( text, line ) =>
                    {
                        val words = luceneTextTokenizer( text )
                        words.foreach( x => uniqueWords = uniqueWords + x.toLowerCase )
                    }
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

class WordInTopicMembershipMapper extends Mapper[Text, Text, Text, IntWritable]
{
    val markupParser = WikiParser()
        
    def extractRawText( parentTopic : Text, elements : Seq[Node], context : Mapper[Text, Text, Text, IntWritable]#Context ) : String =
    {
    	var fullText = new StringBuffer()
    	
        for ( child <- elements )
        {
            child match
            {
                case SectionNode(name, level, children, line ) =>
                {
                    fullText.append( extractRawText( parentTopic, children, context ) )
                }
                case TableNode( caption, children, line ) =>
                {
                    fullText.append( extractRawText( parentTopic, children, context ) )
                }

                case TextNode( text, line ) =>
                {
                    // Nothing for now
                    fullText.append( text )
                }

                case _ =>
            }
        }
        
        fullText.toString
    }

                
    override def map( key : Text, value : Text, context : Mapper[Text, Text, Text, IntWritable]#Context ) =
    {
        val topicTitle = key
        val topicText = value
        
        var uniqueWords = TreeSet[String]()
        try
        {
            val page = new WikiPage( WikiTitle.parse( topicTitle.toString ), 0, 0, topicText.toString )
            val parsed = markupParser( page )
            val words = luceneTextTokenizer( extractRawText( key, parsed.children, context ) )
            
            words.foreach( x => uniqueWords = uniqueWords + x.toLowerCase )
        }
        catch
        {
            case e : WikiParserException =>
            {
                println( "Bad parse: " + topicTitle )
            }
        }
        
        for ( word <- uniqueWords ) context.write( new Text(word), new IntWritable(1) )
    }
}

class WordInTopicMembershipReducer extends Reducer[Text, IntWritable, Text, IntWritable]
{
    override def reduce(key : Text, values : java.lang.Iterable[IntWritable], context : Reducer[Text, IntWritable, Text, IntWritable]#Context)
    {
        var count = 0
        for ( value <- values )
        {
            count += 1
        }
        context.write( key, new IntWritable(count) )
    }
}


object WordInTopicMembership
{
    def main(args:Array[String]) : Unit =
    {
        val conf = new Configuration()
        val otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs
        
        if ( otherArgs.length != 3 )
        {
            println( "Usage: word in doc membership <in> <out> <num reduces>")
            for ( arg <- otherArgs )
            {
                println( "  " + arg )
            }
            return 2
        }
        
        run( conf, args(0), args(1), args(2).toInt )
    }
    
    def run( conf : Configuration, inputFileName : String, outputFilePath : String, numReduces : Int )
    {
        val job = new Job(conf, "Surface forms")
        
        job.setJarByClass(classOf[WordInTopicMembershipMapper])
        job.setMapperClass(classOf[WordInTopicMembershipMapper])
        job.setReducerClass(classOf[WordInTopicMembershipReducer])
        job.setNumReduceTasks( numReduces )
        
        job.setInputFormatClass(classOf[SequenceFileInputFormat[Text, Text] ])
        job.setMapOutputKeyClass(classOf[Text])
        job.setMapOutputValueClass(classOf[IntWritable])
        
        job.setOutputKeyClass(classOf[Text])
        job.setOutputValueClass(classOf[IntWritable])
        
        
                
        FileInputFormat.addInputPath(job, new Path(inputFileName))
        FileOutputFormat.setOutputPath(job, new Path(outputFilePath))
        
        if ( job.waitForCompletion(true) ) 0 else 1
    }
}


