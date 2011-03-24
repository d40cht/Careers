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


/*
val tokenizer = new StandardTokenizer( LUCENE_30, new BufferedReader( new FileReader( testFileName ) ) )
        var run = true
        val wordList = new ArrayBuffer[String]
        while ( run )
        {
            val term = tokenizer.getAttribute(classOf[TermAttribute]).term()
            if ( term != "" )
            {
                wordList.append( term )
            }
            run = tokenizer.incrementToken()
        }
        tokenizer.close()*/


class WordInDocumentMembershipMapper extends Mapper[Text, Text, Text, Text]
{
    val markupParser = WikiParser()
   
    
    def extractRawText( parentTopic : Text, elements : Seq[Node], context : Mapper[Text, Text, Text, IntegerWritable]#Context )
    {
    	var fullText = new StringBuffer()
    	
        for ( child <- elements )
        {
            child match
            {
                case SectionNode(name, level, children, line ) =>
                {
                    extractRawText( parentTopic, children, context )
                }
                case TableNode( caption, children, line ) =>
                {
                    extractRawText( parentTopic, children, context )
                }

                case TextNode( text, line ) =>
                {
                    // Nothing for now
                }

                case _ =>
            }
        }
    }

                
    override def map( key : Text, value : Text, context : Mapper[Text, Text, Text, IntegerWritable]#Context ) =
    {
        val topicTitle = key
        val topicText = value
        
        try
        {
            val page = new WikiPage( WikiTitle.parse( topicTitle.toString ), 0, 0, topicText.toString )
            val parsed = markupParser( page )
            extractWords( key, parsed.children, context )
        }
        catch
        {
            case e : WikiParserException =>
            {
                println( "Bad parse: " + topicTitle )
            }
        }
    }
}

class WordInDocumentMembershipReducer extends Reducer[Text, IntegerWritable, Text, IntegerWritable]
{
    override def reduce(key : Text, values : java.lang.Iterable[IntegerWritable], context : Reducer[Text, IntegerWritable, Text, IntegerWritable]#Context)
    {
        var count = 0
        for ( value <- values )
        {
            count += 1
        }
        context.write( key, count )
    }
}


object WordInDocumentMembership
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
        
        val job = new Job(conf, "Surface forms")
        
        job.setJarByClass(classOf[WordInDocumentMembershipMapper])
        job.setMapperClass(classOf[WordInDocumentMembershipMapper])
        job.setReducerClass(classOf[WordInDocumentMembershipReducer])
        job.setNumReduceTasks( args(2).toInt )
        
        job.setInputFormatClass(classOf[SequenceFileInputFormat[Text, Text] ])
        job.setMapOutputKeyClass(classOf[Text])
        job.setMapOutputValueClass(classOf[IntegerWritable])
        
        job.setOutputKeyClass(classOf[Text])
        job.setOutputValueClass(classOf[IntegerWritable])
        
        
                
        FileInputFormat.addInputPath(job, new Path(args(0)))
        FileOutputFormat.setOutputPath(job, new Path(args(1)))
        
        if ( job.waitForCompletion(true) ) 0 else 1
    }
}


