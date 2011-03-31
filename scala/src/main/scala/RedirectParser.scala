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

class RedirectMapper extends Mapper[Text, Text, Text, Text]
{
    val markupParser = WikiParser()
                
    override def map( key : Text, value : Text, context : Mapper[Text, Text, Text, Text]#Context ) =
    {
        val topicTitle = key
        val topicText = value
        try
        {
            val page = new WikiPage( WikiTitle.parse( topicTitle.toString ), 0, 0, topicText.toString )
            val parsed = markupParser( page )
            
            if ( parsed.isRedirect )
            {
                context.write( new Text(topicTitle), new Text( value ) )
            }
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

class RedirectReducer extends Reducer[Text, Text, Text, Text]
{
    override def reduce(key : Text, values : java.lang.Iterable[Text], context : Reducer[Text, Text, Text, Text]#Context)
    {
        var count = 0
        for ( value <- values )
        {
            context.write( key, value )
            count += 1
            //require( count <= 1 )
        }
    }
}


object RedirectParser
{
    def main(args:Array[String]) : Unit =
    {
        val conf = new Configuration()
        val otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs
        
        if ( otherArgs.length != 3 )
        {
            println( "Usage: redirect parser <in> <out> <num reduces>")
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
        val job = new Job(conf, "Redirect parser")
        
        job.setJarByClass(classOf[RedirectMapper])
        job.setMapperClass(classOf[RedirectMapper])
        job.setReducerClass(classOf[RedirectReducer])
        job.setNumReduceTasks( numReduces )
        
        job.setInputFormatClass(classOf[SequenceFileInputFormat[Text, Text] ])
        job.setMapOutputKeyClass(classOf[Text])
        job.setMapOutputValueClass(classOf[Text])
        
        job.setOutputKeyClass(classOf[Text])
        job.setOutputValueClass(classOf[Text])
        
        
                
        FileInputFormat.addInputPath(job, new Path(inputFileName))
        FileOutputFormat.setOutputPath(job, new Path(outputFilePath))
        
        if ( job.waitForCompletion(true) ) 0 else 1
    }
}


