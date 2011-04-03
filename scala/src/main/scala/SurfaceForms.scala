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

import Utils._

// TODO:
// Sanity check text to remove junk


class SurfaceFormsMapper extends Mapper[Text, Text, Text, Text]
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
            val extractor = new LinkExtractor()
            extractor.run( parsed, (surfaceForm, namespace, Topic, firstSection) => context.write( new Text(surfaceForm), new Text(Topic) ) )
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

class SurfaceFormsReducer extends Reducer[Text, Text, Text, JSONObjectWritable]
{
    override def reduce(key : Text, values : java.lang.Iterable[Text], context : Reducer[Text, Text, Text, JSONObjectWritable]#Context)
    {
        val seen = new HashSet[Text]
        val outValues = new JSONObjectWritable()
        var count = 0
        for ( value <- values )
        {
            if ( !seen.contains(value) )
            {
                //context.write( key, value )
                outValues.put( count.toString(), value.toString() )
                seen += value
                count += 1
            }
            
            if ( count > 1000 )
            {
                return Unit
            }
        }
        context.write( key, outValues )
    }
}


object SurfaceForms
{
    def main(args:Array[String]) : Unit =
    {
        val conf = new Configuration()
        val otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs
        
        if ( otherArgs.length != 3 )
        {
            println( "Usage: surfaceforms <in> <out> <num reduces>")
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
        
        job.setJarByClass(classOf[SurfaceFormsMapper])
        job.setMapperClass(classOf[SurfaceFormsMapper])
        //job.setCombinerClass(classOf[SurfaceFormsReducer])
        job.setReducerClass(classOf[SurfaceFormsReducer])
        job.setNumReduceTasks( numReduces )
        
        job.setInputFormatClass(classOf[SequenceFileInputFormat[Text, Text] ])
        job.setMapOutputKeyClass(classOf[Text])
        job.setMapOutputValueClass(classOf[Text])
        
        job.setOutputKeyClass(classOf[Text])
        job.setOutputValueClass(classOf[JSONObjectWritable])
        
        
                
        FileInputFormat.addInputPath(job, new Path(inputFileName))
        FileOutputFormat.setOutputPath(job, new Path(outputFilePath))
        
        if ( job.waitForCompletion(true) ) 0 else 1
    }
}


