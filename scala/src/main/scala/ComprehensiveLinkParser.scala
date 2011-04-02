import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.hadoop.io.{Writable,IntWritable}
import org.apache.hadoop.io.Text
import org.apache.hadoop.mapreduce.Job
import org.apache.hadoop.mapreduce.Mapper
import org.apache.hadoop.mapreduce.Reducer
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat
import org.apache.hadoop.mapreduce.lib.output.{FileOutputFormat, SequenceFileOutputFormat}
import org.apache.hadoop.util.GenericOptionsParser
import scala.collection.JavaConversions._

import org.dbpedia.extraction.wikiparser._
import org.dbpedia.extraction.sources.WikiPage
import org.dbpedia.extraction.wikiparser.{Node}

import scala.collection.immutable.TreeSet
import java.io.{File, BufferedReader, FileReader, StringReader, Reader}
import java.io.{DataInput, DataOutput}

import Utils._

class LinkData( var surfaceForm : String, var namespace : String, var destination : String, var firstSection : Boolean ) extends Writable
{
    def this() = this(null, null, null, false)
    
    override def write( out : DataOutput )
    {
        out.writeUTF( surfaceForm )
        out.writeUTF( namespace )
        out.writeUTF( destination )
        out.writeBoolean( firstSection )
    }
    
    override def readFields( in : DataInput )
    {
        surfaceForm = in.readUTF()
        namespace = in.readUTF()
        destination = in.readUTF()
        firstSection = in.readBoolean()
    }
}

object LinkData
{
    def read( in : DataInput ) =
    {
        val w = new LinkData()
        w.readFields(in)
        w
    }
}


class ComprehensiveLinkMapper extends Mapper[Text, Text, Text, LinkData]
{
    val markupParser = WikiParser()
                
    override def map( key : Text, value : Text, context : Mapper[Text, Text, Text, LinkData]#Context ) =
    {
        val topicTitle = key
        val topicText = value
        try
        {
            val page = new WikiPage( WikiTitle.parse( topicTitle.toString ), 0, 0, topicText.toString )
            val parsed = markupParser( page )
            val extractor = new LinkExtractor()
            extractor.extractLinks( parsed, (surfaceForm, namespace, Topic, firstSection) =>
            {
                context.write( new Text(topicTitle), new LinkData( surfaceForm, namespace, Topic, firstSection ) )
            } )
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

class ComprehensiveLinkReducer extends Reducer[Text, LinkData, Text, LinkData]
{
    override def reduce(key : Text, values : java.lang.Iterable[LinkData], context : Reducer[Text, LinkData, Text, LinkData]#Context)
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


object ComprehensiveLinkParser
{
    def main(args:Array[String]) : Unit =
    {
        val conf = new Configuration()
        val otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs
        
        if ( otherArgs.length != 3 )
        {
            println( "Usage: link parser <in> <out> <num reduces>")
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
        val job = new Job(conf, "ComprehensiveLink parser")
        
        job.setJarByClass(classOf[ComprehensiveLinkMapper])
        job.setMapperClass(classOf[ComprehensiveLinkMapper])
        job.setReducerClass(classOf[ComprehensiveLinkReducer])
        job.setNumReduceTasks( numReduces )
        
        job.setInputFormatClass(classOf[SequenceFileInputFormat[Text, Text] ])
        job.setMapOutputKeyClass(classOf[Text])
        job.setMapOutputValueClass(classOf[LinkData])
        
        job.setOutputKeyClass(classOf[Text])
        job.setOutputValueClass(classOf[LinkData])
        
        // Output needs to be sequence file otherwise toString is called on LinkData losing information
        job.setOutputFormatClass(classOf[SequenceFileOutputFormat[Text, LinkData] ])
        
        
                
        FileInputFormat.addInputPath(job, new Path(inputFileName))
        FileOutputFormat.setOutputPath(job, new Path(outputFilePath))
        
        if ( job.waitForCompletion(true) ) 0 else 1
    }
}


