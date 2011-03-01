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


class SurfaceFormsMapper extends Mapper[Text, Text, Text, Text]
{
    val markupParser = WikiParser()
    
    def extractLinks( elements : Seq[Node], context : Mapper[Text, Text, Text, Text]#Context )
    {
        for ( child <- elements )
        {
            child match
            {
                case InternalLinkNode(destination, children, line) =>
                {
                    // Interested in 'Main' or 'Category' largely
                    //println( "    " + destination.namespace + ", "  + destination.decoded)
                    
                    if ( children.length != 0 )// && (destination.namespace == "Main" || destination.namespace =="Category"))
                    {
                        val destinationTopic = destination.decoded
                        
                        children(0) match
                        {
                            case TextNode( surfaceForm, line ) =>
                            {
                                context.write( new Text(surfaceForm), new Text(destinationTopic) )
                            }
                            case _ =>
                            {
                                // Do nothing for now
                                //throw new ClassCastException()
                            }
                        }
                    }
                }

                case SectionNode(name, level, children, line ) =>
                {
                    //println( "  Section: " + name )
                    extractLinks( children, context )
                }

                case TemplateNode( title, children, line ) =>
                {
                    // Namespace is 'Template'. Don't want POV, advert, trivia
                    //println( "  " + title.namespace + ", " + title.decoded )
                    extractLinks( children, context )
                }

                case TableNode( caption, children, line ) =>
                {
                    extractLinks( children, context )
                }

                case TextNode( text, line ) =>
                {
                    // Nothing for now
                }

                case _ =>
            }
        }
    }

                
    override def map( key : Text, value : Text, context : Mapper[Text, Text, Text, Text]#Context ) =
    {
        val topicTitle = key
        val topicText = value
        
        try
        {
            val page = new WikiPage( WikiTitle.parse( topicTitle.toString ), 0, 0, topicText.toString )
            val parsed = markupParser( page )
            extractLinks( parsed.children, context )
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

class SurfaceFormsReducer extends Reducer[Text, Text, Text, Text]
{
    override def reduce(key : Text, values : java.lang.Iterable[Text], context : Reducer[Text, Text, Text, Text]#Context) = 
    {
        val seen = new HashSet[Text]
        for ( value <- values )
        {
            if ( !seen.contains(value) )
            {
                context.write( key, value )
                seen += value
            }
        }
    }
}


object SurfaceForms
{
    def main(args:Array[String]) : Unit =
    {
        val conf = new Configuration()
        val otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs
        
        if ( otherArgs.length != 2 )
        {
            println( "Usage: surfaceforms <in> <out>")
            for ( arg <- otherArgs )
            {
                println( "  " + arg )
            }
            return 2
        }
        
        val job = new Job(conf, "Surface forms")
        
        job.setJarByClass(classOf[SurfaceFormsMapper])
        job.setMapperClass(classOf[SurfaceFormsMapper])
        job.setCombinerClass(classOf[SurfaceFormsReducer])
        job.setReducerClass(classOf[SurfaceFormsReducer])
        job.setNumReduceTasks(10)

        job.setOutputKeyClass(classOf[Text])
        job.setOutputValueClass(classOf[Text])
        
        job.setInputFormatClass(classOf[SequenceFileInputFormat[Text, Text] ])
                
        FileInputFormat.addInputPath(job, new Path(args(0)))
        FileOutputFormat.setOutputPath(job, new Path(args(1)))
        
        if ( job.waitForCompletion(true) ) 0 else 1
    }
}



