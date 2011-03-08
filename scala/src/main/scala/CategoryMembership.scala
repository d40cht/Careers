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

// TODO:
// Sanity check text to remove junk


class CategoryMembershipMapper extends Mapper[Text, Text, Text, Text]
{
    val markupParser = WikiParser()
   
    
    def extractCategories( parentTopic : Text, elements : Seq[Node], context : Mapper[Text, Text, Text, Text]#Context )
    {
        for ( child <- elements )
        {
            child match
            {
                case InternalLinkNode(destination, children, line) =>
                {
                    if ( destination.namespace.toString() == "Category" )
                    {
                        // Link this page to this category
                        context.write( new Text( parentTopic ), new Text( destination.decoded ) )
                    }
                }

                case SectionNode(name, level, children, line ) =>
                {
                    //println( "  Section: " + name )
                    extractCategories( parentTopic, children, context )
                }

                case TemplateNode( title, children, line ) =>
                {
                    // Namespace is 'Template'. Don't want POV, advert, trivia
                    //println( "  " + title.namespace + ", " + title.decoded )
                    extractCategories( parentTopic, children, context )
                }

                case TableNode( caption, children, line ) =>
                {
                    extractCategories( parentTopic, children, context )
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
            extractCategories( key, parsed.children, context )
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

class CategoryMembershipReducer extends Reducer[Text, Text, Text, JSONObjectWritable]
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
        }
        context.write( key, outValues )
    }
}


object CategoryMembership
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
        
        val job = new Job(conf, "Surface forms")
        
        job.setJarByClass(classOf[CategoryMembershipMapper])
        job.setMapperClass(classOf[CategoryMembershipMapper])
        //job.setCombinerClass(classOf[SurfaceFormsReducer])
        job.setReducerClass(classOf[CategoryMembershipReducer])
        job.setNumReduceTasks( args(2).toInt )
        
        job.setInputFormatClass(classOf[SequenceFileInputFormat[Text, Text] ])
        job.setMapOutputKeyClass(classOf[Text])
        job.setMapOutputValueClass(classOf[Text])
        
        job.setOutputKeyClass(classOf[Text])
        job.setOutputValueClass(classOf[JSONObjectWritable])
        
        
                
        FileInputFormat.addInputPath(job, new Path(args(0)))
        FileOutputFormat.setOutputPath(job, new Path(args(1)))
        
        if ( job.waitForCompletion(true) ) 0 else 1
    }
}

