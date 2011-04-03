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
import org.apache.hadoop.io.{Writable}

import scala.collection.mutable.HashSet

import org.json.JSONArray

import Utils._
import java.io.{DataInput, DataOutput}

// TODO:
// Sanity check text to remove junk

class TextArrayWritable extends Writable
{
    var elements : List[String] = Nil
    var count = 0
    
    def append( value : String )
    {
        elements = value :: elements
        count += 1
    }
    
    override def write( out : DataOutput )
    {
        out.writeInt( count )
        for ( el <- elements ) out.writeUTF( el )
    }
    
    override def readFields( in : DataInput )
    {
        count = in.readInt
        elements = Nil
        for ( i <- 0 to (count-1) )
        {
            elements = in.readUTF :: elements
        }
    }
}

object SurfaceFormsGleaner extends MapReduceJob[Text, Text, Text, Text, Text, TextArrayWritable]
{    
    override def mapfn( topicTitle : Text, topicText : Text, outputFn : (Text, Text) => Unit )
    {
        val markupParser = WikiParser()
        val page = new WikiPage( WikiTitle.parse( topicTitle.toString ), 0, 0, topicText.toString )
        val parsed = markupParser( page )
        
        traverseWikiTree( parsed, element =>
        {
            element match
            {
                case InternalLinkNode( destination, children, line ) =>
                {
                    if ( children.length != 0 &&
                        (destination.namespace.toString() == "Main" ||
                         destination.namespace.toString() == "Category") )
                    {
                        children(0) match
                        {
                            case TextNode( surfaceForm, line ) =>
                            {
                                outputFn( new Text(normalize( surfaceForm )), new Text(destination.namespace.toString +  ":" + destination.decoded.toString) )
                            }
                            case _ =>
                        }
                    }
                }
            }
        } )
    }
    
    override def reducefn( word : Text, values: java.lang.Iterable[Text], outputFn : (Text, TextArrayWritable) => Unit )
    {
        val seen = new HashSet[Text]
        val outValues = new TextArrayWritable()
        var count = 0
        for ( value <- values )
        {
            if ( !seen.contains(value) )
            {
                outValues.append( value.toString() )
                count += 1
            }
            if ( count > 1000 ) return Unit
        }
        outputFn( word, outValues )
    }
}


