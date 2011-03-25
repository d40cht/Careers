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


class WordInDocumentMembershipMapper extends Mapper[Text, Text, Text, IntWritable]
{
    val markupParser = WikiParser()
    
    def getWords( page : String ) : List[String] =
    {
    	val textSource = new StringReader( page )
        val tokenizer = new StandardTokenizer( LUCENE_30, textSource )
    
        var run = true
        var wordList : List[String] = Nil
        while ( run )
        {
            val nextTerm = tokenizer.getAttribute(classOf[TermAttribute]).term()
            if ( nextTerm != "" )
            {
                wordList = nextTerm :: wordList
            }
            run = tokenizer.incrementToken()
        }
        tokenizer.close()
        return wordList.reverse
    }
   
    
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
            val words = getWords( extractRawText( key, parsed.children, context ) )
            
            words.foreach( x => uniqueWords = uniqueWords + x )
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

class WordInDocumentMembershipReducer extends Reducer[Text, IntWritable, Text, IntWritable]
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
        job.setMapOutputValueClass(classOf[IntWritable])
        
        job.setOutputKeyClass(classOf[Text])
        job.setOutputValueClass(classOf[IntWritable])
        
        
                
        FileInputFormat.addInputPath(job, new Path(args(0)))
        FileOutputFormat.setOutputPath(job, new Path(args(1)))
        
        if ( job.waitForCompletion(true) ) 0 else 1
    }
}


