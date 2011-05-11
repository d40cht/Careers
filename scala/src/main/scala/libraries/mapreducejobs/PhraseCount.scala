package org.seacourt.mapreducejobs

import org.apache.hadoop.io.{Text, IntWritable}
import org.apache.hadoop.mapred.JobConf

import java.net.URI
import java.io.File

import scala.collection.JavaConversions._

import org.dbpedia.extraction.wikiparser._

import scala.collection.immutable.TreeSet
import scala.collection.mutable.StringBuilder

import org.seacourt.utility._
import org.seacourt.mapreduce._
import org.seacourt.berkeleydb

object PhraseCounter extends MapReduceJob[Text, Text, Text, IntWritable, Text, IntWritable]
{
    val phraseDbKey = "org.seacourt.phrasedbtar"
    val phraseDbRaw = "org.seacourt.phrasedb"
    val maxPhraseWordLength = 8
    
    class JobMapper extends MapperType
    {
        var dbenv : berkeleydb.Environment = null
        var db : berkeleydb.Environment#Database = null
        
        override def setup( context : MapperType#Context )
        {
            val phraseDbFileName = context.getConfiguration().get(phraseDbKey)
            val phraseDbRawName = context.getConfiguration().get(phraseDbRaw)
            
            // Make the phrase db accessible
            dbenv = new berkeleydb.Environment( new File(phraseDbRawName), false )
            db = dbenv.openDb( "phrases", false )
        }
        
        override def cleanup( context : MapperType#Context )
        {
            db.close()
            dbenv.close()
        }
    
        def mapWork( topicTitle : String, topicText : String, output : (String, Int) => Unit )
        {
            try
            {
                val parsed = Utils.wikiParse( topicTitle, topicText )
                
                val text = Utils.foldlWikiTree( parsed, List[String](), (element : Node, stringList : List[String] ) =>
                {
                    element match
                    {
                        case TextNode( text, line ) => text::stringList
                        case _ => stringList
                    }
                } )
                
                val words = Utils.luceneTextTokenizer( Utils.normalize( text.mkString( " " ) ) )
                var seenSet = TreeSet[String]()
                
                var iter = words
                val phrase = new StringBuilder()
                while ( iter != Nil )
                {
                    phrase.clear()
                    var iter2 = iter
                    var count = 0
                    
                    while ( iter2 != Nil && count < maxPhraseWordLength )
                    {
                        phrase.append( iter2.head )
                        
                        // If this is the first time we've seen this phrase and it's in the phrase
                        // db then output a count. Otherwise add it to seen and continue
                        val asString = phrase.toString
                        if ( !seenSet.contains( asString ) )
                        {
                            if ( db.get( asString ) != None )
                            {
                                output( asString, 1 )
                            }
                            seenSet += asString
                                
                        }
                        
                        iter2 = iter2.tail
                        count += 1
                        phrase.append( " " )
                    }

                    iter = iter.tail
                }
            }
            catch
            {
                case e : WikiParserException =>
                case _ => 
            }
        }
        
        override def map( topicTitle : Text, topicText : Text, output : MapperType#Context )
        {
            mapWork( topicTitle.toString, topicText.toString, (key, value) => output.write( new Text(key), new IntWritable(value) ) )
        }
    }
    
    class JobReducer extends ReducerType
    {
        override def reduce( phrase : Text, values: java.lang.Iterable[IntWritable], output : ReducerType#Context )
        {
            var count = 0
            for ( value <- values )
            {
                count += 1
            }
            output.write( phrase, new IntWritable(count) )
        }
    }
    
    override def register( job : Job, config : Configuration )
    {
        job.setMapperClass(classOf[JobMapper])
        job.setReducerClass(classOf[JobReducer])
        
        // Copy the phrase db to distributed cache
        val phraseDbFileName = config.get(phraseDbKey)
        job.createSymlink()
        job.addCacheArchive( new URI(phraseDbFileName) )
    }
}


