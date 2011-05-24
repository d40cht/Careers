package org.seacourt.mapreducejobs

import org.apache.hadoop.io.{Text, IntWritable}
import org.apache.hadoop.mapred.JobConf
import org.apache.hadoop.filecache.DistributedCache

import java.net.URI
import java.io.{File, DataInputStream, FileInputStream}

import scala.collection.JavaConversions._

import org.dbpedia.extraction.wikiparser._

import scala.collection.immutable.{TreeSet, TreeMap}
import scala.collection.mutable.StringBuilder

import org.seacourt.utility._
import org.seacourt.mapreduce._
import org.seacourt.berkeleydb
import org.seacourt.disambiguator.{PhraseMapLookup}

object PhraseCounter extends MapReduceJob[Text, Text, IntWritable, IntWritable, IntWritable, IntWritable]
{
    val phraseDbKey = "org.seacourt.phrasemap"

    
    class JobMapper extends MapperType
    {
        val pml = new PhraseMapLookup()
        
        override def setup( context : MapperType#Context )
        {
            val localCacheFiles = context.getLocalCacheArchives()
            require( localCacheFiles.length == 1 )
         
            // Load the entire phrase map file into RAM   
            pml.load( new DataInputStream( new FileInputStream( new File(localCacheFiles(0).toString ) ) ) )
        }
        
        override def cleanup( context : MapperType#Context )
        {
        }
    
        def mapWork( topicTitle : String, topicText : String, output : (Int, Int) => Unit )
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
                
                var activePhrases = List[PhraseMapLookup#PhraseMapIter]()
                
                var foundPhrases = TreeMap[Int, Int]()
                for ( word <- words )
                {
                    val wordLookup = pml.lookupWord( word )
                    
                    wordLookup match
                    {
                        case Some(wordId) =>
                        {
                            val newPhrase = pml.getIter()
                            activePhrases = newPhrase :: activePhrases
                            
                            var newPhrases = List[PhraseMapLookup#PhraseMapIter]()
                            for ( phrase <- activePhrases )
                            {
                                val phraseId = phrase.update(wordId)
                                if ( phraseId != -1 )
                                {
                                    // Send (phraseId, 1) out as the result
                                    val lastCount = foundPhrases.getOrElse( phraseId, 0 )
                                    foundPhrases = foundPhrases.updated( phraseId, lastCount + 1 )
                                    newPhrases = phrase :: newPhrases
                                }
                            }
                            activePhrases = newPhrases
                        }
                        case _ =>
                    }
                }
                
                for ( (phraseId, count) <- foundPhrases )
                {
                    output( phraseId, count )
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
            mapWork( topicTitle.toString, topicText.toString, (key, value) => output.write( new IntWritable(key), new IntWritable(value) ) )
        }
    }
    
    class JobReducer extends ReducerType
    {
        override def reduce( phraseId : IntWritable, values: java.lang.Iterable[IntWritable], output : ReducerType#Context )
        {
            var count = 0
            for ( value <- values )
            {
                count += value.get()
            }
            output.write( phraseId, new IntWritable(count) )
        }
    }
    
    override def register( job : Job, config : Configuration )
    {
        job.setMapperClass(classOf[JobMapper])
        job.setReducerClass(classOf[JobReducer])
        
        //job.setProfileEnabled(true)

        // Copy the phrase db to distributed cache
        val phraseDbFileName = config.get(phraseDbKey)
        println( "Adding archive to local cache" )
        job.addCacheArchive( new URI(phraseDbFileName) )
        job.createSymlink()
        println( "  complete" )
    }
}


