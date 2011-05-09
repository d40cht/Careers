package org.seacourt.mapreducejobs


import org.apache.hadoop.io.{Text, IntWritable}

import scala.collection.JavaConversions._

import org.dbpedia.extraction.wikiparser._

import scala.collection.immutable.TreeSet
import scala.collection.mutable.StringBuilder

import org.seacourt.utility._
import org.seacourt.mapreduce._

object PhraseCounter extends MapReduceJob[Text, Text, Text, IntWritable, Text, IntWritable]
{
    val maxPhraseWordLength = 8
    
    class JobMapper extends MapperType
    {
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
                        
                        val asString = phrase.toString
                        if ( !seenSet.contains( asString ) )
                        {
                            output( asString, 1 )
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
    
    override def register( job : Job )
    {
        job.setMapperClass(classOf[JobMapper])
        job.setReducerClass(classOf[JobReducer])
    }
}


